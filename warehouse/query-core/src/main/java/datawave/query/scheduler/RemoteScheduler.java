package datawave.query.scheduler;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.log4j.Logger;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import datawave.experimental.executor.QueryExecutor;
import datawave.experimental.executor.QueryExecutorFactory;
import datawave.experimental.threads.NamedThreadFactory;
import datawave.experimental.threads.NamedUncaughtExceptionHandler;
import datawave.experimental.util.ScanStats;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.tables.ScannerFactory;
import datawave.query.tables.stats.ScanSessionStats;
import datawave.query.util.MetadataHelper;
import datawave.query.util.MetadataHelperFactory;
import datawave.webservice.common.logging.ThreadConfigurableLogger;
import datawave.webservice.query.configuration.QueryData;

/**
 * A {@link Scheduler} that keeps business logic out of the tablet server.
 * <p>
 * The 'iterator()' method provides callers with a simple iterator interface that has access to the result queue
 * <p>
 * The QueryPlanThread pulls query plans from the GlobalIndex, creates QueryExecutors and submits them to a query executor thread pool. When no more query plans
 * exist the thread shuts down.
 * <p>
 * QueryExecutors handle query execution within the context of a single shard range. The global index lookup often prunes a query, thus the scope of the field
 * index lookup varies from shard to shard. For detailed documentation see {@link QueryExecutor}.
 * <p>
 * All QueryExecutors utilize shared thread pools for uid lookups and document aggregation.
 * <p>
 * The following query features are not supported
 * <ul>
 * <li>grouping functions</li>
 * <li>Ivarators (queries that anchor on a regex backed by file system)</li>
 * <li>Full Table Scans</li>
 * <li>Queries which require a {@link datawave.query.config.Profile}</li>
 * <li>TLD queries are not supported</li>
 * </ul>
 */
public class RemoteScheduler extends Scheduler implements FutureCallback<QueryExecutor> {

    private static final Logger log = ThreadConfigurableLogger.getLogger(RemoteScheduler.class);

    private final AtomicBoolean pullingPlans = new AtomicBoolean(false);
    private final AtomicBoolean pullingResults = new AtomicBoolean(false);

    protected final LinkedBlockingQueue<Entry<Key,Value>> results;

    private final AtomicInteger executorCount = new AtomicInteger(0);
    private ExecutorService executorThreadPool;
    private ExecutorService queryPlanThreadPool;
    private ExecutorService uidThreadPool; // for getting uids
    private ExecutorService eventThreadPool; // for getting events
    private ExecutorService documentThreadPool; // for aggregating documents

    private ScanStats stats = new ScanStats();

    /**
     * This scheduler requires knowledge of the query's ShardQueryConfiguration
     *
     * @param config
     *            an instance of {@link ShardQueryConfiguration}
     */
    public RemoteScheduler(ShardQueryConfiguration config, MetadataHelperFactory metadataHelperFactory) {
        results = new LinkedBlockingQueue<>();

        MetadataHelper metadataHelper = metadataHelperFactory.createMetadataHelper(config.getClient(), config.getMetadataTableName(),
                        config.getAuthorizations());

        pullingPlans.set(true);
        pullingResults.set(true);

        // configure query executor exception handler, thread factory, and executor service
        NamedUncaughtExceptionHandler handler = new NamedUncaughtExceptionHandler("QueryExecutorPool");
        NamedThreadFactory queryExecutorThreadFactory = new NamedThreadFactory("QueryExecutorThreadFactory", handler);

        int threads = config.getNumQueryThreads();
        executorThreadPool = new ThreadPoolExecutor(threads, threads, 60L, TimeUnit.MINUTES, new LinkedBlockingQueue<>(), queryExecutorThreadFactory);
        executorThreadPool = MoreExecutors.listeningDecorator(executorThreadPool);
        log.info("created executor thread pool with " + threads + " threads");

        // executor for finding uids in the FI
        NamedUncaughtExceptionHandler uidHandler = new NamedUncaughtExceptionHandler("UidPool");
        NamedThreadFactory uidThreadFactory = new NamedThreadFactory("UidThreadFactory", uidHandler);
        uidThreadPool = new ThreadPoolExecutor(threads, threads, 1L, TimeUnit.MINUTES, new LinkedBlockingQueue<>(), uidThreadFactory);
        uidThreadPool = MoreExecutors.listeningDecorator(uidThreadPool);

        NamedUncaughtExceptionHandler eventHandler = new NamedUncaughtExceptionHandler("EventPool");
        NamedThreadFactory eventThreadFactory = new NamedThreadFactory("EventThreadFactory", eventHandler);
        eventThreadPool = new ThreadPoolExecutor(threads, threads, 1L, TimeUnit.MINUTES, new LinkedBlockingQueue<>(), eventThreadFactory);
        eventThreadPool = MoreExecutors.listeningDecorator(eventThreadPool);

        // executor for document aggregation
        NamedUncaughtExceptionHandler docHandler = new NamedUncaughtExceptionHandler("DocumentPool");
        NamedThreadFactory docThreadFactory = new NamedThreadFactory("DocThreadFactory", docHandler);
        documentThreadPool = new ThreadPoolExecutor(threads, threads, 1L, TimeUnit.MINUTES, new LinkedBlockingQueue<>(), docThreadFactory);
        documentThreadPool = MoreExecutors.listeningDecorator(documentThreadPool);

        QueryExecutorFactory executorFactory = new QueryExecutorFactory(config, metadataHelper, results, uidThreadPool, eventThreadPool, documentThreadPool,
                        stats);

        NamedUncaughtExceptionHandler queryPlanExceptionHandler = new NamedUncaughtExceptionHandler("QueryPlanPool");
        NamedThreadFactory queryPlanThreadFactory = new NamedThreadFactory("QueryPlanThreadFactory", queryPlanExceptionHandler);
        queryPlanThreadPool = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), queryPlanThreadFactory);
        queryPlanThreadPool = MoreExecutors.listeningDecorator(queryPlanThreadPool);

        QueryPlanCallback queryPlanCallback = new QueryPlanCallback(this);
        QueryPlanIterator queryPlanIterator = new QueryPlanIterator(config.getQueries(), executorThreadPool, executorFactory, pullingPlans, executorCount,
                        this);
        ListenableFuture<QueryPlanIterator> f = (ListenableFuture<QueryPlanIterator>) queryPlanThreadPool.submit(queryPlanIterator);
        Futures.addCallback(f, queryPlanCallback, queryPlanThreadPool);
    }

    /**
     * Issue close to remote scan service and shutdown executor services
     *
     * @throws IOException
     *             if something goes wrong
     */
    @Override
    public void close() throws IOException {
        pullingPlans.set(false);
        pullingResults.set(false);
        queryPlanThreadPool.shutdownNow();
        executorThreadPool.shutdownNow();
        documentThreadPool.shutdownNow();
        eventThreadPool.shutdownNow();
        uidThreadPool.shutdownNow();
        if (stats != null) {
            log.info("=== Remote Scheduler Scan Stats ===");
            stats.logStats(log);
        }
    }

    /**
     * If a {@link QueryExecutor} finished successfully then update the executor count
     *
     * @param executor
     *            an executor that just finished
     */
    @Override
    public void onSuccess(QueryExecutor executor) {
        log.info("scan finished successfully");
        int i = executorCount.decrementAndGet();
        log.info("executor count: " + i);
    }

    @Override
    public void onFailure(Throwable t) {
        executorCount.decrementAndGet();
        t.printStackTrace();
        log.error("executor failed, propagating exception with msg: " + t.getMessage());
        Throwables.propagateIfPossible(t, RuntimeException.class);
    }

    @Override
    public Iterator<Entry<Key,Value>> iterator() {
        return new Iterator<>() {

            private Entry<Key,Value> next = null;

            @Override
            public boolean hasNext() {
                while (next == null && (pullingPlans.get() || pullingResults.get())) {
                    try {
                        next = results.poll(250, TimeUnit.MICROSECONDS);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }

                    if (next == null && !pullingPlans.get() && executorCount.get() == 0) {
                        pullingResults.set(false);
                    }
                }
                return next != null;
            }

            @Override
            public Entry<Key,Value> next() {
                Entry<Key,Value> result = next;
                next = null;
                return result;
            }
        };
    }

    // default impl
    @Override
    public BatchScanner createBatchScanner(ShardQueryConfiguration config, ScannerFactory scannerFactory, QueryData qd) throws TableNotFoundException {
        throw new IllegalStateException("Cannot create batch scanner for the RemoteScheduelr");
    }

    // no-op
    @Override
    public ScanSessionStats getSchedulerStats() {
        return new ScanSessionStats();
    }

    /**
     * Runnable that pulls {@link QueryData} and pushes to the executor service
     */
    static class QueryPlanIterator implements Runnable {

        private final Iterator<QueryData> iterator;
        private final ExecutorService threadPool;
        private final QueryExecutorFactory executorFactory;
        private final AtomicBoolean pullingPlans;
        private final AtomicInteger executorCount;

        private final FutureCallback<QueryExecutor> futureCallback; // the remote scheduler

        public QueryPlanIterator(Iterator<QueryData> iterator, ExecutorService threadPool, QueryExecutorFactory executorFactory, AtomicBoolean pullingPlans,
                        AtomicInteger executorCount, FutureCallback<QueryExecutor> futureCallback) {
            this.iterator = iterator;
            this.threadPool = threadPool;
            this.executorFactory = executorFactory;
            this.pullingPlans = pullingPlans;
            this.executorCount = executorCount;
            this.futureCallback = futureCallback;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void run() {
            Thread.currentThread().setName("QueryPlanThread");
            int count = 0;
            while (iterator.hasNext()) {
                count++;
                QueryData queryData = iterator.next();
                QueryExecutor executor = executorFactory.createExecutor(queryData);

                executorCount.incrementAndGet();

                ListenableFuture<QueryExecutor> f = (ListenableFuture<QueryExecutor>) threadPool.submit(executor);
                Futures.addCallback(f, futureCallback, threadPool);
            }
            log.info(count + " plans submitted");
            pullingPlans.set(false);
        }
    }

    // if there's an exception in the query plan iterator thread, log it and close the remote scheduler
    static class QueryPlanCallback implements FutureCallback<QueryPlanIterator> {

        private static final Logger log = ThreadConfigurableLogger.getLogger(QueryPlanCallback.class);
        private final RemoteScheduler scheduler;

        public QueryPlanCallback(RemoteScheduler scheduler) {
            this.scheduler = scheduler;
        }

        @Override
        public void onSuccess(QueryPlanIterator result) {
            log.info("QueryPlanIterator is done");
            scheduler.pullingPlans.set(false);
        }

        @Override
        public void onFailure(Throwable t) {
            log.error("QueryPlanIterator threw an exception: " + t.getMessage());
            t.printStackTrace();
            try {
                scheduler.close();
            } catch (IOException e) {
                log.error("RemoteScheduler threw an exception trying to close: " + e.getMessage(), e);
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
    }
}
