package datawave.experimental;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import datawave.experimental.util.ScannerChunkUtil;
import datawave.query.attributes.Document;
import datawave.query.tables.async.ScannerChunk;
import datawave.query.util.MetadataHelper;
import datawave.util.TableName;
import datawave.webservice.query.util.QueryUncaughtExceptionHandler;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.Key;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Runs multiple {@link QueryIteratorExecutor}s in parallel.
 *
 * Options to handle field index lookups and document evaluation in separate threads
 */
public class QueryIteratorServiceForTests implements Thread.UncaughtExceptionHandler, FutureCallback<QueryIteratorExecutor> {
    
    private static final Logger log = Logger.getLogger(QueryIteratorServiceForTests.class);
    
    private final int numThreads = 75;
    
    private final Connector conn;
    private final MetadataHelper helper;
    
    private ExecutorService threadPool;
    private ExecutorService fieldIndexPool;
    private ExecutorService evaluationPool;
    
    private final LinkedBlockingQueue<Map.Entry<Key,Document>> results;
    
    private final AtomicInteger executorCount = new AtomicInteger(0);
    private final QueryUncaughtExceptionHandler handler = new QueryUncaughtExceptionHandler();
    private final MyThreadFactory threadFactory = new MyThreadFactory("default name", this);
    
    @Override
    public void uncaughtException(Thread t, Throwable e) {
        t.interrupt();
        // stop logic
        handler.uncaughtException(t, e);
    }
    
    /**
     * If a {@link QueryIteratorExecutor} finished successfully then update the executor count
     * 
     * @param executor
     *            an executor that just finished
     */
    @Override
    public void onSuccess(QueryIteratorExecutor executor) {
        log.info("scan finished successfully");
        int i = executorCount.decrementAndGet();
        log.info("executor count: " + i);
    }
    
    @Override
    public void onFailure(Throwable t) {
        executorCount.decrementAndGet();
        log.error("executor failed, propagating exception with msg: " + t.getMessage());
        Throwables.propagate(t);
    }
    
    /**
     * Creates threads for us with exception handling
     */
    private class MyThreadFactory implements ThreadFactory {
        private String name;
        private final ThreadFactory factory = Executors.defaultThreadFactory();
        private final Thread.UncaughtExceptionHandler handler;
        
        public MyThreadFactory(String name, Thread.UncaughtExceptionHandler handler) {
            this.name = name;
            this.handler = handler;
        }
        
        @SuppressWarnings("NullableProblems")
        public Thread newThread(Runnable r) {
            Thread thread = factory.newThread(r);
            thread.setName(name);
            thread.setDaemon(true);
            thread.setUncaughtExceptionHandler(handler);
            return thread;
        }
        
        public void updateName(String name) {
            this.name = name;
        }
    }
    
    public QueryIteratorServiceForTests(LinkedBlockingQueue<Map.Entry<Key,Document>> results, Connector conn, MetadataHelper helper) {
        this.results = results;
        this.conn = conn;
        this.helper = helper;
        
        // setup main thread pool with exception handling
        this.threadPool = new ThreadPoolExecutor(numThreads, numThreads, 120, TimeUnit.MINUTES, new LinkedBlockingQueue<>(), threadFactory);
        this.threadPool = MoreExecutors.listeningDecorator(threadPool);
        
        // secondary thread pools for field fetching and doc evaluation
        int fiThreads = 5;
        this.fieldIndexPool = new ThreadPoolExecutor(fiThreads, fiThreads, 120, TimeUnit.MINUTES, new LinkedBlockingQueue<>(), threadFactory);
        this.fieldIndexPool = MoreExecutors.listeningDecorator(fieldIndexPool);
        
        int evalThreads = 5;
        this.evaluationPool = new ThreadPoolExecutor(evalThreads, evalThreads, 120, TimeUnit.MINUTES, new LinkedBlockingQueue<>(), threadFactory);
        this.evaluationPool = MoreExecutors.listeningDecorator(evaluationPool);
    }
    
    /**
     * Push a scanner chunk to the service for execution. Callers should ensure room exists via {@link QueryIteratorServiceForTests#canPush()} otherwise their
     * task may sit on the thread pool's work queue.
     *
     * @param scanId
     *            the query id and range
     * @param chunk
     *            a ScannerChunk
     * @return true if
     */
    @SuppressWarnings("UnstableApiUsage")
    public boolean push(String scanId, ScannerChunk chunk) {
        
        // update executor count and set callback
        int i = executorCount.incrementAndGet();
        log.info("executor count: " + i);
        
        QueryIteratorExecutor executor = new QueryIteratorExecutor(results, fieldIndexPool, evaluationPool, conn, TableName.SHARD,
                        ScannerChunkUtil.authsFromChunk(chunk), helper);
        executor.setScannerChunk(chunk);
        
        // use this one weird trick to name callable threads!
        threadFactory.updateName(scanId);
        
        ListenableFuture<QueryIteratorExecutor> f = (ListenableFuture<QueryIteratorExecutor>) threadPool.submit(executor);
        Futures.addCallback(f, this);
        
        // for now assume we always pushed the future into the thread pool
        return true;
    }
    
    /**
     * @return true if a thread is immediately available
     */
    public boolean canPush() {
        return executorCount.get() < numThreads;
    }
    
    /**
     * @return an integer count of currently executing futures
     */
    public int count() {
        return executorCount.get();
    }
}
