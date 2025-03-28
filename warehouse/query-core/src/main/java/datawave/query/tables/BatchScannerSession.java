package datawave.query.tables;

import java.io.InterruptedIOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.clientImpl.ScannerOptions;
import org.apache.accumulo.core.clientImpl.TabletLocator;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.configuration.QueryData;
import datawave.core.query.configuration.Result;
import datawave.core.query.configuration.ResultContext;
import datawave.core.query.logic.QueryCheckpoint;
import datawave.core.query.logic.QueryKey;
import datawave.microservice.query.Query;
import datawave.query.tables.async.Scan;
import datawave.query.tables.async.ScannerChunk;
import datawave.query.tables.async.SessionArbiter;
import datawave.query.tables.async.SpeculativeScan;

public class BatchScannerSession extends ScannerSession implements Iterator<Result>, FutureCallback<Scan>, SessionArbiter, UncaughtExceptionHandler {

    private static final double RANGE_MULTIPLIER = 5;

    private static final double QUEUE_MULTIPLIER = 25;

    /**
     * The configuration used for checkpoints
     */
    private GenericQueryConfiguration config;

    /**
     * Delegates scanners to us, blocking if none are available or used by other sources.
     */
    private ResourceQueue delegatorReference;

    /**
     * Table to which this scanner will connect.
     */
    private String localTableName;

    /**
     * Authorization set
     */
    private Set<Authorizations> localAuths;

    /**
     * This is the iterator of scanner chunks. Basically the work queue.
     */
    protected Iterator<List<ScannerChunk>> scannerBatches;

    /**
     * This is the current batch of chunks pending submission
     */
    protected BlockingQueue<ScannerChunk> currentBatch;

    // set when we need operations to stop gracefully enough to checkpoint
    protected volatile boolean needToCheckpoint = false;

    // set when the processing is at a place where we can checkpoint
    protected volatile boolean readyToCheckpoint = false;

    protected ExecutorService service = null;

    protected StringBuilder threadId = new StringBuilder();

    protected List<Function<ScannerChunk,ScannerChunk>> visitorFunctions = Lists.newArrayList();

    protected long scanLimitTimeout = -1;

    private static final Logger log = Logger.getLogger(BatchScannerSession.class);

    protected Map<String,AtomicInteger> serverFailureMap;

    protected Map<String,AtomicInteger> serverMap;

    protected AtomicInteger runnableCount = new AtomicInteger(0);

    protected Set<ResultContext> runningQueries = Collections.synchronizedSet(new HashSet<>());

    protected boolean backoffEnabled = false;

    protected boolean speculativeScanning = false;

    protected int threadCount = 5;

    public List<QueryCheckpoint> checkpoint(QueryKey queryKey) {
        needToCheckpoint = true;
        while (!readyToCheckpoint && isRunning()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {}
        }
        List<QueryCheckpoint> checkpoints = new ArrayList<>();
        for (ResultContext context : runningQueries) {
            if (!context.isFinished()) {
                checkpoints.add(new QueryCheckpoint(queryKey, Collections.singletonList((QueryData) context)));
            }

        }
        // now add all of the remaining chunks
        for (Iterator<List<ScannerChunk>> it = scannerBatches; it.hasNext();) {
            List<ScannerChunk> chunks = it.next();
            for (ScannerChunk chunk : chunks) {
                checkpoints.add(new QueryCheckpoint(queryKey, Collections.singletonList((QueryData) chunk.getContext())));
            }
        }
        return checkpoints;
    }

    private class BatchReaderThreadFactory implements ThreadFactory {

        private ThreadFactory dtf = Executors.defaultThreadFactory();
        private int threadNum = 1;
        private StringBuilder threadIdentifier;
        private UncaughtExceptionHandler uncaughtHandler = null;

        public BatchReaderThreadFactory(StringBuilder threadName, UncaughtExceptionHandler handler) {
            uncaughtHandler = handler;
            this.threadIdentifier = threadName;
        }

        public Thread newThread(Runnable r) {
            Thread thread = dtf.newThread(r);
            thread.setName("Datawave BatchScanner Session " + threadIdentifier + " -" + threadNum++);
            thread.setDaemon(true);
            thread.setUncaughtExceptionHandler(uncaughtHandler);
            return thread;
        }

    }

    public BatchScannerSession(ScannerSession other) {
        this(other.tableName, other.auths, other.sessionDelegator, other.maxResults, other.settings, other.options, other.ranges);

    }

    /**
     * Constructor
     *
     * @param tableName
     *            incoming table name
     * @param auths
     *            set of authorizations.
     * @param delegator
     *            scanner queue
     * @param maxResults
     *            the max results
     * @param settings
     *            the query settings
     * @param options
     *            the scanner options
     * @param ranges
     *            list of ranges
     */
    public BatchScannerSession(String tableName, Set<Authorizations> auths, ResourceQueue delegator, int maxResults, Query settings, ScannerOptions options,
                    Collection<Range> ranges) {

        super(tableName, auths, delegator, maxResults, settings);
        Preconditions.checkNotNull(delegator);

        localTableName = tableName;

        localAuths = auths;

        delegatorReference = super.sessionDelegator;

        scannerBatches = Collections.emptyIterator();

        currentBatch = Queues.newLinkedBlockingDeque();

        setThreads(1);

        addListener(new BatchScannerListener(), MoreExecutors.newDirectExecutorService());

        serverFailureMap = Maps.newConcurrentMap();

        serverMap = Maps.newConcurrentMap();

    }

    public BatchScannerSession updateThreadService(ExecutorService service) {
        if (service != null)
            this.service.shutdownNow();
        this.service = service;
        return this;
    }

    public void setScanLimit(long timeout) {
        this.scanLimitTimeout = timeout;
    }

    public BatchScannerSession setThreads(int threads) {
        if (service != null)
            service.shutdownNow();
        this.threadCount = threads;
        service = new ThreadPoolExecutor(threads, threads, 120, TimeUnit.MINUTES, new LinkedBlockingQueue<>(), new BatchReaderThreadFactory(threadId, this));
        service = MoreExecutors.listeningDecorator(service);
        return this;
    }

    public BatchScannerSession setConfig(GenericQueryConfiguration config) {
        this.config = config;
        return this;
    }

    public BatchScannerSession updateIdentifier(String threadId) {
        this.threadId.append(threadId);
        return this;
    }

    /**
     * Sets the ranges for the given scannersession.
     *
     * @param chunkIter
     *            list of scanner chunks
     * @return the scanner session
     */
    public synchronized BatchScannerSession setChunkIter(Iterator<List<ScannerChunk>> chunkIter) {

        scannerBatches = chunkIter;

        return this;

    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof BatchScannerSession) {
            EqualsBuilder builder = new EqualsBuilder();
            builder.append(localTableName, ((BatchScannerSession) obj).localTableName);
            builder.append(localAuths, ((BatchScannerSession) obj).localAuths);
            return builder.isEquals();
        }

        return false;
    }

    @Override
    public int hashCode() {
        int result = localTableName != null ? localTableName.hashCode() : 0;
        result = 31 * result + (localAuths != null ? localAuths.hashCode() : 0);
        return result;
    }

    /**
     * Override this for your specific implementation.
     *
     * @param lastKey
     *            the last key
     * @param previousRange
     *            the previous range
     */
    public Range buildNextRange(final Key lastKey, final Range previousRange) {
        return new Range(lastKey.followingKey(PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME), true, previousRange.getEndKey(), previousRange.isEndKeyInclusive());
    }

    /**
     * set the resource class.
     *
     * @param clazz
     *            a class
     */
    public void setResourceClass(Class<? extends AccumuloResource> clazz) {
        delegatedResourceInitializer = clazz;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.util.Iterator#remove()
     */
    @Override
    public void remove() {
        // do nothing.

    }

    /*
     * (non-Javadoc)
     *
     * @see com.google.common.util.concurrent.AbstractExecutionThreadService#run()
     */
    @Override
    protected void run() throws Exception {
        try {
            if (!scannerBatches.hasNext()) {
                if (log.isTraceEnabled())
                    log.trace("Immediate shutdown of scanner session because no work available");
                return;
            }

            while (scannerBatches.hasNext() && !needToCheckpoint) {
                if (runnableCount.get() < (threadCount * RANGE_MULTIPLIER)) {
                    if (currentBatch.isEmpty()) {
                        List<ScannerChunk> chunks = scannerBatches.next();

                        submitTasks(chunks);
                    } else {
                        submitTasks();
                    }
                } else if (currentBatch.size() < (threadCount * QUEUE_MULTIPLIER)) {

                    List<ScannerChunk> chunks = scannerBatches.next();

                    pushChunks(chunks);

                } else {
                    if (log.isTraceEnabled()) {
                        log.trace("Parking for 10 milliseconds until we have additional work that can be done; " + threadCount + " "
                                        + (threadCount * RANGE_MULTIPLIER) + " " + currentBatch.size() + " >= " + (threadCount * QUEUE_MULTIPLIER));
                    }
                    Thread.sleep(10);
                    if (Thread.interrupted() || !isRunning()) {
                        service.shutdownNow();
                        throw new InterruptedException("Interrupted while parking");
                    }
                }

            }
            if (log.isTraceEnabled())
                log.trace("waiting " + runnableCount.get());
            submitTasks();

            // notify those that are wondering
            readyToCheckpoint = true;

            while (runnableCount.get() > 0 && !needToCheckpoint) {
                Thread.sleep(1);
                // if a failure did not occur, let's check the interrupted status
                if (isRunning()) {

                    if (Thread.interrupted()) {
                        service.shutdownNow();
                        throw new InterruptedException("Interrupted while parking");
                    }
                } else {
                    if (log.isTraceEnabled())
                        log.trace(" no longer running");
                    service.shutdownNow();
                    return;
                }
            }
            service.shutdown();
            while (!service.awaitTermination(250, TimeUnit.MILLISECONDS)) {}
        } catch (Exception e) {
            uncaughtExceptionHandler.uncaughtException(Thread.currentThread().currentThread(), e);
            Throwables.propagate(e);
        } finally {
            // make sure nobody is hung up on this flag....
            readyToCheckpoint = true;
        }
    }

    @Override
    protected long getPollTime() {
        return 5;
    }

    protected void pushChunks(List<ScannerChunk> chunks) {
        currentBatch.addAll(chunks);
    }

    protected void submitTasks(List<ScannerChunk> newChunks) {

        for (ScannerChunk chunk : newChunks) {

            AtomicInteger numChunks = serverMap.get(chunk.getLastKnownLocation());
            if (numChunks == null) {
                numChunks = new AtomicInteger(1);
                serverMap.put(chunk.getLastKnownLocation(), numChunks);
            } else
                numChunks.incrementAndGet();

            Scan scan = null;

            if (speculativeScanning) {

                if (log.isTraceEnabled()) {
                    log.trace("Using speculative execution");
                }

                chunk.setQueryId(settings.getId().toString());

                scan = new SpeculativeScan(localTableName, localAuths, chunk, delegatorReference, delegatedResourceInitializer, resultQueue, service);

                scan.setVisitors(visitorFunctions);

                Scan childScan = new Scan(localTableName, localAuths, new ScannerChunk(chunk), delegatorReference, BatchResource.class,
                                ((SpeculativeScan) scan).getQueue(), service);

                childScan.setVisitors(visitorFunctions);

                ((SpeculativeScan) scan).addScan(childScan);

                childScan = new Scan(localTableName, localAuths, new ScannerChunk(chunk), delegatorReference, delegatedResourceInitializer,
                                ((SpeculativeScan) scan).getQueue(), service);

                childScan.setVisitors(visitorFunctions);

                ((SpeculativeScan) scan).addScan(childScan);

            } else {
                scan = new Scan(localTableName, localAuths, chunk, delegatorReference, delegatedResourceInitializer, resultQueue, service);
            }

            if (backoffEnabled) {
                scan.setSessionArbiter(this);
            }
            scan.setVisitors(visitorFunctions);
            scan.setTimeout(scanLimitTimeout);
            if (log.isTraceEnabled()) {
                log.trace("Adding scanner " + chunk);
            }
            submitScan(scan, true);
        }

    }

    /**
     *
     */
    protected void submitTasks() {

        List<ScannerChunk> newChunks;
        newChunks = Lists.newArrayList(currentBatch);
        currentBatch.clear();
        Collections.shuffle(newChunks);
        for (ScannerChunk chunk : newChunks) {

            AtomicInteger numChunks = serverMap.get(chunk.getLastKnownLocation());
            if (numChunks == null) {
                numChunks = new AtomicInteger(1);
                serverMap.put(chunk.getLastKnownLocation(), numChunks);
            } else
                numChunks.incrementAndGet();

            Scan scan = null;

            if (speculativeScanning) {

                if (log.isTraceEnabled()) {
                    log.trace("Using speculative execution");
                }
                scan = new SpeculativeScan(localTableName, localAuths, chunk, delegatorReference, delegatedResourceInitializer, resultQueue, service);

                ((SpeculativeScan) scan).addScan(new Scan(localTableName, localAuths, new ScannerChunk(chunk), delegatorReference, BatchResource.class,
                                ((SpeculativeScan) scan).getQueue(), service));

                ((SpeculativeScan) scan).addScan(new Scan(localTableName, localAuths, new ScannerChunk(chunk), delegatorReference, delegatedResourceInitializer,
                                ((SpeculativeScan) scan).getQueue(), service));

            } else {
                scan = new Scan(localTableName, localAuths, chunk, delegatorReference, delegatedResourceInitializer, resultQueue, service);
            }

            if (backoffEnabled) {
                scan.setSessionArbiter(this);
            }
            scan.setVisitors(visitorFunctions);
            scan.setTimeout(scanLimitTimeout);
            if (log.isTraceEnabled()) {
                log.trace("Adding scanner " + chunk);
            }
            submitScan(scan, true);
        }

    }

    protected void submitScan(Scan scan, boolean increment) {
        ListenableFuture<Scan> future = (ListenableFuture<Scan>) service.submit(scan);
        if (increment) {
            runnableCount.incrementAndGet();
            runningQueries.add((scan.getScannerChunk().getContext()));
        }
        Futures.addCallback(future, this, MoreExecutors.newDirectExecutorService());
    }

    /**
     * Set the scanner options
     *
     * @param options
     *            options
     * @return the scan session
     */
    public BatchScannerSession setOptions(SessionOptions options) {
        return this;
    }

    public void setConsistencyLevel(ScannerBase.ConsistencyLevel consistencyLevel) {
        this.options.setConsistencyLevel(consistencyLevel);
    }

    /**
     * @return the current range object for testing.
     */
    protected Range getCurrentRange() {
        return null;
    }

    /**
     * @return last Range.
     */
    protected Range getLastRange() {
        return lastRange;
    }

    /**
     * @return last key.
     *
     */
    protected Key getLastKey() {
        return null;
    }

    /*
     * (non-Javadoc)
     *
     * @see com.google.common.util.concurrent.FutureCallback#onSuccess(java.lang. Object)
     */
    @Override
    public void onSuccess(Scan finishedScan) {
        /**
         * In the event that we are not finished (i.e. time sliced ) we should resubmit
         */

        if (finishedScan.finished()) {
            runnableCount.decrementAndGet();

            // if we have pulled all of the results of the front end for this query, then and only then can we remove it.
            // otherwise we still need it for checkpointing
            if (finishedScan.getScannerChunk().getContext().isFinished()) {
                runningQueries.remove(finishedScan.getScannerChunk().getContext());
            }

            finishedScan.close();

            if (null != stats && null != finishedScan.getStats()) {
                synchronized (stats) {
                    stats.merge(finishedScan.getStats());
                }
            }
        } else {
            // we've timed out
            AtomicInteger failCount = serverFailureMap.get(finishedScan.getScanLocation());

            if (null == failCount) {
                failCount = new AtomicInteger(1);
                serverFailureMap.put(finishedScan.getScanLocation(), failCount);
            } else {
                failCount.incrementAndGet();
            }

            submitScan(finishedScan, false);
        }

    }

    /*
     * (non-Javadoc)
     *
     * @see com.google.common.util.concurrent.FutureCallback#onFailure(java.lang. Throwable)
     */
    @Override
    public void onFailure(Throwable t) {
        if (isInterruptedException(t)) {
            log.info("BatchScannerSession interrupted");
        } else {
            log.error("BatchScanerSession failed", t);
        }
        uncaughtExceptionHandler.uncaughtException(Thread.currentThread().currentThread(), t);
        stopAsync();
        Throwables.propagate(t);
    }

    private boolean isInterruptedException(Throwable t) {
        while (t != null && !(t instanceof InterruptedException || t instanceof InterruptedIOException)
                        && !(t.getMessage() != null && t.getMessage().contains("InterruptedException"))) {
            t = t.getCause();
        }
        return t != null;
    }

    private class BatchScannerListener extends Service.Listener {
        private static final int MAX_WAIT = 480;

        /*
         * (non-Javadoc)
         *
         * @see com.google.common.util.concurrent.Service.Listener#starting()
         */
        @Override
        public void starting() {
            /**
             * Nothing to do here
             */

        }

        /*
         * (non-Javadoc)
         *
         * @see com.google.common.util.concurrent.Service.Listener#running()
         */
        @Override
        public void running() {
            /**
             * Nothing to do here
             */

        }

        /*
         * (non-Javadoc) QueryIterator
         *
         * @see com.google.common.util.concurrent.Service.Listener#stopping(com. google .common.util.concurrent.Service.State)
         */
        @Override
        public void stopping(State from) {
            /**
             * If we are in the stopping state we should shutdown the service executor
             *
             */
            if (log.isTraceEnabled())
                log.trace("stopping from " + from);
            switch (from) {
                case NEW:
                case RUNNING:
                case STARTING:
                    shutdownServices();
                    break;
                default:
                    break;
            }
        }

        /*
         * (non-Javadoc)
         *
         * @see com.google.common.util.concurrent.Service.Listener#terminated(com .google.common.util.concurrent.Service.State)
         */
        @Override
        public void terminated(State from) {
            /**
             * Shutdown the listener service executor so that we fully release resources
             *
             */
            if (log.isTraceEnabled())
                log.trace("terminated from " + from);
            shutdownServices();
        }

        /*
         * (non-Javadoc)
         *
         * @see com.google.common.util.concurrent.Service.Listener#failed(com.google .common.util.concurrent.Service.State, java.lang.Throwable)
         */
        @Override
        public void failed(State from, Throwable failure) {
            if (log.isTraceEnabled())
                log.trace("failed from " + from + " " + failure);
            shutdownServices();

        }

        /**
         *
         */
        protected void shutdownServices() {
            service.shutdownNow();
            int count = 0;
            try {
                while (!service.awaitTermination(250, TimeUnit.MILLISECONDS) && count < MAX_WAIT) {
                    count++;
                }
                if (count >= MAX_WAIT) {
                    // if this happens it could be a misconfigured scan
                    log.error("Executor did not fully shutdown");
                }
            } catch (InterruptedException e) {
                // we were interrupted while waiting
            }
        }
    }

    @Override
    public void close() {
        stopAsync();
        try {
            awaitTerminated();
        } catch (Exception e) {

        }
        service.shutdownNow();
    }

    public void addVisitor(Function<ScannerChunk,ScannerChunk> visitorFunction) {
        visitorFunctions.add(visitorFunction);

    }

    @Deprecated(forRemoval = true)
    public void setTabletLocator(TabletLocator tl) {
        // no-op
    }

    public void setBackoffEnabled(boolean backoffEnabled) {
        this.backoffEnabled = backoffEnabled;
    }

    @Override
    public boolean canRun(ScannerChunk chunk) {
        if (!scannerBatches.hasNext() && runnableCount.get() <= serverMap.get(chunk.getLastKnownLocation()).get()) {
            return true;
        }
        AtomicInteger failCount = serverFailureMap.get(chunk.getLastKnownLocation());
        if (null == failCount || failCount.get() == 0) {
            return true;
        }
        int val = failCount.decrementAndGet();
        if (val <= 0) {
            failCount.set(0);
            return true;
        } else {
            // must run if we have no other work to do.
            if (runnableCount.get() <= serverMap.get(chunk.getLastKnownLocation()).get()) {
                return true;
            }
        }
        return false;
    }

    public void setSpeculativeScanning(boolean speculative) {
        this.speculativeScanning = speculative;
    }

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        t.interrupt();
        close();
        uncaughtExceptionHandler.uncaughtException(t, e);
    }
}
