package datawave.query.tables.async;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import datawave.core.query.configuration.Result;
import datawave.query.tables.AccumuloResource;
import datawave.query.tables.ResourceQueue;
import datawave.query.tables.stats.ScanSessionStats;

/**
 * Intended for a single lookup
 *
 */
public class SpeculativeScan extends Scan implements FutureCallback<Scan>, UncaughtExceptionHandler {
    private static final Logger log = Logger.getLogger(SpeculativeScan.class);

    protected AtomicInteger successCount = new AtomicInteger(0);

    protected AtomicInteger failureCount = new AtomicInteger(0);

    protected List<Scan> scans;

    protected List<Future<Scan>> scanFutures;

    protected AtomicBoolean finished = new AtomicBoolean(false);

    protected ExecutorService service = null;

    protected LinkedBlockingDeque<Result> myResultQueue;
    protected ReentrantLock writeControl = new ReentrantLock();

    protected Throwable failure = null;

    private class SpeculativeScanThreadFactory implements ThreadFactory {

        private ThreadFactory dtf = Executors.defaultThreadFactory();
        private int threadNum = 1;
        private StringBuilder threadIdentifier;
        private UncaughtExceptionHandler handler;

        public SpeculativeScanThreadFactory(StringBuilder threadName, UncaughtExceptionHandler handler)

        {
            this.handler = handler;
            this.threadIdentifier = threadName;
        }

        public Thread newThread(Runnable r) {
            Thread thread = dtf.newThread(r);
            thread.setName("Speculative Scan " + threadIdentifier + " -" + threadNum++);
            thread.setDaemon(true);
            thread.setUncaughtExceptionHandler(handler);
            return thread;
        }

    }

    public SpeculativeScan(String localTableName, Set<Authorizations> localAuths, ScannerChunk chunk, ResourceQueue delegatorReference,
                    Class<? extends AccumuloResource> delegatedResourceInitializer, ArrayBlockingQueue<Result> results, ExecutorService callingService) {
        super(localTableName, localAuths, chunk, delegatorReference, delegatedResourceInitializer, results, callingService);
        scans = Lists.newArrayList();
        scanFutures = Lists.newArrayList();
        myResultQueue = new LinkedBlockingDeque<>();
        service = Executors.newFixedThreadPool(2, new SpeculativeScanThreadFactory(new StringBuilder(chunk.getQueryId()), this));
        service = MoreExecutors.listeningDecorator(service);
        disableStats();
    }

    public boolean addScan(Scan scan) {

        synchronized (scanFutures) {

            if (finished.get())
                return false;

            scan.disableStats();
            scans.add(scan);
            ListenableFuture<Scan> future = (ListenableFuture<Scan>) service.submit(scan);
            scanFutures.add(future);
            Futures.addCallback(future, this, MoreExecutors.newDirectExecutorService());
        }
        return true;
    }

    public boolean finished() {
        return finished.get();
    }

    @Subscribe
    public void registerShutdown(ShutdownEvent event) {
        continueMultiScan = false;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.util.concurrent.Callable#call()
     */
    @Override
    public Scan call() throws Exception {

        while (!finished.get() && !caller.isShutdown() && !service.isShutdown()) {
            if (log.isTraceEnabled()) {
                log.trace("here with " + myResultQueue.size() + " " + " " + finished.get() + " " + service.isShutdown());
            }
            Thread.sleep(10);
            if (Thread.interrupted()) {
                throw new InterruptedException("Interrupted while parking");
            }
        }

        if (failure != null) {
            log.error("Exception in speculative scan detected", failure);
            throw new RuntimeException(failure);
        }

        return this;
    }

    /**
     * Override this for your specific implementation.
     *
     * @param lastKey
     *            a lastkey
     * @param previousRange
     *            the previous range
     */
    public Range buildNextRange(final Key lastKey, final Range previousRange) {
        return scans.iterator().next().buildNextRange(lastKey, previousRange);
    }

    public ScanSessionStats getStats() {
        return myStats;
    }

    @Override
    public void onSuccess(Scan result) {
        if (log.isTraceEnabled()) {
            log.trace("got result for " + result);
        }

        successCount.incrementAndGet();

        if (finished.get())
            return;
        /**
         * If we can't acquire the semaphore this means that another thread succeeded and our results are to be ignored.
         */
        if (!writeControl.tryLock()) {

            return;
        }

        try {

            while (!myResultQueue.isEmpty()) {
                results.put(myResultQueue.poll(2, TimeUnit.MILLISECONDS));
                if (log.isTraceEnabled())
                    log.trace("status" + Thread.interrupted() + " " + caller.isShutdown() + " " + service.isShutdown());
                if (Thread.interrupted() || caller.isShutdown() || service.isShutdown() || finished.get()) {
                    if (log.isTraceEnabled())
                        log.trace("closing" + Thread.interrupted() + " " + caller.isShutdown() + " " + service.isShutdown());
                    close();
                    break;
                }
            }

            // only consider us finished if our scan
            // shows that we are finished.
            if (result.finished()) {
                // if we are finished, go ahead and close ourselves
                close();
            } else {
                if (!addScan(result)) {
                    close();
                }
            }

        } catch (InterruptedException e) {
            close();
            throw new RuntimeException(e);
        } finally {
            writeControl.unlock();
        }

    }

    protected void closeScans() {
        for (Scan scan : scans) {
            scan.close();
        }

    }

    @Override
    public void onFailure(Throwable t) {

        // if all failed, then return failure
        if (failureCount.incrementAndGet() >= scans.size()) {
            close();
            failure = t;
            throw new RuntimeException(t);
        }

    }

    public LinkedBlockingDeque<Result> getQueue() {
        return myResultQueue;
    }

    protected void setClose() {
        finished.set(true);
    }

    /**
     * Close the underlying futures and scanner sessions
     */
    public void close() {
        setClose();
        closeScans();
        service.shutdownNow();
    }

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        if (failure == null) {
            failure = e;
        }

        close();
    }

}
