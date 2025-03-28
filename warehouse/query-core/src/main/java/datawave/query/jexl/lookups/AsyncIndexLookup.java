package datawave.query.jexl.lookups;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import datawave.core.common.logging.ThreadConfigurableLogger;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.tables.ScannerFactory;

/**
 * Abstract index lookup which provides a framework for creating and populating the {@link IndexLookupMap} asynchronously in a separate thread. Async index
 * lookups may perform some setup in {@link #submit()}, but should not block on any running threads until {@link #lookup()} is called, and even then they should
 * only block for up to the specified timeout {@link ShardQueryConfiguration#getMaxIndexScanTimeMillis()}
 */
public abstract class AsyncIndexLookup extends IndexLookup {
    private static final Logger log = ThreadConfigurableLogger.getLogger(AsyncIndexLookup.class);

    protected boolean unfieldedLookup;

    protected ExecutorService execService;

    public AsyncIndexLookup(ShardQueryConfiguration config, ScannerFactory scannerFactory, boolean unfieldedLookup, ExecutorService execService) {
        super(config, scannerFactory);
        this.unfieldedLookup = unfieldedLookup;
        this.execService = execService;
    }

    /**
     * Non-blocking method which sets up the asynchronous task(s) and submits them to the execService. Implementations of this method should use appropriate
     * synchronization to prevent multiple task submissions.
     */
    public abstract void submit();

    protected long getRemainingTimeMillis(long startTimeMillis) {
        return Math.max(0L, config.getMaxIndexScanTimeMillis() - (System.currentTimeMillis() - startTimeMillis));
    }

    protected void timedScanWait(Future<Boolean> future, CountDownLatch startedLatch, CountDownLatch stoppedLatch, AtomicLong startTimeMillis, long timeout) {
        // this ensures that we don't wait for the future response until the task has started
        if (startedLatch != null) {
            try {
                startedLatch.await();
            } catch (InterruptedException e) {
                throw new UnsupportedOperationException("Interrupted while waiting for IndexLookup to start", e);
            }
        } else {
            throw new UnsupportedOperationException("Cannot wait for IndexLookup timed scan that wasn't started");
        }

        long maxLookup = timeout;

        try {

            // Deal with the case where we let ourselves get interrupted AND support timeout.
            boolean swallowTimeout = false;
            if (maxLookup <= 0 || maxLookup == Long.MAX_VALUE) {
                maxLookup = 1000;
                swallowTimeout = true;
            }

            // Continue in perpetuity iff we swallow the timeout. our state machine has three states 1) timeout exception and continue ( no max lookup ) 2)
            // timeout exception and except ( a max lookup specified ) 3) we receive a value under timeout and we break
            while (!execService.isShutdown() && !execService.isTerminated()) {
                try {
                    future.get((swallowTimeout) ? maxLookup : getRemainingTimeMillis(startTimeMillis.get()), TimeUnit.MILLISECONDS);
                } catch (TimeoutException e) {
                    if (swallowTimeout) {
                        continue;
                    } else {
                        throw e;
                    }
                }
                break;
            }

        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException | CancellationException e) {
            future.cancel(true);

            try {
                stoppedLatch.await();
            } catch (InterruptedException ex) {
                log.error("Interrupted waiting for canceled AsyncIndexLookup to complete.");
                throw new RuntimeException(ex);
            }

            if (log.isTraceEnabled())
                log.trace("Timed out ");
            // Only if not doing an unfielded lookup should we mark all fields as having an exceeded threshold
            if (!unfieldedLookup) {
                for (String field : fields) {
                    if (log.isTraceEnabled()) {
                        log.trace("field is " + field);
                        log.trace("fieldsToValues.isEmpty? " + indexLookupMap.isEmpty());
                    }
                    indexLookupMap.put(field, "");
                    indexLookupMap.get(field).setThresholdExceeded();
                }
            } else {
                indexLookupMap.setKeyThresholdExceeded();
            }
        }
    }
}
