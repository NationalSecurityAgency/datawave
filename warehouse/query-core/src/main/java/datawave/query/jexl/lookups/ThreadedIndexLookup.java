package datawave.query.jexl.lookups;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.tables.ScannerFactory;
import datawave.webservice.common.logging.ThreadConfigurableLogger;
import org.apache.log4j.Logger;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class ThreadedIndexLookup extends IndexLookup {
    private static final Logger log = ThreadConfigurableLogger.getLogger(ThreadedIndexLookup.class);
    
    protected ExecutorService execService;
    
    public ThreadedIndexLookup(ShardQueryConfiguration config, ScannerFactory scannerFactory, boolean supportReference) {
        super(config, scannerFactory, supportReference);
    }
    
    protected long getRemainingTimeMillis(long startTimeMillis) {
        return Math.max(0L, config.getMaxIndexScanTimeMillis() - (System.currentTimeMillis() - startTimeMillis));
    }
    
    protected void timedScanWait(Future<Boolean> future, CountDownLatch startedLatch, long startTimeMillis) {
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
        
        long maxLookup = config.getMaxIndexScanTimeMillis();
        
        try {
            
            /**
             * Deal with the case we we let ourselves get interrupted AND support timeout.
             */
            
            boolean swallowTimeout = false;
            if (maxLookup <= 0 || maxLookup == Long.MAX_VALUE) {
                maxLookup = 1000;
                swallowTimeout = true;
            }
            
            /**
             * Continue in perpetuity iff we swallow the timeout. our state machine has three states 1) timeout exception and continue ( no max lookup ) 2)
             * timeout exception and except ( a max lookup specified ) 3) we receive a value under timeout and we break
             *
             */
            while (!execService.isShutdown() && !execService.isTerminated()) {
                try {
                    future.get((swallowTimeout) ? maxLookup : getRemainingTimeMillis(startTimeMillis), TimeUnit.MILLISECONDS);
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
        } catch (TimeoutException e) {
            future.cancel(true);
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
