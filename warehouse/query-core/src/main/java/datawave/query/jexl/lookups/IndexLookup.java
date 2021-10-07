package datawave.query.jexl.lookups;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.tables.ScannerFactory;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class IndexLookup {
    
    /**
     * Limits lookup to only terms, agnostic of date and data type.
     */
    protected boolean limitToTerms = false;
    
    protected Future<Boolean> timedScanFuture;
    
    protected long lookupStartTimeMillis = Long.MAX_VALUE;
    protected CountDownLatch lookupStartedLatch;
    
    protected ScannerFactory scannerFactory;
    protected long timer;
    protected ExecutorService execService;
    
    protected Iterator<Entry<Key,Value>> iter;
    protected IndexLookupMap fieldsToValues;
    protected ShardQueryConfiguration config;
    protected boolean unfieldedLookup;
    protected Set<String> fields;
    protected boolean isReverse;
    protected long timeout;
    protected Logger log;
    
    public abstract void lookupAsync(ShardQueryConfiguration config, ScannerFactory scannerFactory, long timer, ExecutorService execService);
    
    public abstract IndexLookupMap lookupWait();
    
    public boolean supportReference() {
        return false;
    }
    
    public void setLimitToTerms(boolean limitToTerms) {
        this.limitToTerms = limitToTerms;
    }
    
    public boolean hasStarted() {
        boolean started = false;
        if (lookupStartTimeMillis != Long.MAX_VALUE) {
            started = true;
        }
        return started;
    }
    
    /**
     *
     */
    protected void timedScanAsync(final Iterator<Entry<Key,Value>> iter, final IndexLookupMap fieldsToValues, final ShardQueryConfiguration config,
                    final boolean unfieldedLookup, final Set<String> fields, final boolean isReverse, final long timeout, final Logger log,
                    ExecutorService execService) {
        this.iter = iter;
        this.fieldsToValues = fieldsToValues;
        this.config = config;
        this.unfieldedLookup = unfieldedLookup;
        this.fields = fields;
        this.isReverse = isReverse;
        this.timeout = timeout;
        this.log = log;
        this.execService = execService;
        
        this.lookupStartedLatch = new CountDownLatch(1);
        this.timedScanFuture = execService.submit(new IndexLookupCallable(createTimedCallable(iter, fieldsToValues, config, unfieldedLookup, fields, isReverse,
                        timeout), this));
    }
    
    protected boolean timedScanWait() {
        // this ensures that we don't wait for the future response until the task has started
        if (lookupStartedLatch != null) {
            try {
                lookupStartedLatch.await();
            } catch (InterruptedException e) {
                throw new UnsupportedOperationException("Interrupted while waiting for IndexLookup to start", e);
            }
        } else {
            throw new UnsupportedOperationException("Cannot wait for IndexLookup timed scan that wasn't started");
        }
        
        long maxLookup = timeout;
        
        boolean result = false;
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
                    result = timedScanFuture.get((swallowTimeout) ? maxLookup : getRemainingTimeMillis(), TimeUnit.MILLISECONDS);
                } catch (TimeoutException e) {
                    if (swallowTimeout)
                        continue;
                    else
                        throw e;
                }
                break;
            }
            
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            timedScanFuture.cancel(true);
            if (null != log && log.isTraceEnabled())
                log.trace("Timed out ");
            // Only if not doing an unfielded lookup should we mark all fields as having an exceeded threshold
            if (!unfieldedLookup) {
                for (String field : fields) {
                    if (null != log && log.isTraceEnabled()) {
                        log.trace("field is " + field);
                        log.trace("fieldsToValues.isEmpty? " + fieldsToValues.isEmpty());
                    }
                    fieldsToValues.put(field, "");
                    fieldsToValues.get(field).setThresholdExceeded();
                }
            } else {
                fieldsToValues.setKeyThresholdExceeded();
            }
        }
        
        return result;
    }
    
    protected long getRemainingTimeMillis() {
        return Math.max(0L, timeout - (System.currentTimeMillis() - lookupStartTimeMillis));
    }
    
    protected Callable<Boolean> createTimedCallable(Iterator<Entry<Key,Value>> iter, IndexLookupMap fieldsToValues, ShardQueryConfiguration config,
                    boolean unfieldedLookup, Set<String> fields, boolean isReverse, long timeout) {
        throw new UnsupportedOperationException("This operation isn't supported by this index lookup");
    }
    
    private static class IndexLookupCallable implements Callable<Boolean> {
        private final Callable<Boolean> callable;
        private final IndexLookup indexLookup;
        
        public IndexLookupCallable(Callable<Boolean> callable, IndexLookup indexLookup) {
            this.callable = callable;
            this.indexLookup = indexLookup;
        }
        
        @Override
        public Boolean call() throws Exception {
            indexLookup.lookupStartTimeMillis = System.currentTimeMillis();
            if (indexLookup.lookupStartedLatch != null) {
                indexLookup.lookupStartedLatch.countDown();
            }
            return callable.call();
        }
    }
}
