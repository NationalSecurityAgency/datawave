package datawave.query.jexl.lookups;

import java.util.Iterator;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import datawave.query.config.ShardQueryConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.log4j.Logger;

import datawave.query.tables.ScannerFactory;

public abstract class IndexLookup {
    
    /**
     * Limits lookup to only terms, agnostic of date and data type.
     */
    protected boolean limitToTerms = false;
    
    /**
     * Lookup a set of fieldname to values
     * 
     * @param config
     * @param scannerFactory
     * @return a map of fieldname to values
     */
    public abstract IndexLookupMap lookup(ShardQueryConfiguration config, ScannerFactory scannerFactory, long timer);
    
    public boolean supportReference() {
        return false;
    }
    
    public void setLimitToTerms(boolean limitToTerms) {
        this.limitToTerms = limitToTerms;
    }
    
    /**
     * 
     */
    protected boolean timedScan(final Iterator<Entry<Key,Value>> iter, final IndexLookupMap fieldsToValues, final ShardQueryConfiguration config,
                    final boolean unfieldedLookup, final Set<String> fields, final boolean isReverse, final long timeout, final Logger log) {
        
        long maxLookup = timeout;
        
        ExecutorService execService = Executors.newFixedThreadPool(1);
        
        Future<Boolean> future = execService.submit(createTimedCallable(iter, fieldsToValues, config, unfieldedLookup, fields, isReverse, timeout));
        
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
                    result = future.get(maxLookup, TimeUnit.MILLISECONDS);
                } catch (TimeoutException e) {
                    if (swallowTimeout)
                        continue;
                    else
                        throw e;
                }
                break;
            }
            
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            future.cancel(true);
            if (null != log && log.isTraceEnabled())
                log.trace("Timed out ");
            // Only if not doing an unfielded lookup should we mark all fields as having an exceeded threshold
            if (!unfieldedLookup) {
                for (String field : fields) {
                    if (null != log && log.isTraceEnabled()) {
                        log.trace("field is " + field);
                        log.trace("field is " + (null == fieldsToValues));
                    }
                    fieldsToValues.put(field, "");
                    fieldsToValues.get(field).setThresholdExceeded();
                }
            } else
                fieldsToValues.setKeyThresholdExceeded();
            
        } finally {
            execService.shutdownNow();
        }
        
        return result;
    }
    
    protected Callable<Boolean> createTimedCallable(Iterator<Entry<Key,Value>> iter, IndexLookupMap fieldsToValues, ShardQueryConfiguration config,
                    boolean unfieldedLookup, Set<String> fields, boolean isReverse, long timeout) {
        throw new UnsupportedOperationException("This operation isn't supported by this index lookup");
    }
    
}
