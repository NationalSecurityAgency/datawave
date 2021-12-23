package datawave.query.tables.async;

import com.google.common.base.Function;
import datawave.query.config.DocumentQueryConfiguration;
import datawave.query.tables.DocumentBatchResource;
import datawave.query.tables.DocumentResource;
import datawave.query.tables.DocumentResource.ResourceFactory;
import datawave.query.tables.ResourceQueue;
import datawave.query.tables.serialization.SerializedDocumentIfc;
import datawave.query.tables.stats.ScanSessionStats.TIMERS;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.clientImpl.ThriftScanner.ScanTimedOutException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class DocumentScanner extends BaseScan<DocumentScanner> {

    private static final Logger log = Logger.getLogger(DocumentScanner.class);
    public static final String SCAN_ID = "scan.id";
    protected DocumentQueryConfiguration config;

    /**
     * last seen key, used for moving across the sliding window of ranges.
     */
    protected SerializedDocumentIfc lastSeenKey;

    private ResourceQueue<DocumentResource> delegatorReference;

    protected BlockingQueue<SerializedDocumentIfc> results;

    private Class<? extends DocumentResource> delegatedResourceInitializer;

    private DocumentResource delegatedResource = null;

    public DocumentScanner(DocumentQueryConfiguration config, String localTableName, Set<Authorizations> localAuths, ScannerChunk chunk, ResourceQueue<DocumentResource> delegatorReference,
                           Class<? extends DocumentResource> delegatedResourceInitializer, BlockingQueue<SerializedDocumentIfc> results, ExecutorService callingService) {
        super(localTableName,localAuths,chunk,callingService);

        this.config=config;
        if (log.isTraceEnabled())
            log.trace("Size of ranges:  " + myScan.getRanges().size());
        this.delegatorReference = delegatorReference;
        this.results = results;
        this.delegatedResourceInitializer = delegatedResourceInitializer;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.concurrent.Callable#call()
     */
    @Override
    public DocumentScanner call() throws Exception {
        try {
            
            /**
             * Even though we were delegated a resource, we have not actually been provided the plumbing to run it. Note, below, that we initialize the resource
             * through the resource factory from a running resource.
             */
            
            if (!initialized) {
                if (null != visitorFunctions) {
                    for (Function<ScannerChunk,ScannerChunk> fx : visitorFunctions) {
                        myScan = fx.apply(myScan);
                    }
                }
                initialized = true;
            }
            
            do {
                if (null != myStats)
                    myStats.getTimer(TIMERS.SCANNER_START).resume();
                delegatedResource = delegatorReference.getScannerResource();
                if (log.isTraceEnabled())
                    log.trace("last seen " + lastSeenKey + " " + currentRange);
                // if we have just started or we are at the end of the
                // current range. pop the next range
                Key lsk = null;
                if (null != lastSeenKey){
                    lsk = lastSeenKey.computeKey();
                }
                if (lastSeenKey == null || (currentRange != null && currentRange.getEndKey() != null && lsk.compareTo(currentRange.getEndKey()) >= 0)) {
                    currentRange = myScan.getNextRange();
                    
                    // short circuit and exit
                    if (null == currentRange) {
                        lastSeenKey = null;
                        if (log.isTraceEnabled())
                            log.trace("Leaving");
                        if (null != myStats)
                            myStats.getTimer(TIMERS.SCANNER_START).suspend();
                        return this;
                    }
                    
                    if (log.isTraceEnabled())
                        log.trace("current range is " + currentRange);
                } else {
                    // adjust the end key range.
                    if (log.isTraceEnabled())
                        log.trace("Building new range from " + lastSeenKey);
                    try {
                        currentRange = buildNextRange(lsk, currentRange);
                    } catch (IllegalArgumentException e) {
                        // we are beyond the start range.
                        
                        /**
                         * same net effect, but instead we get to follow the logging better if we pop here and trace
                         */
                        if (log.isTraceEnabled())
                            log.trace(lastSeenKey + " is lastseenKey, previous range is " + currentRange);
                        // we are beyond the start range.
                        currentRange = myScan.getNextRange();
                        if (log.isTraceEnabled())
                            log.trace(lastSeenKey + " is lastseenKey, new range that we have popped is " + currentRange);
                        // short circuit and exit
                        if (null == currentRange) {
                            lastSeenKey = null;
                            log.trace("Leaving");
                            return this;
                        }
                    }
                }
                
                boolean docSpecific = RangeDefinition.isDocSpecific(currentRange);
                
                if (log.isTraceEnabled()) {
                    log.trace(lastSeenKey + ", using current range of " + myScan.getLastRange());
                    log.trace(lastSeenKey + ", using current range of " + currentRange);
                }
                if (log.isTraceEnabled())
                    log.trace("initialize resource " + currentRange + "'s resource with " + myScan.getOptions());
                for (IteratorSetting setting : myScan.getOptions().getIterators()) {
                    log.trace(setting.getName() + " " + setting.getOptions());
                }
                
                Class<? extends DocumentResource> initializer = delegatedResourceInitializer;
                
                if (!docSpecific) {
                    initializer = DocumentBatchResource.class;
                } else {
                    
                    if (null != arbiter && timeout > 0) {
                        
                        myScan.getOptions().setTimeout(timeout, TimeUnit.MILLISECONDS);
                        
                        if (!arbiter.canRun(myScan)) {
                            if (log.isInfoEnabled()) {
                                log.info("Not running " + currentRange);
                            }
                            if (log.isTraceEnabled()) {
                                log.trace("Not running scan as we have other work to do, and this server is unresponsive");
                            }
                            return this;
                        } else {
                            if (log.isTraceEnabled()) {
                                log.trace("Running scan as server is not unresponsive");
                            }
                        }
                    }
                    
                }
                
                String scanId = getNewScanId();
                if (log.isTraceEnabled()) {
                    log.trace("Setting " + SCAN_ID + " = " + scanId);
                }
                
                for (IteratorSetting setting : myScan.getOptions().getIterators()) {
                    myScan.getOptions().updateScanIteratorOption(setting.getName(), SCAN_ID, scanId);
                }
                
                if (log.isTraceEnabled()) {
                    for (IteratorSetting setting : myScan.getOptions().getIterators()) {
                        log.trace(setting.getName() + " " + setting.getOptions());
                    }
                    log.trace("Using " + initializer);
                }
                
                delegatedResource = (DocumentResource) ResourceFactory.initializeResource(initializer, delegatedResource, config, localTableName, localAuths, currentRange).setOptions(
                                myScan.getOptions());
                
                Iterator<SerializedDocumentIfc> iter = delegatedResource.iterator();
                
                if (null != myStats)
                    myStats.getTimer(TIMERS.SCANNER_START).suspend();
                // do not continue if we've reached the end of the corpus
                if (log.isTraceEnabled())
                    log.trace("has next? " + iter.hasNext());
                if (!iter.hasNext()) {
                    if (log.isTraceEnabled())
                        log.trace("We've started, but we have nothing to do on " + localTableName + " " + localAuths + " " + currentRange);
                    lastSeenKey = null;
                }

                SerializedDocumentIfc myEntry = null;
                if (null != myStats)
                    myStats.getTimer(TIMERS.SCANNER_ITERATE).resume();
                while (iter.hasNext()) {
                    if (!continueMultiScan)
                        throw new Exception("Stopped mid cycle");
                    myEntry = iter.next();
                    
                    while (!caller.isShutdown() && !results.offer(myEntry, 25, TimeUnit.MILLISECONDS)) {
                        if (log.isTraceEnabled())
                            log.trace("offering");
                    }
                    
                    if (log.isTraceEnabled())
                        log.trace("size of results " + results.size() + " is shutdown? " + caller.isShutdown());
                    
                    if (caller.isShutdown())
                        break;
                    
                    lastSeenKey = myEntry;
                    if (log.isTraceEnabled())
                        log.trace("last seen key is " + lastSeenKey);
                }
                if (!iter.hasNext())
                    lastSeenKey = null;
                
                // close early
                delegatorReference.close(delegatedResource);
                
                if (null != myStats)
                    myStats.getTimer(TIMERS.SCANNER_ITERATE).suspend();
                
                if (log.isTraceEnabled())
                    log.trace("not finished?" + !finished());
            } while (!finished());
        } catch (ScanTimedOutException e) {
            // this is okay. This means that we are being timesliced.
            myScan.addRange(currentRange);
        } catch (Exception e) {
            if (isInterruptedException(e)) {
                log.info("Scan interrupted");
            } else {
                log.error("Scan failed", e);
            }
            throw e;
        } finally {
            if (null != delegatedResource) {
                delegatorReference.close(delegatedResource);
            }
        }
        return this;
        
    }

    
    /**
     * Added because speculative scan could reach a condition by which we won't be closing the futures and therefore the batch scanner session won't close this
     * Scan
     */
    @Override
    public void close() {
        if (null != delegatedResource) {
            try {
                delegatedResource.close();
            } catch (Exception e) {
                log.warn("Ignoring error on close", e);
            }
        }
    }
    
}
