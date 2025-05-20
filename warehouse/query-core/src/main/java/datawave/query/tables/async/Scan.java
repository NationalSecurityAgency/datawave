package datawave.query.tables.async;

import java.io.InterruptedIOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.clientImpl.ThriftScanner.ScanTimedOutException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.eventbus.Subscribe;

import datawave.core.query.configuration.Result;
import datawave.mr.bulk.RfileResource;
import datawave.query.tables.AccumuloResource;
import datawave.query.tables.AccumuloResource.ResourceFactory;
import datawave.query.tables.BatchResource;
import datawave.query.tables.ResourceQueue;
import datawave.query.tables.stats.ScanSessionStats;
import datawave.query.tables.stats.ScanSessionStats.TIMERS;

public class Scan implements Callable<Scan> {

    private static final Logger log = Logger.getLogger(Scan.class);
    public static final String SCAN_ID = "scan.id";

    protected ScannerChunk myScan;

    /**
     * last seen key, used for moving across the sliding window of ranges.
     */
    protected Key lastSeenKey;

    /**
     * Current range that we are using.
     */
    protected Range currentRange;

    protected boolean continueMultiScan;

    private ResourceQueue delegatorReference;

    protected BlockingQueue<Result> results;

    private String localTableName;

    private Set<Authorizations> localAuths;

    private Class<? extends AccumuloResource> delegatedResourceInitializer;

    protected ExecutorService caller;

    protected ScanSessionStats myStats;

    protected boolean initialized = false;

    private List<Function<ScannerChunk,ScannerChunk>> visitorFunctions = null;

    protected SessionArbiter arbiter = null;

    protected long timeout = -1;

    private AccumuloResource delegatedResource = null;

    public Scan(String localTableName, Set<Authorizations> localAuths, ScannerChunk chunk, ResourceQueue delegatorReference,
                    Class<? extends AccumuloResource> delegatedResourceInitializer, BlockingQueue<Result> results, ExecutorService callingService) {
        myScan = chunk;
        if (log.isTraceEnabled())
            log.trace("Size of ranges:  " + myScan.getRanges().size());
        continueMultiScan = true;
        this.delegatorReference = delegatorReference;
        this.results = results;
        this.localTableName = localTableName;
        this.localAuths = localAuths;
        this.delegatedResourceInitializer = delegatedResourceInitializer;
        this.caller = callingService;
        myStats = new ScanSessionStats();
        myStats.initializeTimers();
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    public void setVisitors(List<Function<ScannerChunk,ScannerChunk>> visitorFunctions) {
        this.visitorFunctions = visitorFunctions;
    }

    public List<Function<ScannerChunk,ScannerChunk>> getVisitors() {
        return this.visitorFunctions;
    }

    public boolean finished() {
        if (caller.isShutdown() && log.isTraceEnabled()) {
            log.trace("Prematurely shutting down because we were forced to stop");
        }
        return caller.isShutdown() || (currentRange == null && lastSeenKey == null);
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
                if (lastSeenKey == null || (currentRange != null && currentRange.getEndKey() != null && lastSeenKey.compareTo(currentRange.getEndKey()) >= 0)) {
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
                        currentRange = buildNextRange(lastSeenKey, currentRange);
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

                if (log.isTraceEnabled()) {
                    log.trace(lastSeenKey + ", using current range of " + myScan.getLastRange());
                    log.trace(lastSeenKey + ", using current range of " + currentRange);
                }
                if (log.isTraceEnabled())
                    log.trace("initialize resource " + currentRange + "'s resource with " + myScan.getOptions());
                for (IteratorSetting setting : myScan.getOptions().getIterators()) {
                    log.trace(setting.getName() + " " + setting.getOptions());
                }

                Class<? extends AccumuloResource> initializer = delegatedResourceInitializer;

                boolean docSpecific = RangeDefinition.isDocSpecific(currentRange);
                if (!docSpecific && !initializer.isAssignableFrom(RfileResource.class)) {
                    // this catches the case where a scanner was created with a RunningResource and a shard range was generated
                    // when bypassing accumulo with a RFileResource, do not override the initializer with a BatchResource
                    initializer = BatchResource.class;
                } else if (null != arbiter && timeout > 0) {

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

                delegatedResource = ResourceFactory.initializeResource(initializer, delegatedResource, localTableName, localAuths, currentRange)
                                .setOptions(myScan.getOptions());

                Iterator<Result> iter = Result.resultIterator(myScan.getContext(), delegatedResource.iterator());

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

                Result myEntry = null;
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

                    lastSeenKey = myEntry.getKey();
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

    private boolean isInterruptedException(Throwable t) {
        while (t != null && !(t instanceof InterruptedException || t instanceof InterruptedIOException)
                        && !(t.getMessage() != null && t.getMessage().contains("InterruptedException"))) {
            t = t.getCause();
        }
        return t != null;
    }

    static final AtomicLong scanIdFactory = new AtomicLong(0);

    private String getNewScanId() {
        long scanId = scanIdFactory.incrementAndGet();
        return Long.toHexString(scanId);
    }

    /**
     * Override this for your specific implementation.
     *
     * @param lastKey
     *            the last key
     * @param previousRange
     *            a previous range
     * @return a new range
     */
    public Range buildNextRange(final Key lastKey, final Range previousRange) {
        return new Range(lastKey.followingKey(PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME), true, previousRange.getEndKey(), previousRange.isEndKeyInclusive());
    }

    public ScanSessionStats getStats() {
        return myStats;
    }

    public void setSessionArbiter(SessionArbiter arbiter) {
        this.arbiter = arbiter;
    }

    public ScannerChunk getScannerChunk() {
        return myScan;
    }

    public String getScanLocation() {
        return myScan.getLastKnownLocation();
    }

    /**
     * Added because speculative scan could reach a condition by which we won't be closing the futures and therefore the batch scanner session won't close this
     * Scan
     */
    public void close() {
        if (null != delegatedResource) {
            try {
                delegatedResource.close();
            } catch (Exception e) {
                log.warn("Ignoring error on close", e);
            }
        }
    }

    /**
     * Disables Statistics for this scan.
     */
    public void disableStats() {
        myStats = null;
    }

}
