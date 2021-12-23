package datawave.query.tables.async;

import com.google.common.base.Function;
import com.google.common.eventbus.Subscribe;
import datawave.query.tables.stats.ScanSessionStats;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;

import java.io.InterruptedIOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

public abstract class BaseScan<T extends BaseScan> implements Callable<T> {

    private static final Logger log = Logger.getLogger(BaseScan.class);
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

    protected String localTableName;

    protected Set<Authorizations> localAuths;

    protected ExecutorService caller;

    protected ScanSessionStats myStats;

    protected boolean initialized = false;

    protected List<Function<ScannerChunk,ScannerChunk>> visitorFunctions = null;

    protected SessionArbiter arbiter = null;

    protected long timeout = -1;


    public BaseScan(String localTableName, Set<Authorizations> localAuths, ScannerChunk chunk, ExecutorService callingService) {
        myScan = chunk;
        if (log.isTraceEnabled())
            log.trace("Size of ranges:  " + myScan.getRanges().size());
        continueMultiScan = true;
        this.localTableName = localTableName;
        this.localAuths = localAuths;
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
    public abstract T call() throws Exception;


    boolean isInterruptedException(Throwable t) {
        while (t != null && !(t instanceof InterruptedException || t instanceof InterruptedIOException)
                        && !(t.getMessage() != null && t.getMessage().contains("InterruptedException"))) {
            t = t.getCause();
        }
        return t != null;
    }
    
    static final AtomicLong scanIdFactory = new AtomicLong(0);
    
    String getNewScanId() {
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
    
    public String getScanLocation() {
        return myScan.getLastKnownLocation();
    }
    
    /**
     * Added because speculative scan could reach a condition by which we won't be closing the futures and therefore the batch scanner session won't close this
     * Scan
     */
    public abstract void close();
    
    /**
     * Disables Statistics for this scan.
     */
    public void disableStats() {
        myStats = null;
    }
    
}
