package datawave.query.tables;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.MoreExecutors;
import datawave.query.config.DocumentQueryConfiguration;
import datawave.query.tables.serialization.SerializedDocumentIfc;
import datawave.query.tables.stats.ScanSessionStats;
import datawave.query.tables.stats.StatsListener;
import datawave.webservice.query.Query;
import datawave.webservice.query.util.QueryUncaughtExceptionHandler;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * This will handle running a scan against a set of ranges. The actual scan is performed in a separate thread which places the results in a result queue. The
 * result queue is polled in the actual next() and hasNext() calls. Note that the uncaughtExceptionHandler from the Query is used to pass exceptions up which
 * will also fail the overall query if something happens. If this is not desired then a local handler should be set.
 */
public abstract class BaseScannerSession<T> extends AbstractExecutionThreadService implements Iterator<T> {


    private static final Logger log = Logger.getLogger(BaseScannerSession.class);
    /**
     * Table to which this scanner will connect.
     */
    protected String tableName;

    /**
     * Authorization set
     */
    protected Set<Authorizations> auths;

    /**
     * Max results to return at any given time.
     */
    protected int maxResults;


    /**
     * Last range in our sorted list of ranges.
     */
    protected Range lastRange;

    /**
     * Stack of ranges for us to progress through within this scanner queue.
     */
    protected ConcurrentLinkedQueue<Range> ranges;

    protected QueryUncaughtExceptionHandler uncaughtExceptionHandler = null;


    /**
     * Current range that we are using.
     */
    protected Range currentRange;

    protected volatile boolean forceClose = false;

    /**
     * Scanner options.
     */
    protected SessionOptions options = null;

    protected Query settings;

    protected ScanSessionStats stats = null;

    protected ExecutorService statsListener = null;

    protected boolean accrueStats;

    protected boolean isFair = true;


    /**
     * Result queue, providing us objects
     */
    protected ArrayBlockingQueue<T> resultQueue;

    /**
     * Current entry to return. this will be popped from the result queue.
     */
    protected T currentEntry;

    protected Class<? extends Resource<T>> delegatedResourceInitializer;

    protected Resource<T> delegatedResource = null;


    /**
     * Delegates scanners to us, blocking if none are available or used by other sources.
     */
    protected ResourceQueue sessionDelegator;


    public BaseScannerSession(String tableName, Set<Authorizations> auths, ResourceQueue delegator, int maxResults, Query settings, SessionOptions options,
                                  Collection<Range> ranges) {

        Preconditions.checkNotNull(options);
        Preconditions.checkNotNull(delegator);

        this.options = options;
        // build a stack of ranges
        this.ranges = new ConcurrentLinkedQueue<>();

        this.tableName = tableName;
        this.auths = auths;

        if (null != ranges && !ranges.isEmpty()) {
            List<Range> rangeList = Lists.newArrayList(ranges);
            Collections.sort(rangeList);

            this.ranges.addAll(ranges);
            lastRange = Iterables.getLast(rangeList);

        }

        resultQueue = Queues.newArrayBlockingQueue(maxResults);

        sessionDelegator = delegator;

        currentEntry = null;

        this.maxResults = maxResults;

        this.settings = settings;

        if (this.settings != null) {
            this.uncaughtExceptionHandler = this.settings.getUncaughtExceptionHandler();
        }

        // ensure we have an exception handler
        if (this.uncaughtExceptionHandler == null) {
            this.uncaughtExceptionHandler = new QueryUncaughtExceptionHandler();
        }

        delegatedResourceInitializer = getRunningResourceClass(); 

    }

    protected abstract Class<? extends Resource<T>> getRunningResourceClass();

    /*
     * (non-Javadoc)
     *
     * @see java.util.Iterator#remove()
     */
    @Override
    public void remove() {
        // do nothing.

    }


    /**
     * Override this for your specific implementation.
     *
     * @param lastKey
     *            the last key
     * @param previousRange
     *            the previous range
     * @return a new range
     */
    public Range buildNextRange(final Key lastKey, final Range previousRange) {
        return new Range(lastKey.followingKey(PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME), true, previousRange.getEndKey(), previousRange.isEndKeyInclusive());
    }

    protected long getPollTime() {
        return 1;
    }

    /**
     * Place all timers in a suspended state.
     */
    protected void initializeTimers() {
        stats.getTimer(ScanSessionStats.TIMERS.HASNEXT).start();
        stats.getTimer(ScanSessionStats.TIMERS.HASNEXT).suspend();

        stats.getTimer(ScanSessionStats.TIMERS.SCANNER_ITERATE).start();
        stats.getTimer(ScanSessionStats.TIMERS.SCANNER_ITERATE).suspend();

        stats.getTimer(ScanSessionStats.TIMERS.SCANNER_START).start();
        stats.getTimer(ScanSessionStats.TIMERS.SCANNER_START).suspend();

    }


    @Override
    protected Executor executor() {
        return command -> {
            String name = serviceName();
            Preconditions.checkNotNull(name);
            Preconditions.checkNotNull(command);
            Thread result = MoreExecutors.platformThreadFactory().newThread(command);
            try {
                result.setName(name);
                result.setUncaughtExceptionHandler(uncaughtExceptionHandler);
            } catch (SecurityException e) {
                // OK if we can't set the name in this environment.
            }
            result.start();
        };
    }

    /**
     * Sets the ranges for the given scannersession.
     *
     * @param ranges
     * @return
     */
    public BaseScannerSession setRanges(Collection<Range> ranges) {
        Preconditions.checkNotNull(ranges);
        // ensure that we are not already running
        Preconditions.checkArgument(!isRunning());
        List<Range> rangeList = Lists.newArrayList(ranges);
        Collections.sort(rangeList);
        this.ranges.clear();
        this.ranges.addAll(rangeList);
        lastRange = Iterables.getLast(rangeList);
        return this;

    }

    /*
     * (non-Javadoc)
     *
     * @see java.util.Iterator#next()
     *
     * Note that this method needs to check the uncaught exception handler and propogate any set throwables.
     */
    @Override
    public T next() {
        try {
            T retVal = currentEntry;
            currentEntry = null;
            return retVal;
        } finally {
            if (uncaughtExceptionHandler.getThrowable() != null) {
                log.error("Exception discovered on next call", uncaughtExceptionHandler.getThrowable());
                throw new RuntimeException(uncaughtExceptionHandler.getThrowable());
            }
        }
    }

    protected abstract int scannerInvariant(final Iterator<T> iter);

    /**
     * Set the scanner options
     *
     * @param options
     *            options to set
     * @return scanner options
     */
    public BaseScannerSession setOptions(SessionOptions options) {
        Preconditions.checkNotNull(options);
        this.options = options;
        return this;

    }

    /**
     * Return scanner options.
     *
     * @return scanner options
     */
    public SessionOptions getOptions() {
        return this.options;
    }

    protected void waitUntilCapacity() throws InterruptedException {
        while (resultQueue.remainingCapacity() > 0) {
            Thread.sleep(500);
        }
    }

    protected Range getCurrentRange() {
        return currentRange;
    }

    protected void flush() {

    }

    protected boolean flushNeeded() {
        return false;
    }

    /**
     * Get last Range.
     *
     * @return last Range
     */
    protected Range getLastRange() {
        return lastRange;
    }

    /**
     * Get last key.
     *
     * @return last key
     */
    protected abstract Key getLastKey();


    protected abstract void findTop() throws Exception;

    /*
     * (non-Javadoc)
     *
     * @see com.google.common.util.concurrent.AbstractExecutionThreadService#run()
     *
     * Note that this method must set exceptions on the uncaughtExceptionHandler, otherwise any failures will be completed ignored/dropped.
     */
    @Override
    protected void run() throws Exception {
        try {
            while (isRunning()) {
                findTop();
            }

            flush();
        } catch (Exception e) {
            uncaughtExceptionHandler.uncaughtException(Thread.currentThread(), e);
            throw new RuntimeException(e);
        }
    }

    public ScanSessionStats getStatistics() {
        return stats;
    }

    public BaseScannerSession<T> applyStats(ScanSessionStats stats) {
        if (null != stats) {
            Preconditions.checkArgument(this.stats == null);
            this.stats = stats;
            statsListener = Executors.newFixedThreadPool(1);
            addListener(new StatsListener(stats, statsListener), statsListener);
        }
        return this;
    }


    public abstract void close();


    public void setFairness(boolean fairness) {
        isFair = fairness;

    }

    public void setMaxResults(int maxResults) {
        this.maxResults = maxResults;

    }

}
