package datawave.core.iterators;

import static datawave.core.iterators.DatawaveFieldIndexCachingIteratorJexl.EMPTY_CFS;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.log4j.Logger;

import com.google.common.collect.Multimap;

import datawave.query.exceptions.DatawaveIvaratorMaxResultsException;
import datawave.query.iterator.profile.QuerySpan;
import datawave.query.iterator.profile.SourceTrackingIterator;

public class IvaratorRunnable implements Runnable {

    private static final Logger log = Logger.getLogger(IvaratorRunnable.class);
    private final String taskName;
    private final Range boundingFiRange;
    private final TotalResults totalResults;
    private DatawaveFieldIndexCachingIteratorJexl ivarator;
    private boolean suspended = false;
    private AtomicBoolean suspendRequested = new AtomicBoolean(false);
    private AtomicBoolean running = new AtomicBoolean(false);
    private Key restartKey = null;
    private SortedKeyValueIterator<Key,Value> source;
    private Range seekRange;
    private QuerySpan querySpan = null;
    private int scanned = 0;
    private int matched = 0;
    private int nextCount = 0;

    public IvaratorRunnable(DatawaveFieldIndexCachingIteratorJexl ivarator, SortedKeyValueIterator<Key,Value> source, final Range boundingFiRange,
                    final Range seekRange, final TotalResults totalResults) {
        this.ivarator = ivarator;
        this.source = source;
        this.boundingFiRange = boundingFiRange;
        this.seekRange = seekRange;
        this.totalResults = totalResults;
        this.taskName = ivarator.getTaskName(boundingFiRange);
    }

    public void prepareForResume(DatawaveFieldIndexCachingIteratorJexl resumingIvarator) {
        // IvaratorRunnable runnable;
        if (!suspended || restartKey == null) {
            throw new IllegalStateException("IvaratorRunnable not suspended.  Can not prepareForResume");
        }
        ivarator = resumingIvarator;
        // use a new seek range that starts from restartKey of the suspended IvaratorRunnable
        seekRange = new Range(restartKey, true, boundingFiRange.getEndKey(), boundingFiRange.isEndKeyInclusive());
        // this will block until an ivarator source becomes available
        source = ivarator.takePoolSource();
        suspended = false;
        suspendRequested.set(false);
    }

    public String getTaskName() {
        return taskName;
    }

    public void suspend() {
        if (running.get()) {
            suspendRequested.set(true);
            // the run method should see suspended == true, save state, and exit
            // the state will be available via the IvaratorFuture during a subsequent call
            waitUntilComplete();
        }
    }

    public boolean isSupended() {
        return suspended;
    }

    public boolean isRunning() {
        return running.get();
    }

    public void waitUntilComplete() {
        if (running.get()) {
            synchronized (running) {
                while (running.get()) {
                    try {
                        running.wait();
                    } catch (InterruptedException e) {

                    }
                }
            }
        }
    }

    private boolean suspendNow(Key key) {
        boolean suspendNow = suspendRequested.get();
        if (suspendNow) {
            restartKey = key;
            suspended = true;
        }
        return suspendNow;
    }

    public DatawaveFieldIndexCachingIteratorJexl getIvarator() {
        return ivarator;
    }

    @Override
    public void run() {
        running.set(true);
        if (log.isDebugEnabled()) {
            if (seekRange.equals(boundingFiRange)) {
                log.debug(String.format("Starting IvaratorRunnable.run() for range %s", boundingFiRange));
            } else {
                log.debug(String.format("Resuming IvaratorRunnable.run() for range %s at key %s", boundingFiRange, restartKey));
            }
        }
        Key nextSeekKey = null;
        try {
            if (ivarator.getCollectTimingDetails() && source instanceof SourceTrackingIterator) {
                querySpan = ((SourceTrackingIterator) source).getQuerySpan();
            }

            // seek the source to a range covering the entire row....the bounding box will dictate the actual scan
            // if we are resuming the ivarator, then we will be seeking to where we left off when suspended
            source.seek(seekRange, EMPTY_CFS, false);
            scanned++;
            ivarator.getScannedKeys().incrementAndGet();

            // if this is a range iterator, build the composite-safe Fi range
            Range compositeSafeFiRange = (ivarator instanceof DatawaveFieldIndexRangeIteratorJexl)
                            ? ((DatawaveFieldIndexRangeIteratorJexl) ivarator).buildCompositeSafeFiRange(ivarator.getFiRow(), ivarator.getFiName(),
                                            ivarator.getFieldValue())
                            : null;

            while (source.hasTop()) {
                Key top = source.getTopKey();
                // if suspended, set the restartKey and exit the run method
                if (suspendNow(top)) {
                    break;
                }
                ivarator.checkTiming();

                // if we are setup for composite seeking, seek if we are out of range
                if (ivarator.getCompositeSeeker() != null && compositeSafeFiRange != null) {
                    String colQual = top.getColumnQualifier().toString();
                    String ingestType = colQual.substring(colQual.indexOf('\0') + 1, colQual.lastIndexOf('\0'));
                    String colFam = top.getColumnFamily().toString();
                    String fieldName = colFam.substring(colFam.indexOf('\0') + 1);

                    Collection<String> componentFields = null;
                    String separator = null;
                    Multimap<String,String> compositeToFieldMap = ivarator.getCompositeMetadata().getCompositeFieldMapByType().get(ingestType);
                    Map<String,String> compositeSeparatorMap = ivarator.getCompositeMetadata().getCompositeFieldSeparatorsByType().get(ingestType);
                    if (compositeToFieldMap != null && compositeSeparatorMap != null) {
                        componentFields = compositeToFieldMap.get(fieldName);
                        separator = compositeSeparatorMap.get(fieldName);
                    }

                    if (componentFields != null && separator != null && !ivarator.getCompositeSeeker().isKeyInRange(top, compositeSafeFiRange, separator)) {
                        boolean shouldSeek = false;

                        // top key precedes nextSeekKey
                        if (nextSeekKey != null && top.compareTo(nextSeekKey) < 0) {
                            // if we hit the seek threshold, seek
                            if (nextCount >= ivarator.getCompositeSeekThreshold())
                                shouldSeek = true;
                        }
                        // top key exceeds nextSeekKey, or nextSeekKey unset
                        else {
                            nextCount = 0;
                            nextSeekKey = null;

                            // get a new seek key
                            Key newStartKey = ivarator.getCompositeSeeker().nextSeekKey(new ArrayList<>(componentFields), top, compositeSafeFiRange, separator);
                            if (newStartKey != boundingFiRange.getStartKey() && newStartKey.compareTo(boundingFiRange.getStartKey()) > 0
                                            && newStartKey.compareTo(boundingFiRange.getEndKey()) <= 0) {
                                nextSeekKey = newStartKey;

                                // if we hit the seek threshold (i.e. if it is set to 0), seek
                                if (nextCount >= ivarator.getCompositeSeekThreshold())
                                    shouldSeek = true;
                            }
                        }

                        if (shouldSeek) {
                            source.seek(new Range(nextSeekKey, boundingFiRange.isStartKeyInclusive(), boundingFiRange.getEndKey(),
                                            boundingFiRange.isEndKeyInclusive()), EMPTY_CFS, false);

                            // reset next count and seek key
                            nextSeekKey = null;
                            nextCount = 0;
                        } else {
                            nextCount++;
                            source.next();
                        }

                        scanned++;
                        continue;
                    }
                }

                // terminate if timed out or cancelled
                if (ivarator.getSetControl().isCancelledQuery()) {
                    break;
                }

                if (suspendNow(top)) {
                    break;
                }

                if (ivarator.addKey(top)) {
                    matched++;
                    if (!totalResults.increment()) {
                        throw new DatawaveIvaratorMaxResultsException("Exceeded the maximum set size");
                    }
                }

                source.next();
                scanned++;
                ivarator.getScannedKeys().incrementAndGet();
            }
            if (suspended && log.isDebugEnabled()) {
                log.debug(String.format("Suspended IvaratorRunnable.run() for range %s at key %s", boundingFiRange, restartKey));
            }
        } catch (Exception e) {
            // throw the exception up which will be available via the Future
            log.error("Failed to complete fillSet(" + boundingFiRange + ")", e);
            throw new RuntimeException(e);
        } finally {
            // return the ivarator source back to the pool.
            ivarator.returnPoolSource(source);
            source = null;
            if (log.isDebugEnabled()) {
                StringBuilder builder = new StringBuilder();
                builder.append("Matched ").append(matched).append(" out of ").append(scanned).append(" for ").append(boundingFiRange).append(": ")
                                .append(ivarator);
                log.debug(builder.toString());
            }
            if (ivarator.getCollectTimingDetails() && ivarator.getQuerySpanCollector() != null && querySpan != null) {
                ivarator.getQuerySpanCollector().addQuerySpan(querySpan);
            }
            synchronized (running) {
                running.set(false);
                running.notify();
            }
        }
    }

    /**
     * A class to keep track of the total result size across all of the bounding ranges
     */
    public static class TotalResults {

        private final long maxResults;
        private AtomicLong size = new AtomicLong();

        public TotalResults(long maxResults) {
            this.maxResults = maxResults;
        }

        public boolean increment() {
            if (maxResults <= 0) {
                return true;
            }
            return size.incrementAndGet() <= maxResults;
        }

        public boolean add(long val) {
            if (maxResults <= 0) {
                return true;
            }
            return size.addAndGet(val) <= maxResults;
        }
    }
}
