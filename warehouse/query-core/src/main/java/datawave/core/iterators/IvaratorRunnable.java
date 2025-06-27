package datawave.core.iterators;

import static datawave.core.iterators.DatawaveFieldIndexCachingIteratorJexl.EMPTY_CFS;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.collect.Multimap;

import datawave.query.exceptions.DatawaveIvaratorMaxResultsException;
import datawave.query.iterator.profile.QuerySpan;
import datawave.query.iterator.profile.SourceTrackingIterator;

public class IvaratorRunnable implements Runnable {

    private static final Logger log = Logger.getLogger(IvaratorRunnable.class);
    private final String taskName;
    private final Range boundingFiRange;
    private final String rangeHash;
    private final TotalResults totalResults;
    private DatawaveFieldIndexCachingIteratorJexl ivarator;
    private Status status;
    private final AtomicBoolean suspendRequested = new AtomicBoolean(false);
    private Exception suspendException = null;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private Key restartKey = null;
    private SortedKeyValueIterator<Key,Value> source;
    private Range seekRange;
    private Text fiRow;
    private QuerySpan querySpan = null;
    private String queryId;
    private long scanned = 0;
    private long matched = 0;
    private long nextCount = 0;
    private long currentStartTime = System.currentTimeMillis();
    private long createdTime = System.currentTimeMillis();
    private long totalTime = 0;
    private long restarts = 0;

    public enum Status {
        CREATED, RESUMABLE, RUNNING, SUSPENDED, COMPLETED, FAILED
    }

    public IvaratorRunnable(DatawaveFieldIndexCachingIteratorJexl ivarator, SortedKeyValueIterator<Key,Value> source, final Range boundingFiRange,
                    final Range seekRange, final Text fiRow, final String queryId, final TotalResults totalResults) {
        this.ivarator = ivarator;
        this.source = source;
        this.boundingFiRange = boundingFiRange;
        this.rangeHash = String.valueOf(Math.abs(boundingFiRange.hashCode() / 2));
        this.seekRange = seekRange;
        this.fiRow = fiRow;
        this.queryId = queryId;
        this.totalResults = totalResults;
        this.taskName = ivarator.getTaskName(boundingFiRange);
        this.setStatus(Status.CREATED);
    }

    public void setIvarator(DatawaveFieldIndexCachingIteratorJexl newIvarator) {
        this.ivarator = newIvarator;
    }

    public void prepareForResume(DatawaveFieldIndexCachingIteratorJexl resumingIvarator) {
        if (this.getStatus().equals(Status.CREATED) || this.getStatus().equals(Status.SUSPENDED)) {
            if (this.getStatus().equals(Status.SUSPENDED)) {
                if (this.restartKey == null) {
                    throw new IllegalStateException("restartKey == null; can not resume suspended IvaratorRunnable");
                }
                // use a new seek range that starts from restartKey of the suspended IvaratorRunnable
                this.seekRange = new Range(this.restartKey, true, this.boundingFiRange.getEndKey(), this.boundingFiRange.isEndKeyInclusive());
                this.setStatus(Status.RESUMABLE);
                this.suspendException = null;
                this.suspendRequested.set(false);
            }
            this.ivarator = resumingIvarator;
            // this will block until an ivarator source becomes available
            this.source = this.ivarator.takePoolSource();
        } else {
            throw new IllegalStateException("prepareForResume called with status " + this.getStatus());
        }
    }

    public String getTaskName() {
        return this.taskName;
    }

    // Gracefully stop the Runnable and then throw any supplied Exception so that
    // it will be returned when the user calls get() on the IvaratorFuture
    public void suspend(Exception suspendException) {
        if (this.running.get()) {
            this.suspendRequested.set(true);
            this.suspendException = suspendException;
            // The run method should call suspendNow(), see suspendedRequested == true, save state,
            // and exit. The state will be available to restart the IvaratorRunnable during a subsequent call
            // the duration is used to prevent the Thread from waiting indefinitely
            waitUntilFinished(60, TimeUnit.SECONDS);
        }
    }

    synchronized public Status getStatus() {
        return status;
    }

    synchronized private void setStatus(Status status) {
        this.status = status;
    }

    // Use !this.running.get() && !isStarted() to avoid a race condition with
    // this.running.get() which is only true while run() is being executed
    public void waitUntilStarted(long duration, TimeUnit timeUnit) {
        long currentDuration = timeUnit.toMillis(duration);
        long endTime = System.currentTimeMillis() + currentDuration;
        if (!this.running.get() && getStatus().equals(Status.CREATED)) {
            synchronized (this.running) {
                while (!this.running.get() && getStatus().equals(Status.CREATED) && currentDuration > 0) {
                    try {
                        // notify is called at the beginning of run()
                        this.running.wait(Math.min(currentDuration, 10));
                    } catch (InterruptedException e) {

                    }
                    currentDuration = endTime - System.currentTimeMillis();
                }
            }
        }
    }

    public void waitUntilFinished(long duration, TimeUnit timeUnit) {
        long currentDuration = timeUnit.toMillis(duration);
        long endTime = System.currentTimeMillis() + currentDuration;
        if (this.running.get()) {
            synchronized (this.running) {
                while (this.running.get() && currentDuration > 0) {
                    try {
                        // notify is called in the finally block at the end of run()
                        this.running.wait(Math.min(currentDuration, 10));
                    } catch (InterruptedException e) {

                    }
                    currentDuration = endTime - System.currentTimeMillis();
                }
            }
        }
    }

    private boolean suspendNow(Key key) throws Exception {
        boolean suspendNow = this.suspendRequested.get();
        if (suspendNow && key != null) {
            this.restartKey = key;
            // set status here to ensure that a restartKey is set
            this.setStatus(Status.SUSPENDED);
            if (suspendException != null) {
                throw suspendException;
            }
        }
        return suspendNow;
    }

    public DatawaveFieldIndexCachingIteratorJexl getIvarator() {
        return this.ivarator;
    }

    public String getQueryId() {
        return this.queryId;
    }

    public long getScanned() {
        return scanned;
    }

    public long getMatched() {
        return matched;
    }

    public String getRangeHash() {
        return rangeHash;
    }

    private String getIvaratorInfo() {
        return ivarator.getIvaratorInfo(fiRow.toString(), false);
    }

    public long getCurrentStartTime() {
        return currentStartTime;
    }

    public long getCreatedTime() {
        return createdTime;
    }

    @Override
    public void run() {
        synchronized (this.running) {
            this.running.set(true);
            this.running.notify();
        }
        this.setStatus(Status.RUNNING);
        currentStartTime = System.currentTimeMillis();
        if (this.seekRange.equals(this.boundingFiRange)) {
            log.info(String.format("Started IvaratorRunnable %s rangeHash:%s range:%s %s", getIvaratorInfo(), this.rangeHash, this.boundingFiRange,
                            this.ivarator.toStringNoQueryId()));
        } else {
            restarts++;
            log.info(String.format("Resumed IvaratorRunnable %s rangeHash:%s at key:%s %s", getIvaratorInfo(), this.rangeHash, this.restartKey,
                            this.ivarator.toStringNoQueryId()));
        }
        Key nextSeekKey = null;
        try {
            if (this.ivarator.getCollectTimingDetails() && this.source instanceof SourceTrackingIterator) {
                this.querySpan = ((SourceTrackingIterator) this.source).getQuerySpan();
            }

            // seek the source to a range covering the entire row....the bounding box will dictate the actual scan
            // if we are resuming the ivarator, then we will be seeking to where we left off when suspended
            this.source.seek(this.seekRange, EMPTY_CFS, false);

            // if this is a range iterator, build the composite-safe Fi range
            Range compositeSafeFiRange = (this.ivarator instanceof DatawaveFieldIndexRangeIteratorJexl)
                            ? ((DatawaveFieldIndexRangeIteratorJexl) this.ivarator).buildCompositeSafeFiRange(this.fiRow, this.ivarator.getFiName(),
                                            this.ivarator.getFieldValue())
                            : null;

            while (this.source.hasTop()) {
                Key top = this.source.getTopKey();
                // if suspended, set the restartKey and exit the run method
                if (suspendNow(top)) {
                    break;
                }
                this.ivarator.checkTiming();

                // if we are setup for composite seeking, seek if we are out of range
                if (this.ivarator.getCompositeSeeker() != null && compositeSafeFiRange != null) {
                    String colQual = top.getColumnQualifier().toString();
                    String ingestType = colQual.substring(colQual.indexOf('\0') + 1, colQual.lastIndexOf('\0'));
                    String colFam = top.getColumnFamily().toString();
                    String fieldName = colFam.substring(colFam.indexOf('\0') + 1);

                    Collection<String> componentFields = null;
                    String separator = null;
                    Multimap<String,String> compositeToFieldMap = this.ivarator.getCompositeMetadata().getCompositeFieldMapByType().get(ingestType);
                    Map<String,String> compositeSeparatorMap = this.ivarator.getCompositeMetadata().getCompositeFieldSeparatorsByType().get(ingestType);
                    if (compositeToFieldMap != null && compositeSeparatorMap != null) {
                        componentFields = compositeToFieldMap.get(fieldName);
                        separator = compositeSeparatorMap.get(fieldName);
                    }

                    if (componentFields != null && separator != null
                                    && !this.ivarator.getCompositeSeeker().isKeyInRange(top, compositeSafeFiRange, separator)) {
                        boolean shouldSeek = false;

                        // top key precedes nextSeekKey
                        if (nextSeekKey != null && top.compareTo(nextSeekKey) < 0) {
                            // if we hit the seek threshold, seek
                            if (this.nextCount >= this.ivarator.getCompositeSeekThreshold())
                                shouldSeek = true;
                        }
                        // top key exceeds nextSeekKey, or nextSeekKey unset
                        else {
                            this.nextCount = 0;
                            nextSeekKey = null;

                            // get a new seek key
                            Key newStartKey = this.ivarator.getCompositeSeeker().nextSeekKey(new ArrayList<>(componentFields), top, compositeSafeFiRange,
                                            separator);
                            if (newStartKey != this.boundingFiRange.getStartKey() && newStartKey.compareTo(this.boundingFiRange.getStartKey()) > 0
                                            && newStartKey.compareTo(this.boundingFiRange.getEndKey()) <= 0) {
                                nextSeekKey = newStartKey;

                                // if we hit the seek threshold (i.e. if it is set to 0), seek
                                if (this.nextCount >= this.ivarator.getCompositeSeekThreshold())
                                    shouldSeek = true;
                            }
                        }

                        if (shouldSeek) {
                            this.source.seek(new Range(nextSeekKey, this.boundingFiRange.isStartKeyInclusive(), this.boundingFiRange.getEndKey(),
                                            this.boundingFiRange.isEndKeyInclusive()), EMPTY_CFS, false);

                            // reset next count and seek key
                            nextSeekKey = null;
                            this.nextCount = 0;
                        } else {
                            this.nextCount++;
                            this.source.next();
                        }

                        this.scanned++;
                        continue;
                    }
                }

                // terminate if timed out or cancelled
                if (this.ivarator.getSetControl().isCancelledQuery()) {
                    break;
                }

                if (suspendNow(top)) {
                    break;
                }

                this.scanned++;
                this.ivarator.getScannedKeys().incrementAndGet();

                if (this.ivarator.addKey(top)) {
                    this.matched++;
                    if (!this.totalResults.increment()) {
                        throw new DatawaveIvaratorMaxResultsException("Exceeded the maximum set size");
                    }
                }
                this.source.next();
            }
            long currentTime = System.currentTimeMillis() - currentStartTime;
            this.totalTime += currentTime;
            String timing;
            if (restarts == 0) {
                timing = String.format("(total:%dms)", currentTime, this.totalTime);
            } else {
                timing = String.format("(restarts:%d current:%dms total:%dms)", restarts, currentTime, this.totalTime);
            }
            // Status.SUSPENDED is set in suspendNow to ensure that a restartKey is set
            if (this.getStatus().equals(Status.SUSPENDED)) {
                log.info(String.format("Suspended IvaratorRunnable %s rangeHash:%s at key:%s %s timing:%s", getIvaratorInfo(), this.rangeHash, this.restartKey,
                                this.ivarator.toStringNoQueryId(), timing));
            } else {
                log.info(String.format("Completed IvaratorRunnable %s rangeHash:%s range:%s %s timing:%s matched %d of %d keys", getIvaratorInfo(),
                                this.rangeHash, this.boundingFiRange, this.ivarator.toStringNoQueryId(), timing, this.matched, this.scanned));
                this.setStatus(Status.COMPLETED);
            }
        } catch (Exception e) {
            // throw the exception up which will be available via the Future
            log.error(String.format("Failed IvaratorRunnable %s rangeHash:%s %s", getIvaratorInfo(), this.rangeHash, this.ivarator.toStringNoQueryId()), e);
            this.setStatus(Status.FAILED);
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            } else {
                throw new RuntimeException(e);
            }
        } finally {
            // return the ivarator source back to the pool.
            this.ivarator.returnPoolSource(this.source);
            this.source = null;
            if (this.ivarator.getCollectTimingDetails() && this.ivarator.getQuerySpanCollector() != null && this.querySpan != null) {
                this.ivarator.getQuerySpanCollector().addQuerySpan(this.querySpan);
            }
            synchronized (this.running) {
                this.running.set(false);
                this.running.notify();
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
            if (this.maxResults <= 0) {
                return true;
            }
            return this.size.incrementAndGet() <= this.maxResults;
        }

        public boolean add(long val) {
            if (this.maxResults <= 0) {
                return true;
            }
            return this.size.addAndGet(val) <= this.maxResults;
        }
    }
}
