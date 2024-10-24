package datawave.query.tables;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.collections4.iterators.PeekingIterator;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.MoreExecutors;

import datawave.core.query.configuration.Result;
import datawave.microservice.query.Query;
import datawave.mr.bulk.RfileScanner;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.index.lookup.IndexInfo;
import datawave.query.index.lookup.IndexMatch;
import datawave.query.index.lookup.ShardEquality;
import datawave.query.tables.stats.ScanSessionStats.TIMERS;

/**
 * Purpose: Extends Scanner session so that we can modify how we build our subsequent ranges. Breaking this out cleans up the code. May require implementation
 * specific details if you are using custom iterators, as we are reinitializing a seek
 * <p>
 * Design: Extends Scanner session and only overrides the buildNextRange.
 * <p>
 * The {@link datawave.query.index.lookup.RangeStream} configures the iterator running against the global index.
 * <p>
 * Typically the iterator is a {@link datawave.query.index.lookup.CreateUidsIterator} or variant.
 * <p>
 * The iterator returns a tuple of shard - {@link IndexInfo} object pairs, each shard representing and day or shard range.
 * <p>
 * Results from the iterator are put onto the currentQueue and then flushed into the resultQueue. Under certain circumstances these results may be modified
 * final to the prior flush into the resultQueue.
 * <p>
 * The RangeStreamScanner supports "seeking" the global index iterator. Because the RangeStreamScanner supports a {@link PeekingIterator} some implementation
 * details are not immediately obvious. For more information, see {@link #seek(String)}.
 */
public class RangeStreamScanner extends ScannerSession implements Callable<RangeStreamScanner> {

    private static final Logger log = Logger.getLogger(RangeStreamScanner.class);
    // simply compare the strings. no need for a date formatter
    protected static final int DATE_CF_LENGTH = 8;
    protected boolean seenUnexpectedKey = false;
    protected ArrayDeque<Result> currentQueue;

    protected Result prevDay = null;

    protected ReentrantReadWriteLock queueLock = new ReentrantReadWriteLock(true);

    protected Lock readLock;
    protected Lock writeLock;

    volatile boolean finished = false;

    ExecutorService myExecutor;

    // If this flag is true, we build the next range using the seekShard.
    public boolean seeking = false;
    protected String seekShard = null;

    protected ScannerFactory scannerFactory;

    @Override
    protected String serviceName() {
        String id = "NoQueryId";
        if (null != settings && null != settings.getId()) {
            id = settings.getId().toString();
        }
        return "RangeStreamScanner (" + id + ")";
    }

    public RangeStreamScanner(String tableName, Set<Authorizations> auths, ResourceQueue delegator, int maxResults, Query settings) {
        super(tableName, auths, delegator, maxResults, settings);
        delegatedResourceInitializer = BatchResource.class;
        currentQueue = Queues.newArrayDeque();
        readLock = queueLock.readLock();
        writeLock = queueLock.writeLock();
        myExecutor = MoreExecutors.newDirectExecutorService();
        if (null != stats)
            initializeTimers();
    }

    public RangeStreamScanner(String tableName, Set<Authorizations> auths, ResourceQueue delegator, int maxResults, Query settings, SessionOptions options,
                    Collection<Range> ranges) {
        super(tableName, auths, delegator, maxResults, settings, options, ranges);
        delegatedResourceInitializer = BatchResource.class;
        currentQueue = Queues.newArrayDeque();
        readLock = queueLock.readLock();
        writeLock = queueLock.writeLock();
        myExecutor = MoreExecutors.newDirectExecutorService();
        if (null != stats)
            initializeTimers();
    }

    public RangeStreamScanner(ScannerSession other) {
        this(other.tableName, other.auths, other.sessionDelegator, other.maxResults, other.settings, other.options, other.ranges);
    }

    public void setExecutor(ExecutorService service) {
        myExecutor = service;
    }

    public RangeStreamScanner setScannerFactory(ScannerFactory factory) {
        this.scannerFactory = factory;
        return this;
    }

    /**
     * Override this for your specific implementation.
     * <p>
     * In this specific implementation our row key will be the term, the column family will be the field name, and the column family will be the shard,so we
     * should have the following as our last key
     * <p>
     * bar FOO:20130101_0
     * <p>
     * so we should append a null so that we don't skip shards. similarly, an assumption is made of the key structure within this class.
     *
     * @param lastKey
     *            the last key
     * @param previousRange
     *            the previous range
     */
    @Override
    public Range buildNextRange(final Key lastKey, final Range previousRange) {
        /*
         * This path includes the following key from the shard_id onward. The reason we also append the hex 255 value is because we receive a key not unlike
         * foo:20130101_0. If our next search space is foo:20130101_0\x00 we will hit all data types within that range...again..and again...and again. To
         * account for this, we put \uffff after the null byte so that we start key is technically the last value within the provided shard, moving us to the
         * exact next key within our RangeStream
         */
        Key startKey = new Key(lastKey.getRow(), lastKey.getColumnFamily(), new Text(lastKey.getColumnQualifier() + "\uffff"));
        return new Range(startKey, true, previousRange.getEndKey(), previousRange.isEndKeyInclusive());
    }

    /**
     * Seek range is built with a start key column qualifier set to the seek shard.
     *
     * In a worst case scenario the built seek range is for the very next shard, no differently than {@link #buildNextRange}.
     *
     * Best case the seek range lies beyond the data available for this scanner, thus negating any need to continue searching within this range stream.
     *
     * @param seekShard
     *            the shard to seek to
     * @param range
     *            the RangeStreamScanner's current range
     * @return the current range with a modified start key
     */
    public Range buildSeekRange(String seekShard, Range range) {

        // Adjust startKey columnQualifier to be the shard.
        Key startKey = range.getStartKey();
        Text row = startKey.getRow();
        Text cf = startKey.getColumnFamily();
        startKey = new Key(row, cf, new Text(seekShard));

        return new Range(startKey, true, range.getEndKey(), range.isEndKeyInclusive());
    }

    /**
     * Seek the underlying iterator to the specified shard.
     *
     * First ensures the seek shard does not fall within the currentEntry, resultQueue, or currentQueue.
     *
     * The queues are setup not unlike [resultQ.head] - [currentQ.first - currentQ.last] - lastSeenKey
     *
     * @param seekShard
     *            the shard to seek to.
     * @return the shard we seek'd to.
     */
    public String seek(String seekShard) {
        if (currentEntry == null && resultQueue.isEmpty() && finished) {
            return null;
        }

        String seekedShard = advanceQueues(seekShard);
        if (seekedShard == null) {

            this.seekShard = seekShard;
            this.seeking = true;

            // Clear queues before calling findTop().
            this.currentEntry = null;
            this.resultQueue.clear();
            this.currentQueue.clear();

            // Call to hasNext() with empty queues and a null currentEntry triggers a new run of the iterator.
            if (hasNext()) {
                // Check to see if the shard exists within the queues.
                seekedShard = advanceQueues(seekShard);
            } else {
                return null;
            }
        }
        return seekedShard;
    }

    /**
     * If the seek shard falls within the bounds of the currentQueue or resultQueue, advance the queues to the shard.
     *
     * @param seekShard
     *            the shard we should advance to
     * @return the matched shard if exact match, the next highest shard if greater than seekShard, or null if no more elements exist
     */
    public String advanceQueues(String seekShard) {

        // CASE 0: Check the currentEntry first.
        String topShard = currentEntryMatchesShard(seekShard);
        if (topShard != null) {
            return topShard;
        }

        // CASE 1: The seek shard is within the bounds of the result queue. Advance the result queue to the seek shard.
        if (resultQueue.size() > 0) {

            // If the top shard is a day and we are seeking to a shard within the day, return the shard.
            String resultQShard = shardFromKey(resultQueue.peek().getKey());
            if (resultQShard.length() == 8 && seekShard.startsWith(resultQShard)) {
                return resultQShard;
            }

            // If the top shard of the result queue is greater than or equal to the seek shard then we're already at the correct spot.
            if (ShardEquality.greaterThanOrEqual(resultQShard, seekShard)) {
                return resultQShard;
            }

            // Sanity check to make sure the resultQShard is sorted lower than the seek shard.
            if (ShardEquality.lessThan(resultQShard, seekShard)) {
                // Advance the resultQueue to the specified shard.
                return advanceQueueToShard(resultQueue, seekShard);
            }
        }

        // CASE 2: The seek shard is within the bounds of the current queue.
        if (currentQueue.size() > 0) {
            String firstShard = shardFromKey(currentQueue.peekFirst().getKey());
            String lastShard = shardFromKey(currentQueue.peekLast().getKey());
            if (ShardEquality.greaterThan(firstShard, seekShard) && ShardEquality.lessThan(lastShard, seekShard)) {
                // Advance currentQueue to the specified shard.
                resultQueue.clear();
                return advanceQueueToShard(currentQueue, seekShard);
            }
        }

        // CASE 3: The seek shard is beyond the bounds of the data currently held by this range stream scanner.
        return null;
    }

    /**
     * Advance the provided queue to the specified shard. Assumes the shard is already determined to exist within the queue.
     *
     * Returns the seek'd shard or the next highest value if the seek shard does not exist.
     *
     * If the queue is exhausted then return null.
     *
     * @param queue
     *            either the currentQueue or the resultQueue
     * @param shard
     *            shard to seek to
     * @return the matched shard, the next highest shard, or null
     */
    public String advanceQueueToShard(Queue<Result> queue, String shard) {
        Result top;
        String topShard = null;

        boolean advancing = true;
        while (advancing) {
            top = queue.peek();
            if (top == null)
                return null;

            topShard = shardFromKey(top.getKey());

            if (ShardEquality.greaterThanOrEqual(topShard, shard)) {
                // Stop advancing if the peeked shard is greater than or equal to the seek shard.
                advancing = false;
            } else if (topShard.indexOf('_') == -1 && shard.startsWith(topShard)) {
                // Check for special case where the top shard is a day.
                advancing = false;
            } else {
                queue.poll();
            }
        }
        return topShard;
    }

    /**
     * Determines if the currentEntry matches the seek shard.
     *
     * @param seekShard
     *            the shard to seek to
     * @return the matched shard, the next highest shard, or null
     */
    public String currentEntryMatchesShard(String seekShard) {
        if (currentEntry == null) {
            return null;
        }

        String topShard = shardFromKey(currentEntry.getKey());

        // Is the current entry an exact match?
        if (topShard.equals(seekShard))
            return seekShard;

        // Is the current entry beyond the seek shard?
        if (ShardEquality.greaterThan(topShard, seekShard))
            return topShard;

        // Seek to '20190314', top shard '20190314_0' matches. Return the day range.
        if (ShardEquality.isDay(seekShard) && topShard.startsWith(seekShard))
            return seekShard;

        // Seek to '20190314_0', top shard '20190314' matches. Return the day range.
        if (ShardEquality.isDay(topShard) && seekShard.startsWith(topShard))
            return topShard;

        // Return null to signify no match.
        return null;
    }

    /**
     * If the currentEntry is null this method will first check the resultQueue for an entry. If the resultQueue is empty then the scanner will submit a new
     * task which pulls results off the shard index, thus populating the result queue.
     *
     * @return true if the scanner has a next element
     */
    @Override
    public boolean hasNext() {
        /*
         * Let's take a moment to look through all states S
         */
        try {
            if (null != stats)
                stats.getTimer(TIMERS.HASNEXT).resume();

            while (null == currentEntry && (!finished || !resultQueue.isEmpty() || flushNeeded())) {

                try {
                    /*
                     * Poll for one second. We're in a do/while loop that will break iff we are no longer running or there is a current entry available.
                     */
                    currentEntry = resultQueue.poll(getPollTime(), TimeUnit.MILLISECONDS);

                } catch (InterruptedException e) {
                    log.error(e);
                    throw new RuntimeException(e);
                }
                // if we pulled no data and we are not running, and there is no data in the queue
                // we can flush if needed and retry
                if (currentEntry == null && (!finished && resultQueue.isEmpty())) {
                    submitTask();
                } else if (flushNeeded()) {
                    flush();
                }
            }
        } finally {
            if (null != stats) {
                try {
                    stats.getTimer(TIMERS.HASNEXT).suspend();
                } catch (Exception e) {
                    log.error(e);
                }
            }
            if (uncaughtExceptionHandler.getThrowable() != null) {
                log.error("Exception discovered on hasNext call", uncaughtExceptionHandler.getThrowable());
                Throwables.propagate(uncaughtExceptionHandler.getThrowable());
            }
        }
        return (null != currentEntry);
    }

    private void submitTask() {
        // wait on results. submit the task if we can
        Future future = myExecutor.submit(this);
        try {
            future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see com.google.common.util.concurrent.AbstractExecutionThreadService#run()
     */
    @Override
    protected void run() {
        try {
            findTop();
            flush();
        } catch (Exception e) {
            uncaughtExceptionHandler.uncaughtException(Thread.currentThread(), e);
            Throwables.propagate(e);
        }
    }

    protected int scannerInvariant(final Iterator<Result> iter) {
        PeekingIterator<Result> kvIter = new PeekingIterator<>(iter);

        int retrievalCount = 0;
        String currentDay = null;

        if (null != prevDay) {
            try {
                if (log.isTraceEnabled())
                    log.trace("Attempting to insert " + prevDay);
                if (!resultQueue.offer(prevDay, 1, TimeUnit.SECONDS)) {
                    return 0;
                }
                prevDay = null;
            } catch (InterruptedException e) {
                return 0;
            }
        }

        writeLock.lock();
        try {
            while (kvIter.hasNext()) {
                Result<?> currentKeyValue = kvIter.peek();

                // become a pass-through if we've seen an unexpected key.
                if (seenUnexpectedKey) {
                    currentQueue.add(trimTrailingUnderscore(currentKeyValue));
                    break;
                }

                if (null == currentDay) {
                    if (log.isTraceEnabled()) {
                        log.trace("it's a new day!");
                        log.trace("adding " + currentKeyValue.getKey() + " to queue because it matches" + currentDay);
                    }

                    currentDay = getDay(currentKeyValue.getKey());

                    currentQueue.add(trimTrailingUnderscore(currentKeyValue));
                    lastSeenKey = kvIter.next().getKey();
                } else {
                    String nextKeysDay = getDay(currentKeyValue.getKey());
                    if (currentDay.equals(nextKeysDay)) {
                        if (log.isTraceEnabled()) {
                            log.trace("adding " + currentKeyValue.getKey() + " to queue because it matches" + currentDay);
                        }

                        IndexInfo info = readInfoFromValue(currentKeyValue.getValue());

                        if (log.isTraceEnabled()) {
                            log.trace("adding count of " + info.count());
                        }

                        currentQueue.add(trimTrailingUnderscore(currentKeyValue));
                        lastSeenKey = kvIter.next().getKey();
                    } else {

                        int dequeueCount = dequeue();
                        retrievalCount += dequeueCount;
                        int queueSize = currentQueue.size();
                        dequeue(true);
                        currentDay = null;

                        if (dequeueCount != queueSize || retrievalCount <= Math.ceil(maxResults * 1.5)) {
                            break;
                        }
                    }
                }
            }

            retrievalCount += dequeue();

        } finally {
            writeLock.unlock();
        }
        return retrievalCount;
    }

    public IndexInfo readInfoFromValue(Value value) {
        try {
            IndexInfo info = new IndexInfo();
            info.readFields(new DataInputStream(new ByteArrayInputStream(value.get())));
            if (log.isTraceEnabled()) {
                for (IndexMatch match : info.uids()) {
                    log.trace("match is " + StringUtils.split(match.getUid(), '\u0000')[1]);
                }
            }
            return info;
        } catch (IOException e) {
            log.error(e);
            throw new DatawaveFatalQueryException(e);
        }
    }

    public Value writeInfoToValue() {
        return writeInfoToValue(new IndexInfo(-1));
    }

    public Value writeInfoToValue(IndexInfo info) {
        try {
            ByteArrayOutputStream outByteStream = new ByteArrayOutputStream();
            DataOutputStream outDataStream = new DataOutputStream(outByteStream);
            info.write(outDataStream);

            outDataStream.close();
            outByteStream.close();

            return new Value(outByteStream.toByteArray());
        } catch (IOException e) {
            log.error(e);
            throw new DatawaveFatalQueryException(e);
        }
    }

    private int dequeue() {
        return dequeue(false);
    }

    private int dequeue(boolean forceAll) {
        int count = 0;

        Queue<Result> kvIter = Queues.newArrayDeque(currentQueue);

        currentQueue.clear();
        boolean result = true;
        for (Result top : kvIter) {

            if (result) {
                do {
                    result = resultQueue.offer(top);

                    if (!result) {
                        if (log.isTraceEnabled())
                            log.trace("Failed adding " + resultQueue.size() + " " + forceAll);
                        if (forceAll)
                            continue;
                    }

                    break;
                } while (!finished && forceAll);
            }

            if (!result && !(!finished && forceAll)) {
                if (log.isTraceEnabled())
                    log.trace("Adding " + top.getKey() + " back ");
                currentQueue.add(top);
            } else {
                if (log.isTraceEnabled())
                    log.trace("missing " + top.getKey() + " true? " + result);
            }

            if (log.isTraceEnabled())
                log.trace("Last key is " + lastSeenKey);

            count++;
        }

        if (log.isTraceEnabled()) {
            log.trace("we have " + currentQueue.size() + " " + kvIter.size());
        }

        return count;
    }

    @Override
    protected void flush() {
        writeLock.lock();
        try {
            dequeue(false);
        } finally {
            writeLock.unlock();
        }
    }

    protected boolean flushNeeded() {
        readLock.lock();
        try {
            return !currentQueue.isEmpty();
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Get the day from the key
     *
     * @param key
     *            a key
     * @return the day
     */
    protected String getDay(final Key key) {
        String myDay = null;
        byte[] cq = key.getColumnQualifierData().getBackingArray();
        if (cq.length >= DATE_CF_LENGTH) {
            myDay = new String(cq, 0, DATE_CF_LENGTH);
            if (log.isTraceEnabled()) {
                log.trace("Day is " + myDay + " for " + key);
            }
        }
        return myDay;
    }

    /**
     * Get the shard from the accumulo key.
     *
     * Strip any trailing underscores from the returned shard.
     *
     * @param key
     *            the key to everything
     * @return just a shard
     */
    public static String shardFromKey(final Key key) {
        byte[] cq = key.getColumnQualifierData().getBackingArray();
        if (cq[cq.length - 1] == '_') {
            return new String(cq, 0, cq.length - 1);
        } else {
            return new String(cq, 0, cq.length);
        }
    }

    @Override
    public RangeStreamScanner call() throws Exception {
        findTop();
        return this;
    }

    /**
     * FindTop -- Follows the logic outlined in the comments, below. Effectively, we continue
     *
     * @throws Exception
     *             if there are issues
     */
    protected void findTop() throws Exception {
        if (ranges.isEmpty() && lastSeenKey == null) {
            if (log.isTraceEnabled()) {
                log.trace("Finished");
            }
            finished = true;
            if (flushNeeded()) {
                if (log.isTraceEnabled())
                    log.trace("flush needed");
                flush();
                return;
            }
            return;
        }

        ScannerBase baseScanner = null;
        try {
            if (resultQueue.remainingCapacity() == 0) {
                return;
            }

            /*
             * Even though we were delegated a resource, we have not actually been provided the plumbing to run it. Note, below, that we initialize the resource
             * through the resource factory from a running resource.
             */
            if (null != stats)
                stats.getTimer(TIMERS.SCANNER_START).resume();

            baseScanner = scannerFactory.newSingleScanner(tableName, auths, settings);

            if (baseScanner instanceof Scanner)
                ((Scanner) baseScanner).setReadaheadThreshold(Long.MAX_VALUE);
            else if (baseScanner instanceof RfileScanner)
                ((RfileScanner) baseScanner).setRanges(Collections.singleton(currentRange));

            for (Column family : options.getFetchedColumns()) {
                if (family.columnQualifier != null)
                    baseScanner.fetchColumn(new Text(family.columnFamily), new Text(family.columnQualifier));
                else {
                    if (log.isTraceEnabled())
                        log.trace("Setting column family " + new Text(family.columnFamily));
                    baseScanner.fetchColumnFamily(new Text(family.columnFamily));
                }
            }
            for (IteratorSetting setting : options.getIterators()) {
                if (log.isTraceEnabled())
                    log.trace("Adding setting, " + setting);
                baseScanner.addScanIterator(setting);
            }

            // if we have just started or we are at the end of the current range. pop the next range
            if (lastSeenKey == null || (currentRange != null && currentRange.getEndKey() != null && isBeyondRange(lastSeenKey, currentRange.getEndKey()))) {
                currentRange = ranges.poll();
                // short circuit and exit
                if (null == currentRange) {
                    lastSeenKey = null;
                    return;
                }
            } else {
                // adjust the end key range.
                if (seeking) {
                    currentRange = buildSeekRange(seekShard, currentRange);
                    seeking = false;
                } else {
                    currentRange = buildNextRange(lastSeenKey, currentRange);
                }

                if (log.isTraceEnabled())
                    log.trace("Building " + currentRange + " from " + lastSeenKey);
            }

            if (log.isTraceEnabled()) {
                log.trace(lastSeenKey + ", using current range of " + lastRange);
                log.trace(lastSeenKey + ", using current range of " + currentRange);
            }
            if (baseScanner instanceof Scanner)
                ((Scanner) baseScanner).setRange(currentRange);

            Iterator<Result> iter = Iterators.transform(baseScanner.iterator(), input -> {
                if (input == null) {
                    return null;
                }
                return new Result<>(input.getKey(), input.getValue());
            });
            // do not continue if we've reached the end of the corpus

            if (!iter.hasNext()) {
                if (log.isTraceEnabled()) {
                    log.trace("We've started, but we have nothing to do on " + tableName + " " + auths + " " + currentRange);
                }
                lastSeenKey = null;
                return;
            }

            int retrievalCount = 0;
            try {
                if (null != stats) {
                    stats.getTimer(TIMERS.SCANNER_ITERATE).resume();
                }
                retrievalCount = scannerInvariant(iter);
            } finally {
                if (null != stats) {
                    stats.incrementKeysSeen(retrievalCount);
                    stats.getTimer(TIMERS.SCANNER_ITERATE).suspend();
                }
            }
        } catch (IllegalArgumentException e) {
            /*
             * If we get an illegal argument exception, we know that the ScannerSession extending class created a start key after our end key, which means that
             * we've finished with this range. As a result, we set lastSeenKey to null, so that on our next pass through, we pop the next range from the queue
             * and continue or finish. We're going to timeslice and come back as know this range is likely finished.
             */
            if (log.isTraceEnabled())
                log.trace(lastSeenKey + " is lastSeenKey, previous range is " + currentRange, e);

            lastSeenKey = null;

        } catch (Exception e) {

            log.error(e);
            throw e;

        } finally {

            if (null != stats)
                stats.getTimer(TIMERS.SCANNER_START).suspend();

            scannerFactory.close(baseScanner);
            // no point in running again
            if (ranges.isEmpty() && lastSeenKey == null) {
                finished = true;
            }
        }
    }

    private boolean isBeyondRange(Key lastSeenKey, Key endKey) {
        if (lastSeenKey.compareTo(endKey) >= 0) {
            return true;
        } else {

            String cf = lastSeenKey.getColumnQualifier().toString();
            String endCf = endKey.getColumnQualifier().toString();

            if (log.isTraceEnabled()) {
                log.trace(cf + " " + endCf);
            }

            if (DATE_CF_LENGTH == cf.length()) {
                endCf = endCf.substring(0, DATE_CF_LENGTH);
                if (cf.compareTo(endCf) >= 0) {
                    return true;
                }
            }
            return false;
        }
    }

    // Overloaded
    public static Result trimTrailingUnderscore(Result entry) {
        Key nextKey = trimTrailingUnderscore(entry.getKey());
        return new Result(entry.getContext(), nextKey, entry.getValue());
    }

    /**
     * It may be possible that a trailing underscore is appended to a day range. Check for and remove any trailing underscores that exist.
     *
     * @param key
     *            the key
     * @return the key, less any trailing underscores
     */
    public static Key trimTrailingUnderscore(Key key) {
        ByteSequence sequence = key.getColumnQualifierData();
        if (sequence.byteAt(sequence.length() - 1) == '_') {
            sequence = sequence.subSequence(0, sequence.length() - 1);
            return new Key(key.getRow(), key.getColumnFamily(), new Text(sequence.toString()));
        } else {
            return key;
        }
    }
}
