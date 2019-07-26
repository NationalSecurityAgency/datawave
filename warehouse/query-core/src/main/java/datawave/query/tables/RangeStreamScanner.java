package datawave.query.tables;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.base.Throwables;
import datawave.mr.bulk.RfileScanner;
import datawave.query.index.lookup.IndexInfo;
import datawave.query.index.lookup.IndexMatch;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.tables.stats.ScanSessionStats.TIMERS;
import datawave.webservice.query.Query;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * Purpose: Extends Scanner session so that we can modify how we build our subsequent ranges. Breaking this out cleans up the code. May require implementation
 * specific details if you are using custom iterators, as we are reinitializing a seek
 * 
 * Design: Extends Scanner session and only overrides the buildNextRange.
 * 
 * 
 */
public class RangeStreamScanner extends ScannerSession implements Callable<RangeStreamScanner> {
    
    private static final int MAX_MEDIAN = 20;
    private static final Logger log = Logger.getLogger(RangeStreamScanner.class);
    private int shardsPerDayThreshold = Integer.MAX_VALUE;
    // simply compare the strings. no need for a date formatter
    protected static final int dateCfLength = 8;
    protected boolean seenUnexpectedKey = false;
    protected Queue<Entry<Key,Value>> currentQueue;
    
    protected Entry<Key,Value> prevDay = null;
    
    protected ReentrantReadWriteLock queueLock = new ReentrantReadWriteLock(true);
    
    protected Lock readLock;
    protected Lock writeLock;
    
    volatile boolean finished = false;
    
    ExecutorService myExecutor;
    
    protected ScannerFactory scannerFactory;
    
    @Override
    protected String serviceName() {
        String id = "NoQueryId";
        if (null != settings && null != settings.getId()) {
            id = settings.getId().toString();
        }
        return "RangeStreamScanner (" + id + ")";
    }
    
    /**
     * @param tableName
     * @param auths
     * @param delegator
     * @param maxResults
     */
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
    
    /**
     * @param tableName
     * @param auths
     * @param delegator
     * @param maxResults
     */
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
     * 
     * In this specific implementation our row key will be the term, the column family will be the field name, and the column family will be the shard,so we
     * should have the following as our last key
     * 
     * bar FOO:20130101_0
     * 
     * so we should append a null so that we we don't skip shards. similarly, an assumption is made of the key structure within this class.
     * 
     * @param lastKey
     * @param previousRange
     */
    @Override
    public Range buildNextRange(final Key lastKey, final Range previousRange) {
        
        /*
         * This path includes the following key from the shard_id onward. The reason we also append the hex 255 value is because we receive a key not unlike
         * foo:20130101_0. If our next search space is foo:20130101_0\x00 we will hit all data types within that range...again..and again...and again. To
         * account for this, we put \uffff after the null byte so that we start key is technically the last value within the provided shard, moving us to the
         * exact next key within our RangeStream
         */
        return new Range(new Key(lastKey.getRow(), lastKey.getColumnFamily(), new Text(lastKey.getColumnQualifier() + "\uffff")), true,
                        previousRange.getEndKey(), previousRange.isEndKeyInclusive());
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.util.Iterator#hasNext()
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
        Future<RangeStreamScanner> future = myExecutor.submit(this);
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
    
    protected int scannerInvariant(final Iterator<Entry<Key,Value>> iter) {
        PeekingIterator<Entry<Key,Value>> kvIter = new PeekingIterator<>(iter);
        
        int retrievalCount = 0;
        
        Entry<Key,Value> myEntry;
        
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
        // produces stats for us, so we don't have to!
        DescriptiveStatistics stats = new DescriptiveStatistics();
        
        writeLock.lock();
        try {
            while (kvIter.hasNext()) {
                Entry<Key,Value> currentKeyValue = kvIter.peek();
                
                IndexInfo infos = new IndexInfo();
                try {
                    infos.readFields(new DataInputStream(new ByteArrayInputStream(currentKeyValue.getValue().get())));
                    if (log.isTraceEnabled()) {
                        for (IndexMatch match : infos.uids()) {
                            log.trace("match is " + StringUtils.split(match.getUid(), '\u0000')[1]);
                        }
                    }
                } catch (IOException e) {
                    log.error(e);
                }
                
                // become a pass-through if we've seen an unexpected key.
                if (seenUnexpectedKey) {
                    currentQueue.add(currentKeyValue);
                    break;
                }
                
                if (null == currentDay) {
                    if (log.isTraceEnabled()) {
                        log.trace("it's a new day!");
                        log.trace("adding " + currentKeyValue.getKey() + " to queue because it matches" + currentDay);
                    }
                    
                    currentDay = getDay(currentKeyValue.getKey());
                    
                    currentQueue.add(currentKeyValue);
                    
                    lastSeenKey = kvIter.next().getKey();
                } else {
                    String nextKeysDay = getDay(currentKeyValue.getKey());
                    if (currentDay.equals(nextKeysDay)) {
                        if (log.isTraceEnabled()) {
                            log.trace("adding " + currentKeyValue.getKey() + " to queue because it matches" + currentDay);
                        }
                        
                        IndexInfo info = new IndexInfo();
                        try {
                            info.readFields(new DataInputStream(new ByteArrayInputStream(currentKeyValue.getValue().get())));
                        } catch (IOException e) {
                            throw new DatawaveFatalQueryException(e);
                        }
                        
                        if (log.isTraceEnabled()) {
                            log.trace("adding count of " + info.count());
                        }
                        
                        stats.addValue(info.count());
                        
                        if (currentQueue.size() <= shardsPerDayThreshold
                                        || (currentQueue.size() >= shardsPerDayThreshold && stats.getPercentile(50) < MAX_MEDIAN)) {
                            
                            if (log.isTraceEnabled()) {
                                log.trace("adding our stats are " + stats.getPercentile(50) + " on " + currentQueue.size());
                            }
                            
                            currentQueue.add(currentKeyValue);
                            
                        } else {
                            if (log.isTraceEnabled()) {
                                log.trace("breaking because our stats are " + stats.getPercentile(50) + " on " + currentQueue.size());
                            }
                            break;
                        }
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
            
            if (currentQueue.size() >= shardsPerDayThreshold && stats.getPercentile(50) > MAX_MEDIAN) {
                
                Entry<Key,Value> top = currentQueue.poll();
                
                Key topKey = top.getKey();
                if (log.isTraceEnabled())
                    log.trace(topKey + " for " + currentDay + " exceeds limit of " + shardsPerDayThreshold + " with " + currentQueue.size());
                Key newKey = new Key(topKey.getRow(), topKey.getColumnFamily(), new Text(currentDay), topKey.getColumnVisibility(), topKey.getTimestamp());
                
                IndexInfo info = new IndexInfo(-1);
                
                Value newValue;
                try {
                    ByteArrayOutputStream outByteStream = new ByteArrayOutputStream();
                    DataOutputStream outDataStream = new DataOutputStream(outByteStream);
                    info.write(outDataStream);
                    
                    outDataStream.close();
                    outByteStream.close();
                    
                    newValue = new Value(outByteStream.toByteArray());
                } catch (IOException e) {
                    throw new DatawaveFatalQueryException(e);
                }
                
                myEntry = Maps.immutableEntry(newKey, newValue);
                lastSeenKey = newKey;
                
                try {
                    if (!resultQueue.offer(myEntry, 1, TimeUnit.SECONDS)) {
                        if (log.isTraceEnabled()) {
                            log.trace("could not add day! converting " + myEntry + " to " + prevDay);
                        }
                        prevDay = myEntry;
                    }
                } catch (InterruptedException exception) {
                    prevDay = myEntry;
                }
                
                currentQueue.clear();
                
            } else {
                retrievalCount += dequeue();
            }
        } finally {
            writeLock.unlock();
        }
        return retrievalCount;
    }
    
    private int dequeue() {
        return dequeue(false);
    }
    
    private int dequeue(boolean forceAll) {
        int count = 0;
        
        Queue<Entry<Key,Value>> kvIter = Queues.newArrayDeque(currentQueue);
        
        currentQueue.clear();
        boolean result = true;
        for (Entry<Key,Value> top : kvIter) {
            
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
     * @return
     */
    protected String getDay(final Key key) {
        String myDay = null;
        byte[] cq = key.getColumnQualifierData().getBackingArray();
        if (cq.length >= dateCfLength) {
            myDay = new String(cq, 0, dateCfLength);
            if (log.isTraceEnabled()) {
                log.trace("Day is " + myDay + " for " + key);
            }
        }
        return myDay;
    }
    
    public RangeStreamScanner setShardsPerDayThreshold(int shardsPerDayThreshold) {
        this.shardsPerDayThreshold = shardsPerDayThreshold;
        return this;
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
     * 
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
                currentRange = buildNextRange(lastSeenKey, currentRange);
                
                if (log.isTraceEnabled())
                    log.trace("Building " + currentRange + " from " + lastSeenKey);
            }
            
            if (log.isTraceEnabled()) {
                log.trace(lastSeenKey + ", using current range of " + lastRange);
                log.trace(lastSeenKey + ", using current range of " + currentRange);
            }
            if (baseScanner instanceof Scanner)
                ((Scanner) baseScanner).setRange(currentRange);
            else if (baseScanner instanceof RfileScanner) {
                ((RfileScanner) baseScanner).setRanges(Collections.singleton(currentRange));
            }
            
            Iterator<Entry<Key,Value>> iter = baseScanner.iterator();
            
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
            
            if (dateCfLength == cf.length()) {
                endCf = endCf.substring(0, dateCfLength);
                if (cf.compareTo(endCf) >= 0) {
                    return true;
                }
            }
            return false;
        }
    }
}
