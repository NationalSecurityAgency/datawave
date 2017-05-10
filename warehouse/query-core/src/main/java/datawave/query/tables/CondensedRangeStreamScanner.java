package datawave.query.tables;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.collect.Maps;

import datawave.query.index.lookup.CondensedIndexInfo;
import datawave.query.index.lookup.CondensedUidIterator;
import datawave.query.index.lookup.IndexInfo;
import datawave.query.rewrite.exceptions.DatawaveFatalQueryException;

/**
 * Purpose: Extends Scanner session so that we can modify how we build our subsequent ranges. Breaking this out cleans up the code. May require implementation
 * specific details if you are using custom iterators, as we are reinitializing a seek
 * 
 * Design: Extends Scanner session and only overrides the buildNextRange.
 * 
 * 
 */
public class CondensedRangeStreamScanner extends RangeStreamScanner {
    
    protected Iterator<String> currentShard = null;
    
    protected boolean nextDay = false;
    
    protected Key nextTopKey = null;
    protected Value nextTopValue = null;
    protected CondensedIndexInfo currentInfo;
    protected boolean initialized = false;
    protected boolean compressUids = false;
    
    private static final Logger log = Logger.getLogger(CondensedRangeStreamScanner.class);
    
    public CondensedRangeStreamScanner(ScannerSession other) {
        super(other);
        
    }
    
    @Override
    public boolean hasNext() {
        if (!initialized) {
            for (IteratorSetting setting : options.getIterators()) {
                String compressOpt = setting.getOptions().get(CondensedUidIterator.COMPRESS_MAPPING);
                if (null != compressOpt && compressOpt.equalsIgnoreCase("true")) {
                    compressUids = true;
                    break;
                }
            }
            initialized = true;
        }
        boolean hasNext = false;
        if (null == currentEntry || nextDay) {
            nextDay = false;
            currentEntry = null;
            hasNext = super.hasNext();
            currentShard = null;
            if (!hasNext)
                return false;
            else
                currentEntry = super.next();
            
            currentInfo = new CondensedIndexInfo();
            try {
                if (compressUids) {
                    currentInfo.fromByteArray(currentEntry.getValue().get());
                } else {
                    currentInfo.readFields(new DataInputStream(new ByteArrayInputStream(currentEntry.getValue().get())));
                }
            } catch (IOException e) {
                log.error(e);
            }
            
            if (currentInfo.isDay()) {
                
                nextTopKey = currentEntry.getKey();
                
                if (log.isTraceEnabled())
                    log.trace(nextTopKey + " CurrentInfo " + currentInfo.getDay() + " is a day ");
                
                ByteSequence sequence = currentEntry.getKey().getColumnQualifierData();
                if (sequence.byteAt(sequence.length() - 1) == '_') {
                    sequence = sequence.subSequence(0, sequence.length() - 1);
                }
                nextTopKey = new Key(currentEntry.getKey().getRow(), currentEntry.getKey().getColumnFamily(), new Text(sequence.toString()));
                
                IndexInfo info = new IndexInfo(-1);
                
                try {
                    
                    ByteArrayOutputStream outByteStream = new ByteArrayOutputStream();
                    DataOutputStream outDataStream = new DataOutputStream(outByteStream);
                    info.write(outDataStream);
                    
                    outDataStream.close();
                    outByteStream.close();
                    nextTopValue = new Value(outByteStream.toByteArray());
                } catch (IOException e) {
                    throw new DatawaveFatalQueryException(e);
                }
                
                return true;
            }
            
        }
        
        if (null == currentShard)
        
        {
            currentShard = currentInfo.getShards().iterator();
            if (null == currentShard || currentInfo.isDay()) {
                
                if (currentInfo.isDay()) {
                    return true;
                } else {
                    return super.hasNext();
                }
            }
            
            return currentShard.hasNext();
        } else {
            
            if (!currentShard.hasNext()) {
                nextDay = true;
                boolean hasNextDay = hasNext();
                return hasNextDay;
            } else {
                
                return true;
            }
        }
        
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.util.Iterator#next()
     */
    @Override
    public Entry<Key,Value> next() {
        if (nextDay) {
            hasNext();
        }
        
        if (!currentInfo.isDay()) {
            
            String nextShard = currentShard.next();
            
            if (log.isTraceEnabled())
                log.trace(currentEntry.getKey() + " CurrentInfo " + currentInfo.getDay() + " is not a day " + nextShard);
            
            if (null != nextShard) {
                Key currentTopKey = currentEntry.getKey();
                nextTopKey = new Key(currentTopKey.getRow(), currentTopKey.getColumnFamily(), new Text(nextShard));
                IndexInfo info = new IndexInfo(5);
                if (!currentInfo.isIgnored(nextShard)) {
                    info = currentInfo.getShard(nextShard);
                    
                }
                
                if (log.isTraceEnabled())
                    log.trace(nextTopKey + " CurrentInfo " + currentInfo.getDay() + " not a day " + nextShard + " " + info.uids().size());
                
                try {
                    
                    ByteArrayOutputStream outByteStream = new ByteArrayOutputStream();
                    DataOutputStream outDataStream = new DataOutputStream(outByteStream);
                    info.write(outDataStream);
                    
                    outDataStream.close();
                    outByteStream.close();
                    
                    nextTopValue = new Value(outByteStream.toByteArray());
                } catch (IOException e) {
                    throw new DatawaveFatalQueryException(e);
                }
            }
        } else {
            if (log.isTraceEnabled())
                log.trace("CurrentInfo " + currentInfo.getDay() + " is a day ");
            nextDay = true;
        }
        
        Entry<Key,Value> retVal = Maps.immutableEntry(nextTopKey, nextTopValue);
        return retVal;
        
    }
    
    protected int scannerInvariant(final Iterator<Entry<Key,Value>> iter) {
        
        int retrievalCount = 0;
        
        writeLock.lock();
        try {
            Entry<Key,Value> myEntry = null;
            
            while (iter.hasNext()) {
                myEntry = iter.next();
                
                try {
                    if (!resultQueue.offer(myEntry, 200, TimeUnit.MILLISECONDS))
                        break;
                } catch (InterruptedException exception) {
                    break;
                }
                
                lastSeenKey = myEntry.getKey();
                // do not continue if we have reached the capacity of the queue
                // or we are 1.5x the maxResults ( to ensure fairness to other threads
                if (resultQueue.remainingCapacity() == 0 || (isFair && retrievalCount >= Math.ceil(maxResults * 1.5))) {
                    if (log.isTraceEnabled())
                        log.trace("stopping because we're full after adding " + resultQueue.remainingCapacity() + " " + retrievalCount + " " + maxResults);
                    break;
                }
                retrievalCount++;
            }
            
        } finally
        
        {
            writeLock.unlock();
        }
        return retrievalCount;
    }
}
