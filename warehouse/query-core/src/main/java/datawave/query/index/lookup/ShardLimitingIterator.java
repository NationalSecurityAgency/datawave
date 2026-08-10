package datawave.query.index.lookup;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Queue;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.collections4.iterators.PeekingIterator;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.collect.Maps;
import com.google.common.collect.Queues;

import datawave.query.exceptions.DatawaveFatalQueryException;

/**
 * Purpose: Limits the number of shards we will evaluate for a given term. Once we hit the preconfigured number will short circuit.
 *
 */
public class ShardLimitingIterator implements Iterator<Entry<Key,Value>> {

    protected PeekingIterator<Entry<Key,Value>> kvIter;
    protected int maxShardsPerDay = Integer.MAX_VALUE;
    protected Queue<Entry<Key,Value>> currentQueue;
    protected String currentDay = null;
    // simply compare the strings. no need for a date formatter
    protected static final int dateCfLength = 8;

    protected boolean seenUnexpectedKey = false;

    private static final Logger log = Logger.getLogger(ShardLimitingIterator.class);

    public ShardLimitingIterator(Iterator<Entry<Key,Value>> kvIter, int maxShardsPerDay) {
        this.kvIter = new PeekingIterator<>(kvIter);
        this.maxShardsPerDay = maxShardsPerDay;
        currentQueue = Queues.newArrayDeque();
    }

    @Override
    public boolean hasNext() {
        if (currentQueue.isEmpty()) {
            // reset the state of the current day.
            currentDay = null;
            peekInSource();
        }
        return !currentQueue.isEmpty();
    }

    protected void peekInSource() {
        while (kvIter.hasNext()) {
            Entry<Key,Value> currentKeyValue = kvIter.peek();

            // become a passthrough if we've seen an unexpected key.
            if (seenUnexpectedKey) {
                currentQueue.add(currentKeyValue);
                break;
            }

            if (null == currentDay) {
                if (log.isTraceEnabled()) {
                    log.trace("it's a new day!");
                }
                currentDay = getDay(currentKeyValue.getKey());
                currentQueue.add(currentKeyValue);
                kvIter.next();
            } else {
                String nextKeysDay = getDay(currentKeyValue.getKey());
                if (currentDay.equals(nextKeysDay)) {
                    if (log.isTraceEnabled()) {
                        log.trace("adding " + currentKeyValue.getKey() + " to queue because it matches" + currentDay);
                    }
                    if (currentQueue.size() <= maxShardsPerDay)
                        currentQueue.add(currentKeyValue);
                    kvIter.next();
                } else
                    break;
            }

        }

    }

    /**
     * Get the day from the key
     *
     * @param key
     *            a key
     * @return the day string
     */
    protected String getDay(final Key key) {
        String myDay = null;
        byte[] cq = key.getColumnQualifierData().getBackingArray();
        if (cq.length >= dateCfLength) {
            myDay = new String(cq, 0, dateCfLength);
            if (log.isTraceEnabled()) {
                log.trace("Day is " + myDay);
            }
        }
        return myDay;
    }

    @Override
    public Entry<Key,Value> next() {

        Entry<Key,Value> top = currentQueue.poll();
        if (currentQueue.size() >= maxShardsPerDay) {

            Key topKey = top.getKey();
            if (log.isTraceEnabled())
                log.trace(topKey + " for " + currentDay + " exceeds limit of " + maxShardsPerDay + " with " + currentQueue.size());
            Key newKey = new Key(topKey.getRow(), topKey.getColumnFamily(), new Text(currentDay), topKey.getColumnVisibility(), topKey.getTimestamp());

            currentQueue.clear();

            IndexInfo info = new IndexInfo(-1);

            Value newValue = null;
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

            return Maps.immutableEntry(newKey, newValue);
        } else {
            if (log.isTraceEnabled())
                log.trace(top + " for " + currentDay + " does not exceed limit of " + maxShardsPerDay + " with " + currentQueue.size());
            return top;
        }
    }

    @Override
    public void remove() {

    }

}
