package datawave.query.iterator;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.YieldCallback;
import org.apache.hadoop.io.Text;

import com.google.common.collect.Maps;

import datawave.data.type.util.NumericalEncoder;

/**
 * Created on 9/6/16.
 */
public class ResultCountingIterator implements Iterator<Entry<Key,Value>> {
    private AtomicLong resultCount = new AtomicLong(0);
    private Iterator<Entry<Key,Value>> serializedDocuments = null;
    private YieldCallback<Key> yield;

    public ResultCountingIterator(Iterator<Entry<Key,Value>> serializedDocuments, long resultCount, YieldCallback<Key> yieldCallback) {
        this.serializedDocuments = serializedDocuments;
        this.resultCount.set(resultCount);
        this.yield = yieldCallback;
    }

    @Override
    public boolean hasNext() {
        boolean hasNext = serializedDocuments.hasNext();
        if (yield != null && yield.hasYielded()) {
            yield.yield(addKeyCount(yield.getPositionAndReset()));
        }
        return hasNext;
    }

    @Override
    public Entry<Key,Value> next() {
        Entry<Key,Value> next = serializedDocuments.next();
        if (next != null) {
            next = Maps.immutableEntry(addKeyCount(next.getKey()), next.getValue());
        }
        return next;
    }

    public Key addKeyCount(Key key) {
        resultCount.getAndIncrement();
        return new Key(key.getRow(), new Text(NumericalEncoder.encode(Long.toString(resultCount.get())) + '\0' + key.getColumnFamily()),
                        key.getColumnQualifier(), key.getColumnVisibility(), key.getTimestamp());
    }

    @Override
    public void remove() {
        serializedDocuments.remove();
    }
}
