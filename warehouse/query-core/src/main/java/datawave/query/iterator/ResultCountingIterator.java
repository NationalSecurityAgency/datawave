package datawave.query.iterator;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.iterators.YieldCallback;
import org.apache.hadoop.io.Text;

import com.google.common.collect.Maps;

import datawave.data.type.util.NumericalEncoder;
import datawave.query.attributes.Document;

/**
 * Created on 9/6/16.
 */
public class ResultCountingIterator implements Iterator<Entry<Key,Document>> {
    private final AtomicLong resultCount = new AtomicLong(0);
    private final Iterator<Entry<Key,Document>> documentIterator;
    private final YieldCallback<Key> yield;

    public ResultCountingIterator(Iterator<Entry<Key,Document>> documentIterator, long resultCount, YieldCallback<Key> yieldCallback) {
        this.documentIterator = documentIterator;
        this.resultCount.set(resultCount);
        this.yield = yieldCallback;
    }

    @Override
    public boolean hasNext() {
        boolean hasNext = documentIterator.hasNext();
        if (yield != null && yield.hasYielded()) {
            yield.yield(addKeyCount(yield.getPositionAndReset()));
        }
        return hasNext;
    }

    @Override
    public Entry<Key,Document> next() {
        Entry<Key,Document> next = documentIterator.next();
        if (next != null) {
            next = Maps.immutableEntry(addKeyCount(next.getKey()), next.getValue());
        }
        return next;
    }

    private Key addKeyCount(Key key) {
        resultCount.getAndIncrement();
        return new Key(key.getRow(), new Text(NumericalEncoder.encode(Long.toString(resultCount.get())) + '\0' + key.getColumnFamily()),
                        key.getColumnQualifier(), key.getColumnVisibility(), key.getTimestamp());
    }

    @Override
    public void remove() {
        documentIterator.remove();
    }
}
