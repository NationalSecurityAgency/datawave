package datawave.query.jexl.functions;

import com.google.common.base.Function;
import com.google.common.collect.Maps;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.iterators.YieldCallback;
import org.apache.hadoop.io.Text;

import java.util.Iterator;
import java.util.Map.Entry;

/**
 * Key adjudicator, will take an accumulo key based entry whose value is specified by T.
 * 
 * @param <T>
 *            type of the key adjudicator
 */
public class KeyAdjudicator<T> implements Iterator<Entry<Key,T>>, Function<Entry<Key,T>,Entry<Key,T>> {
    
    public static final Text COLUMN_QUALIFIER_SUFFIX = new Text("\uffff");
    public static final Text EMPTY_COLUMN_QUALIFIER = new Text();
    
    private final Text colQualRef;
    private final Iterator<Entry<Key,T>> source;
    private final YieldCallback<Key> yield;
    
    public KeyAdjudicator(Iterator<Entry<Key,T>> source, Text colQualRef, YieldCallback<Key> yield) {
        this.colQualRef = colQualRef;
        this.source = source;
        this.yield = yield;
    }
    
    public KeyAdjudicator(Iterator<Entry<Key,T>> source, YieldCallback<Key> yield) {
        this(source, COLUMN_QUALIFIER_SUFFIX, yield);
    }
    
    public KeyAdjudicator(Text colQualRef) {
        this(null, colQualRef, null);
    }
    
    public KeyAdjudicator() {
        this(null, null);
    }
    
    @Override
    public Entry<Key,T> apply(Entry<Key,T> entry) {
        final Key entryKey = entry.getKey();
        return Maps.immutableEntry(new Key(entryKey.getRow(), entryKey.getColumnFamily(), colQualRef, entryKey.getColumnVisibility(), entryKey.getTimestamp()),
                        entry.getValue());
    }
    
    @Override
    public boolean hasNext() {
        boolean hasNext = source.hasNext();
        if (yield != null && yield.hasYielded()) {
            yield.yield(apply((Entry<Key,T>) Maps.immutableEntry(yield.getPositionAndReset(), null)).getKey());
        }
        return hasNext;
    }
    
    @Override
    public Entry<Key,T> next() {
        Entry<Key,T> next = source.next();
        if (next != null) {
            next = apply(next);
        }
        return next;
    }
    
    @Override
    public void remove() {
        source.remove();
    }
}
