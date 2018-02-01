package datawave.query.util;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

/**
 * This is a SortedKeyValueIterator implementation that simply wraps an underlying {@code Iterator<Map.Entry<Key, Value>>}
 * 
 * 
 * 
 */
public class IteratorToSortedKeyValueIterator implements SortedKeyValueIterator<Key,Value> {
    private Iterator<Map.Entry<Key,Value>> iterator = null;
    private Map.Entry<Key,Value> next = null;
    private boolean initialized = false;
    
    public IteratorToSortedKeyValueIterator(Iterator<Map.Entry<Key,Value>> iterator) {
        this.iterator = iterator;
    }
    
    @Override
    public boolean hasTop() {
        init();
        return next != null;
    }
    
    @Override
    public Key getTopKey() {
        if (next != null) {
            return next.getKey();
        }
        return null;
    }
    
    @Override
    public Value getTopValue() {
        if (next != null) {
            return next.getValue();
        }
        return null;
    }
    
    @Override
    public void next() {
        init();
        next = null;
        if (iterator.hasNext()) {
            next = iterator.next();
            if (next.getKey() == null && next.getValue() == null) {
                next = null;
            }
        }
    }
    
    private void init() {
        if (!initialized) {
            if (iterator.hasNext()) {
                next = iterator.next();
            }
            initialized = true;
        }
    }
    
    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {}
    
    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        return new IteratorToSortedKeyValueIterator(this.iterator);
    }
    
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        this.iterator = new SortedKeyValueIteratorToIterator(source);
    }
    
}
