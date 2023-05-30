package datawave.iterators;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iteratorsImpl.system.IterationInterruptedException;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.InterruptibleIterator;

import com.google.common.collect.TreeMultimap;

/**
 * 
 */
public class SortedMultiMapIterator implements InterruptibleIterator {
    private Iterator<Entry<Key,Value>> iter;
    private Entry<Key,Value> entry;
    
    private TreeMultimap<Key,Value> map;
    private Range range;
    
    private AtomicBoolean interruptFlag;
    private int interruptCheckCount = 0;
    
    public SortedMultiMapIterator deepCopy(IteratorEnvironment env) {
        return new SortedMultiMapIterator(map, interruptFlag);
    }
    
    private SortedMultiMapIterator(TreeMultimap<Key,Value> map, AtomicBoolean interruptFlag) {
        this.map = map;
        iter = null;
        this.range = new Range();
        entry = null;
        
        this.interruptFlag = interruptFlag;
    }
    
    public SortedMultiMapIterator(TreeMultimap<Key,Value> map) {
        this(map, null);
    }
    
    @Override
    public Key getTopKey() {
        return entry.getKey();
    }
    
    @Override
    public Value getTopValue() {
        return entry.getValue();
    }
    
    @Override
    public boolean hasTop() {
        return entry != null;
    }
    
    @Override
    public void next() throws IOException {
        
        if (entry == null)
            throw new IllegalStateException();
        
        if (interruptFlag != null && interruptCheckCount++ % 100 == 0 && interruptFlag.get())
            throw new IterationInterruptedException();
        
        if (iter.hasNext()) {
            entry = iter.next();
            if (range.afterEndKey(entry.getKey())) {
                entry = null;
            }
        } else
            entry = null;
        
    }
    
    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        
        if (interruptFlag != null && interruptFlag.get())
            throw new IterationInterruptedException();
        
        this.range = range;
        
        Key key = range.getStartKey();
        if (key == null) {
            key = new Key();
        }
        
        iter = map.entries().iterator();
        
        if (iter.hasNext()) {
            entry = iter.next();
            while (entry.getKey().compareTo(key) <= 0)
                entry = iter.next();
            if (range.afterEndKey(entry.getKey())) {
                entry = null;
            }
        } else
            entry = null;
        
        while (hasTop() && range.beforeStartKey(getTopKey())) {
            next();
        }
    }
    
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void setInterruptFlag(AtomicBoolean flag) {
        this.interruptFlag = flag;
    }
}
