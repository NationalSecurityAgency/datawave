package datawave.query.iterator;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

/**
 * Iterator that can be used to back a source with a List of Key/Value entries. Useful for small data test sets
 */
public class SortedListKeyValueIterator implements SortedKeyValueIterator {
    private List<Map.Entry<Key,Value>> sourceList;
    private int currentIndex;
    private Collection columnFamilies;
    private Range range;
    private boolean inclusive;
    
    private boolean initiated = false;
    
    public SortedListKeyValueIterator(Iterator<Map.Entry<Key,Value>> sourceIterator) {
        this.sourceList = new ArrayList<>();
        while (sourceIterator.hasNext()) {
            sourceList.add(sourceIterator.next());
        }
        this.sourceList.sort(Map.Entry.comparingByKey());
        currentIndex = 0;
    }
    
    /**
     *
     * @param sourceList
     *            non-null
     */
    public SortedListKeyValueIterator(List<Map.Entry<Key,Value>> sourceList) {
        // defensive copy
        this.sourceList = new ArrayList<>(sourceList);
        this.sourceList.sort(Map.Entry.comparingByKey());
        currentIndex = 0;
    }
    
    public SortedListKeyValueIterator(SortedMap<Key,Value> map) {
        this.sourceList = new ArrayList<>(map.entrySet().size());
        currentIndex = 0;
        for (Map.Entry<Key,Value> entry : map.entrySet()) {
            sourceList.add(entry);
        }
    }
    
    public SortedListKeyValueIterator(SortedListKeyValueIterator source) {
        this.sourceList = source.sourceList;
        this.currentIndex = source.currentIndex;
    }
    
    @Override
    public void init(SortedKeyValueIterator sortedKeyValueIterator, Map map, IteratorEnvironment iteratorEnvironment) throws IOException {
        throw new IllegalStateException("unsupported");
    }
    
    @Override
    public boolean hasTop() {
        if (initiated) {
            if (sourceList.size() == currentIndex) {
                return false;
            }
            return sourceList.size() > currentIndex && range.contains(sourceList.get(currentIndex).getKey());
        } else {
            throw new IllegalStateException("can't do this");
        }
    }
    
    @Override
    public void next() {
        if (initiated) {
            currentIndex++;
            while (hasTop() && !acceptColumnFamily(currentIndex)) {
                currentIndex++;
            }
        } else {
            throw new IllegalStateException("can't do this");
        }
    }
    
    @Override
    public WritableComparable<?> getTopKey() {
        if (initiated && hasTop()) {
            return sourceList.get(currentIndex).getKey();
        } else {
            throw new IllegalStateException("can't do this");
        }
    }
    
    @Override
    public Writable getTopValue() {
        if (initiated && hasTop()) {
            return sourceList.get(currentIndex).getValue();
        } else {
            throw new IllegalStateException("can't do this");
        }
    }
    
    @Override
    public SortedKeyValueIterator deepCopy(IteratorEnvironment iteratorEnvironment) {
        return new SortedListKeyValueIterator(this);
    }
    
    @Override
    public void seek(Range range, Collection columnFamilies, boolean inclusive) throws IOException {
        this.columnFamilies = columnFamilies;
        this.inclusive = inclusive;
        this.range = range;
        initiated = true;
        
        int newIndex = 0;
        while (sourceList.size() > newIndex && (!acceptColumnFamily(newIndex) || !acceptRange(newIndex))) {
            newIndex++;
        }
        
        currentIndex = newIndex;
    }
    
    private boolean acceptColumnFamily(int index) {
        return columnFamilies.isEmpty() || (inclusive && columnFamilies.contains(sourceList.get(index).getKey().getColumnFamilyData()))
                        || (!inclusive && !columnFamilies.contains(sourceList.get(index).getKey().getColumnFamilyData()));
    }
    
    private boolean acceptRange(int index) {
        return range.contains(sourceList.get(index).getKey());
    }
}
