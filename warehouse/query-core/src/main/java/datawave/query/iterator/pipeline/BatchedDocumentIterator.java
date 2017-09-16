package datawave.query.iterator.pipeline;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.SortedSet;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import com.google.common.collect.Sets;

public class BatchedDocumentIterator implements Iterator<Entry<Key,Value>> {
    
    SortedSet<Entry<Key,Value>> sortedResponses = null;
    Iterator<Entry<Key,Value>> subIter = null;
    private PipelineIterator parent;
    
    public BatchedDocumentIterator(PipelineIterator parent) {
        this.parent = parent;
        sortedResponses = Sets.newTreeSet(new Comparator<Entry<Key,Value>>() {
            
            @Override
            public int compare(Entry<Key,Value> o1, Entry<Key,Value> o2) {
                return o1.getKey().compareTo(o2.getKey());
            }
            
        });
    }
    
    @Override
    public boolean hasNext() {
        boolean hasResults = false;
        while (parent.hasNext()) {
            sortedResponses.add(parent.next());
            hasResults = true;
        }
        if (hasResults == true)
            subIter = sortedResponses.iterator();
        
        if (subIter == null) {
            return false;
        }
        
        return subIter.hasNext();
    }
    
    @Override
    public Entry<Key,Value> next() {
        return subIter.next();
    }
    
    @Override
    public void remove() {
        
    }
    
}
