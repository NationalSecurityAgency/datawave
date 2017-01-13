package nsa.datawave.query.rewrite.iterator.logic;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

import java.util.Map;
import nsa.datawave.query.rewrite.iterator.NestedIterator;

import com.google.common.collect.TreeMultimap;
import nsa.datawave.query.rewrite.iterator.Util;
import nsa.datawave.query.rewrite.iterator.Util.Transformer;
import org.apache.accumulo.core.data.Key;

class NegationFilter {
    
    /**
     * Checks the value <code>t</code> against the supplied filters. If necessary, the filters are advanced up to at least T before checking.
     * 
     * @param t
     * @return
     */
    public static <T extends Comparable<T>> boolean isFiltered(T t, TreeMultimap<T,NestedIterator<T>> filters, Transformer<T> transformer) {
        // quick check to see if we already know we're supposed to be filtered
        if (filters.containsKey(t)) {
            return true;
        }
        
        Collection<T> currentFilters = new LinkedList<>(filters.keySet().headSet(t));
        
        // return as soon as we find a match
        Iterator<T> needsMoving = currentFilters.iterator();
        while (needsMoving.hasNext()) {
            for (NestedIterator<T> filter : filters.removeAll(needsMoving.next())) {
                T nextFilter = filter.move(t);
                if (nextFilter != null) {
                    T transform = transformer.transform(nextFilter);
                    filters.put(transform, filter);
                    if (transform.equals(t)) {
                        return true;
                    }
                }
            }
        }
        
        return false;
    }
}
