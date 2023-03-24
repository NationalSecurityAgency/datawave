package datawave.query.iterator.logic;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

import datawave.query.iterator.Util;
import datawave.query.iterator.NestedIterator;

import com.google.common.collect.TreeMultimap;

class NegationFilter {
    
    /**
     * Checks the value <code>t</code> against the supplied filters. If necessary, the filters are advanced up to at least T before checking.
     *
     * @param t
     *            a key
     * @param <T>
     *            type for the key
     * @param filters
     *            filters
     * @param transformer
     *            a transformer
     * @return boolean
     */
    public static <T extends Comparable<T>> boolean isFiltered(T t, TreeMultimap<T,NestedIterator<T>> filters, Util.Transformer<T> transformer) {
        // quick check to see if we already know we're supposed to be filtered
        if (filters.containsKey(t)) {
            return true;
        }
        
        Collection<T> currentFilters = new LinkedList<>(filters.keySet().headSet(t));
        boolean filtered = false;
        // return as soon as we find a match
        Iterator<T> needsMoving = currentFilters.iterator();
        while (needsMoving.hasNext()) {
            for (NestedIterator<T> filter : filters.removeAll(needsMoving.next())) {
                T nextFilter = filter.move(t);
                if (nextFilter != null) {
                    T transform = transformer.transform(nextFilter);
                    filters.put(transform, filter);
                    if (transform.equals(t)) {
                        filtered = true;
                    }
                }
            }
        }
        
        return filtered;
    }
}
