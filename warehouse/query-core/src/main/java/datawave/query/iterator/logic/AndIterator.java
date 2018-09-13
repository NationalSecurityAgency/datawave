package datawave.query.iterator.logic;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

import datawave.query.iterator.Util.Transformer;
import org.apache.log4j.Logger;

import datawave.query.attributes.Document;
import datawave.query.iterator.NestedIterator;
import datawave.query.iterator.Util;

import com.google.common.collect.TreeMultimap;

/**
 * Performs a merge join of the child iterators. It is expected that all child iterators return values in sorted order.
 */
public class AndIterator<T extends Comparable<T>> implements NestedIterator<T> {
    // temporary stores of uninitialized streams of iterators
    private List<NestedIterator<T>> includes, excludes;
    
    private Map<T,T> transforms;
    private Transformer<T> transformer;
    
    private TreeMultimap<T,NestedIterator<T>> includeHeads, excludeHeads;
    private T next;
    
    private Document prevDocument, document;
    
    private static final Logger log = Logger.getLogger(AndIterator.class);
    
    public AndIterator(Iterable<NestedIterator<T>> sources) {
        this(sources, null);
    }
    
    public AndIterator(Iterable<NestedIterator<T>> sources, Iterable<NestedIterator<T>> filters) {
        includes = new LinkedList<>();
        for (NestedIterator<T> src : sources) {
            includes.add(src);
        }
        
        if (filters == null) {
            excludes = Collections.emptyList();
        } else {
            excludes = new LinkedList<>();
            for (NestedIterator<T> filter : filters) {
                excludes.add(filter);
            }
        }
    }
    
    public void initialize() {
        Comparator<T> keyComp = Util.keyComparator();
        // nestedIteratorComparator will keep a deterministic ordering, unlike hashCodeComparator
        Comparator<NestedIterator<T>> itrComp = Util.nestedIteratorComparator();
        
        transformer = Util.keyTransformer();
        transforms = new HashMap<>();
        
        includeHeads = TreeMultimap.create(keyComp, itrComp);
        includeHeads = initSubtree(includeHeads, includes, transformer, transforms, true);
        
        if (excludes.isEmpty()) {
            excludeHeads = Util.getEmpty();
        } else {
            excludeHeads = TreeMultimap.create(keyComp, itrComp);
            // pass null in for transforms as excludes are not returned
            excludeHeads = initSubtree(excludeHeads, excludes, transformer, null, false);
        }
        
        next();
    }
    
    public T next() {
        
        T returnValue = next;
        prevDocument = document;
        
        while (!includeHeads.isEmpty()) {
            SortedSet<T> topKeys = includeHeads.keySet();
            T lowest = topKeys.first();
            T highest = topKeys.last();
            
            if (lowest.equals(highest)) {
                if (!NegationFilter.isFiltered(lowest, excludeHeads, transformer)) {
                    next = transforms.get(lowest);
                    document = Util.buildNewDocument(includeHeads.values());
                    includeHeads = advanceIterators(lowest);
                    break;
                } else {
                    includeHeads = advanceIterators(lowest);
                }
            } else {
                includeHeads = advanceIterators(lowest);
                
            }
            
        }
        
        // if we didn't move after the loop, then we don't have a next after this
        if (returnValue == next) {
            next = null;
        }
        
        return returnValue;
    }
    
    public void remove() {
        throw new UnsupportedOperationException("This iterator does not support remove.");
    }
    
    public boolean hasNext() {
        if (null == includeHeads) {
            throw new IllegalStateException("initialize() was never called");
        }
        
        return next != null;
    }
    
    public T move(T minimum) {
        if (null == includeHeads) {
            throw new IllegalStateException("initialize() was never called");
        }
        
        Set<T> headSet = includeHeads.keySet().headSet(minimum);
        
        // If we are already at `minimum`, we can just call next which will
        // return the current next and seed the next.
        if (headSet.isEmpty()) {
            return next();
        }
        
        // first let's make sure all of the sub trees are at least at `minimum`
        Iterator<T> topKeys = new LinkedList<>(headSet).iterator();
        while (!includeHeads.isEmpty() && topKeys.hasNext()) {
            includeHeads = moveIterators(topKeys.next(), minimum);
        }
        
        next = null;
        next();
        
        // now find the next match and return it; return <code>null</code> if not
        if (hasNext()) {
            return next();
        } else {
            includeHeads = Util.getEmpty();
            return null;
        }
    }
    
    public Collection<NestedIterator<T>> leaves() {
        LinkedList<NestedIterator<T>> leaves = new LinkedList<>();
        for (NestedIterator<T> itr : includes) {
            leaves.addAll(itr.leaves());
        }
        for (NestedIterator<T> itr : excludes) {
            leaves.addAll(itr.leaves());
        }
        return leaves;
    }
    
    @Override
    public Collection<NestedIterator<T>> children() {
        ArrayList<NestedIterator<T>> children = new ArrayList<>(includes.size() + excludes.size());
        
        children.addAll(includes);
        children.addAll(excludes);
        
        return children;
    }
    
    /**
     * Advances all iterators associated with the supplied key and adds them back into the sorted multimap. If any of the sub-trees returns false, this method
     * immediately returns false to indicate that a sub-tree has been exhausted.
     * 
     * @param key
     * @return
     */
    protected TreeMultimap<T,NestedIterator<T>> advanceIterators(T key) {
        transforms.remove(key);
        for (NestedIterator<T> itr : includeHeads.removeAll(key)) {
            if (itr.hasNext()) {
                T next = itr.next();
                T transform = transformer.transform(next);
                transforms.put(transform, next);
                includeHeads.put(transform, itr);
            } else {
                return Util.getEmpty();
            }
        }
        return includeHeads;
    }
    
    /**
     * Similar to <code>advanceIterators</code>, but instead of calling <code>next</code> on each sub-tree, this calls <code>move</code> with the supplied
     * <code>to</code> parameter.
     * 
     * @param key
     * @param to
     * @return
     */
    protected TreeMultimap<T,NestedIterator<T>> moveIterators(T key, T to) {
        transforms.remove(key);
        for (NestedIterator<T> itr : includeHeads.removeAll(key)) {
            T next = itr.move(to);
            if (next == null) {
                return Util.getEmpty();
            } else {
                T transform = transformer.transform(next);
                transforms.put(transform, next);
                includeHeads.put(transform, itr);
            }
        }
        return includeHeads;
    }
    
    /**
     * Creates a sorted mapping of values to iterators.
     * 
     * @param subtree
     * @param sources
     * @return
     */
    private static <T extends Comparable<T>> TreeMultimap<T,NestedIterator<T>> initSubtree(TreeMultimap<T,NestedIterator<T>> subtree,
                    Iterable<NestedIterator<T>> sources, Transformer<T> transformer, Map<T,T> transforms, boolean anded) {
        for (NestedIterator<T> src : sources) {
            src.initialize();
            if (src.hasNext()) {
                T next = src.next();
                T transform = transformer.transform(next);
                if (transforms != null) {
                    transforms.put(transform, next);
                }
                subtree.put(transform, src);
            } else if (anded) {
                // If a source has no valid records, it shouldn't throw an exception. It should just return no results.
                // For an And, once one source is exhausted, the entire tree is exhausted
                return Util.getEmpty();
            }
        }
        return subtree;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("AndIterator: ");
        
        sb.append("Includes: ");
        sb.append(includes);
        sb.append(", Excludes: ");
        sb.append(excludes);
        
        return sb.toString();
    }
    
    public Document document() {
        return prevDocument;
    }
}
