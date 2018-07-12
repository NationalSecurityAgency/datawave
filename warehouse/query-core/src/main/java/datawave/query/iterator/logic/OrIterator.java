package datawave.query.iterator.logic;

import com.google.common.collect.TreeMultimap;
import datawave.query.attributes.Document;
import datawave.query.iterator.NestedIterator;
import datawave.query.iterator.Util;
import datawave.query.iterator.filter.composite.CompositePredicateFilter;
import datawave.query.iterator.filter.composite.CompositePredicateFilterer;
import org.apache.commons.jexl2.parser.JexlNode;

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

/**
 * Performs a deduping merge of iterators.
 *
 * NOTE***** if however sortedUIDs is false, then deduping is not performed and filters cannot be applied *****NOTE
 * 
 * @param <T>
 */
public class OrIterator<T extends Comparable<T>> implements NestedIterator<T>, CompositePredicateFilterer {
    // temporary stores of uninitialized streams of iterators
    private List<NestedIterator<T>> includes, excludes;
    
    private Map<T,T> transforms;
    private Util.Transformer<T> transformer;
    
    private TreeMultimap<T,NestedIterator<T>> includeHeads, excludeHeads;
    
    // sortedUIDs is normally true, however in some circumstances it may not in which case we cannot assume the underlying iterators are returning sorted
    // UIDs. When this is true, we cannot advance iterators based on returned keys.
    private final boolean sortedUIDs;
    
    private T next;
    
    private Document prevDocument, document;
    
    public OrIterator(Iterable<NestedIterator<T>> sources, boolean sortedUIDs) {
        this(sources, null, sortedUIDs);
    }
    
    public OrIterator(Iterable<NestedIterator<T>> sources, Iterable<NestedIterator<T>> filters, boolean sortedUIDs) {
        this.sortedUIDs = sortedUIDs;
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
        if (!excludes.isEmpty() && !sortedUIDs) {
            throw new UnsupportedOperationException("Cannot apply filters if sortedUIDs is false");
        }
    }
    
    /**
     * Allows creators of this iterator to defer creating the sorted mapping of values to iterators until some condition is met. This is intended to let us
     * build the tree of iterators in <code>init()</code> and defer sorting the iterators until after <code>seek()</code> is called.
     */
    public void initialize() {
        Comparator<T> keyComp = Util.keyComparator();
        // nestedIteratorComparator will keep a deterministic ordering, unlike hashCodeComparator
        Comparator<NestedIterator<T>> itrComp = Util.nestedIteratorComparator();
        
        transformer = Util.keyTransformer();
        transforms = new HashMap<T,T>();
        
        includeHeads = TreeMultimap.create(keyComp, itrComp);
        initSubtree(includeHeads, includes, transformer, transforms, false);
        
        if (excludes.isEmpty()) {
            excludeHeads = Util.getEmpty();
        } else {
            excludeHeads = TreeMultimap.create(keyComp, itrComp);
            // pass null in for transforms as excludes are not returned
            initSubtree(excludeHeads, excludes, transformer, null, false);
        }
        
        next();
    }
    
    public boolean hasNext() {
        if (null == includeHeads) {
            throw new IllegalStateException("initialize() was never called");
        }
        
        return next != null;
    }
    
    public T next() {
        T returnVal = next;
        prevDocument = document;
        
        while (!includeHeads.isEmpty()) {
            T lowest = includeHeads.keySet().first();
            if (!NegationFilter.isFiltered(lowest, excludeHeads, transformer)) {
                next = transforms.get(lowest);
                document = Util.buildNewDocument(includeHeads.get(lowest));
                includeHeads = advanceIterators(lowest);
                break;
            } else {
                includeHeads = advanceIterators(lowest);
            }
        }
        
        // the loop couldn't find a new next, so set next to null because we're done after this
        if (returnVal == next) {
            next = null;
        }
        
        return returnVal;
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
    
    /**
     * Advances all iterators associated with the supplied key and adds them back into the sorted multimap. If any of the sub-trees returns false, then they are
     * dropped.
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
            if (next != null) {
                T transform = transformer.transform(next);
                transforms.put(transform, next);
                includeHeads.put(transform, itr);
            }
        }
        return includeHeads;
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
    
    public void remove() {
        throw new UnsupportedOperationException("This iterator does not support remove.");
    }
    
    public Document document() {
        return prevDocument;
    }
    
    @Override
    public Collection<NestedIterator<T>> children() {
        ArrayList<NestedIterator<T>> children = new ArrayList<>(includes.size() + excludes.size());
        
        children.addAll(includes);
        children.addAll(excludes);
        
        return children;
    }
    
    private static <T extends Comparable<T>> TreeMultimap<T,NestedIterator<T>> initSubtree(TreeMultimap<T,NestedIterator<T>> subtree,
                    Iterable<NestedIterator<T>> sources, Util.Transformer<T> transformer, Map<T,T> transforms, boolean anded) {
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
        StringBuilder sb = new StringBuilder("OrIterator: ");
        
        sb.append("Includes: ");
        sb.append(includes);
        sb.append(", Excludes: ");
        sb.append(excludes);
        
        return sb.toString();
    }
    
    @Override
    public void addCompositePredicates(Set<JexlNode> compositePredicates) {
        for (NestedIterator include : includes)
            if (include instanceof CompositePredicateFilter)
                ((CompositePredicateFilter) include).addCompositePredicates(compositePredicates);
    }
}
