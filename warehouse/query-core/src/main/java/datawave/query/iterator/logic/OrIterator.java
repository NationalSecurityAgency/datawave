package datawave.query.iterator.logic;

import com.google.common.collect.TreeMultimap;
import datawave.query.attributes.Document;
import datawave.query.iterator.NestedIterator;
import datawave.query.iterator.Util;

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
import java.util.TreeSet;

/**
 * Performs a deduping merge of iterators.
 *
 * 
 * @param <T>
 */
public class OrIterator<T extends Comparable<T>> implements NestedIterator<T> {
    // temporary stores of uninitialized streams of iterators
    private List<NestedIterator<T>> includes, deferredIncludes, deferredExcludes;
    
    private Map<T,T> transforms;
    private Util.Transformer<T> transformer;
    
    private TreeMultimap<T,NestedIterator<T>> includeHeads, deferredIncludeHeads, deferredIncludeNullHeads, deferredExcludeHeads, deferredExcludeNullHeads;
    
    private T prev;
    private T next;
    
    private Document prevDocument, document;
    
    private T deferredContext;
    
    public OrIterator(Iterable<NestedIterator<T>> sources) {
        this(sources, null);
    }
    
    public OrIterator(Iterable<NestedIterator<T>> sources, Iterable<NestedIterator<T>> filters) {
        includes = new LinkedList<>();
        deferredIncludes = new LinkedList<>();
        for (NestedIterator<T> src : sources) {
            if (src.isDeferred()) {
                deferredIncludes.add(src);
            } else {
                includes.add(src);
            }
        }
        
        if (filters == null) {
            deferredExcludes = Collections.emptyList();
        } else {
            deferredExcludes = new LinkedList<>();
            for (NestedIterator<T> filter : filters) {
                deferredExcludes.add(filter);
            }
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
        transforms = new HashMap<>();
        
        includeHeads = TreeMultimap.create(keyComp, itrComp);
        initSubtree(includeHeads, includes, transformer, transforms, false);
        
        if (deferredIncludes.size() > 0) {
            deferredIncludeHeads = TreeMultimap.create(keyComp, itrComp);
            deferredIncludeNullHeads = TreeMultimap.create(keyComp, itrComp);
        }
        
        if (deferredExcludes.size() > 0) {
            deferredExcludeHeads = TreeMultimap.create(keyComp, itrComp);
            deferredExcludeNullHeads = TreeMultimap.create(keyComp, itrComp);
        }
        
        next();
    }
    
    public boolean hasNext() {
        if (null == includeHeads) {
            throw new IllegalStateException("initialize() was never called");
        }
        
        return next != null;
    }
    
    /**
     * return the previously found next and set its document. If there are more head references, get the lowest, advancing all iterators tied to lowest and set
     * next/document for the next call
     * 
     * @return the previously found next
     */
    public T next() {
        if (isDeferred() && deferredContext == null) {
            throw new IllegalStateException("deferredContext must be set prior to each next");
        }
        
        prev = next;
        prevDocument = document;
        
        SortedSet<T> candidateSet = new TreeSet<>(Util.keyComparator());
        T lowest;
        if (includeHeads.keySet().size() > 0) {
            lowest = includeHeads.keySet().first();
            candidateSet.add(lowest);
        }
        T lowestDeferred = DeferredIterator.evalOr(deferredContext, deferredIncludes, deferredIncludeHeads, deferredIncludeNullHeads, transformer);
        if (lowestDeferred != null) {
            candidateSet.add(lowestDeferred);
        }
        T lowestNegatedDeferred = DeferredIterator
                        .evalOrNegated(deferredContext, deferredExcludes, deferredExcludeHeads, deferredExcludeNullHeads, transformer);
        if (deferredContext != null && deferredContext.equals(lowestNegatedDeferred)) {
            candidateSet.add(deferredContext);
        }
        
        // take the lowest of the candidates
        if (candidateSet.size() > 0) {
            lowest = candidateSet.first();
            
            if (lowest.equals(lowestDeferred)) {
                next = lowestDeferred;
                document = Util.buildNewDocument(deferredIncludeHeads.get(next));
            } else if (lowest.equals(deferredContext)) {
                next = deferredContext;
                document = Util.buildNewDocument(Collections.emptyList());
            } else {
                next = transforms.get(lowest);
                document = Util.buildNewDocument(includeHeads.get(lowest));
            }
            
            // regardless of where we hit make sure to advance includeHeads if it matches there
            if (includeHeads.get(lowest) != null) {
                includeHeads = advanceIterators(lowest);
            }
        }
        
        // the loop couldn't find a new next, so set next to null because we're done after this
        if (prev == next) {
            next = null;
        }
        
        return prev;
    }
    
    /**
     * Test all layers of cache for the minimum, then if necessary advance heads
     * 
     * @param minimum
     *            the minimum to return
     * @return the first greater than or equal to minimum or null if none exists
     * @throws IllegalStateException
     *             if prev is greater than or equal to minimum
     */
    public T move(T minimum) {
        if (null == includeHeads) {
            throw new IllegalStateException("initialize() was never called");
        }
        
        // test preconditions
        if (prev != null && prev.compareTo(minimum) >= 0) {
            throw new IllegalStateException("Tried to call move when already at or beyond move point: topkey=" + prev + ", movekey=" + minimum);
        }
        
        // test if the cached next is already beyond the minimum
        if (next != null && next.compareTo(minimum) >= 0) {
            // simply advance to next
            return next();
        }
        
        Set<T> headSet = includeHeads.keySet().headSet(minimum);
        
        // some iterators need to be moved into the target range before recalculating the next
        Iterator<T> topKeys = new LinkedList<>(headSet).iterator();
        while (!includeHeads.isEmpty() && topKeys.hasNext()) {
            // advance each iterator that is under the threshold
            includeHeads = moveIterators(topKeys.next(), minimum);
        }
        
        // next < minimum, so advance throwing next away and re-populating next with what should be >= minimum
        next();
        
        // now as long as the newly computed next exists return it and advance
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
        
        for (NestedIterator<T> itr : deferredIncludes) {
            leaves.addAll(itr.leaves());
        }
        
        for (NestedIterator<T> itr : deferredExcludes) {
            leaves.addAll(itr.leaves());
        }
        
        // TODO add deferred?
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
        ArrayList<NestedIterator<T>> children = new ArrayList<>(includes.size());
        
        children.addAll(includes);
        
        // TODO add deferred?
        
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
        sb.append(", Deferred Includes: ");
        sb.append(deferredIncludes);
        sb.append(", Deferred Excludes: ");
        sb.append(deferredExcludes);
        
        return sb.toString();
    }
    
    @Override
    public boolean isDeferred() {
        return !deferredExcludes.isEmpty() || !deferredIncludes.isEmpty();
    }
    
    @Override
    public void setDeferredContext(T context) {
        this.deferredContext = context;
    }
}
