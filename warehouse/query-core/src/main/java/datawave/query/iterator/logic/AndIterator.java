package datawave.query.iterator.logic;

import com.google.common.collect.TreeMultimap;
import datawave.query.attributes.Document;
import datawave.query.iterator.NestedIterator;
import datawave.query.iterator.SeekableIterator;
import datawave.query.iterator.Util;
import datawave.query.iterator.Util.Transformer;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.collections.MapUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
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

/**
 * Performs a merge join of the child iterators. It is expected that all child iterators return values in sorted order.
 */
public class AndIterator<T extends Comparable<T>> implements NestedIterator<T>, SeekableIterator {
    // temporary stores of uninitialized streams of iterators
    private List<NestedIterator<T>> includes, excludes;
    
    private Map<T,T> transforms;
    private Transformer<T> transformer;
    
    private TreeMultimap<T,NestedIterator<T>> includeHeads, excludeHeads;
    private T prev;
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
    
    public boolean isInitialized() {
        return includeHeads != null;
    }
    
    /**
     * return the previously found next and set its document. If there are more head references, advance until the lowest and highest match that is not
     * filtered, advancing all iterators tied to lowest and set next/document for the next call
     * 
     * @return the previously found next
     */
    public T next() {
        if (!isInitialized()) {
            throw new IllegalStateException("initialize() was never called");
        }
        
        prev = next;
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
                // jump the lowest to the highest
                includeHeads = moveIterators(lowest, highest);
            }
        }
        
        // if we didn't move after the loop, then we don't have a next after this
        if (prev == next) {
            next = null;
        }
        
        return prev;
    }
    
    public void remove() {
        throw new UnsupportedOperationException("This iterator does not support remove.");
    }
    
    public boolean hasNext() {
        if (!isInitialized()) {
            throw new IllegalStateException("initialize() was never called");
        }
        
        return next != null;
    }
    
    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        // seek all of the iterators. Drop those that fail, as long as we have at least one include left
        Iterator<NestedIterator<T>> include = includes.iterator();
        while (include.hasNext()) {
            NestedIterator<T> child = include.next();
            try {
                for (NestedIterator<T> itr : child.leaves()) {
                    if (itr instanceof SeekableIterator) {
                        ((SeekableIterator) itr).seek(range, columnFamilies, inclusive);
                    }
                }
            } catch (Exception e) {
                include.remove();
                if (includes.isEmpty()) {
                    throw e;
                } else {
                    log.warn("Failed include lookup, but dropping in lieu of other terms", e);
                }
                
            }
        }
        Iterator<NestedIterator<T>> exclude = excludes.iterator();
        while (exclude.hasNext()) {
            NestedIterator<T> child = exclude.next();
            try {
                for (NestedIterator<T> itr : child.leaves()) {
                    if (itr instanceof SeekableIterator) {
                        ((SeekableIterator) itr).seek(range, columnFamilies, inclusive);
                    }
                }
            } catch (Exception e) {
                exclude.remove();
                if (includes.isEmpty()) {
                    throw e;
                } else {
                    log.warn("Failed include lookup, but dropping in lieu of other terms", e);
                }
            }
        }
        
        if (isInitialized()) {
            // advance throwing next away and re-populating next with what should be
            next();
        }
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
        if (!isInitialized()) {
            throw new IllegalStateException("initialize() was never called");
        }
        
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
    
    public Collection<NestedIterator<T>> leaves() {
        LinkedList<NestedIterator<T>> leaves = new LinkedList<>();
        // treat this node as a leaf as it's seek
        leaves.add(this);
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
            try {
                if (itr.hasNext()) {
                    T next = itr.next();
                    T transform = transformer.transform(next);
                    transforms.put(transform, next);
                    includeHeads.put(transform, itr);
                } else {
                    return Util.getEmpty();
                }
            } catch (Exception e) {
                // only need to actually fail if we have nothing left in the AND clause
                if (includeHeads.isEmpty()) {
                    throw e;
                } else {
                    log.warn("Failed include lookup, but dropping in lieu of other terms", e);
                }
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
