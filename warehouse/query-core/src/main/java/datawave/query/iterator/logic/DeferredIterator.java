package datawave.query.iterator.logic;

import com.google.common.collect.TreeMultimap;
import datawave.query.iterator.NestedIterator;
import datawave.query.iterator.Util;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DeferredIterator {
    
    /**
     * Given t, get all bound iterators that are less than t in headMap, or have never been evaluated. Will remove iterators from the headMap
     * 
     * @param t
     * @param deferredIterators
     * @param headMap
     * @param <T>
     * @return a non-null Set of initialized NestedIterators which should be moved
     */
    private static <T> Set<NestedIterator<T>> getIteratorsToMove(T t, List<NestedIterator<T>> deferredIterators, TreeMultimap<T,NestedIterator<T>> headMap,
                    TreeMultimap<T,NestedIterator<T>> nullHeadMap) {
        
        Set<NestedIterator<T>> sourcesToMove = new HashSet<>();
        
        // find all the sources that have never been moved
        for (NestedIterator<T> source : deferredIterators) {
            if (!headMap.values().contains(source) && !nullHeadMap.values().contains(source)) {
                // first time touching this source, initialize it
                if (source.isDeferred()) {
                    source.setDeferredContext(t);
                }
                source.initialize();
                sourcesToMove.add(source);
            }
        }
        // take all previously seen keys that are less than t and move them again
        Set<T> keysToMove = headMap.keySet().headSet(t);
        
        // find and remove these sources, note that they have now been removed from the headMap nothing should return early until they are re-added
        for (T key : keysToMove) {
            sourcesToMove.addAll(headMap.removeAll(key));
        }
        
        // take all the nullHead map that need to be moved as well
        Set<T> nullKeysToMove = nullHeadMap.keySet().headSet(t);
        for (T key : nullKeysToMove) {
            sourcesToMove.addAll(nullHeadMap.removeAll(key));
        }
        
        return sourcesToMove;
    }
    
    /**
     * Process moves against the set of sources to move, setting the deferredContext and move on each. Will update the headMap with new references except for
     * null responses
     * 
     * @param t
     * @param sourcesToMove
     * @param headMap
     * @param transformer
     * @param <T>
     * @return non-null Set of null response iterators
     */
    private static <T> Set<NestedIterator<T>> processMoves(T t, Set<NestedIterator<T>> sourcesToMove, TreeMultimap<T,NestedIterator<T>> headMap,
                    Util.Transformer<T> transformer) {
        Set<NestedIterator<T>> nullSources = new HashSet<>();
        for (NestedIterator<T> deferred : sourcesToMove) {
            deferred.setDeferredContext(t);
            T result = deferred.move(t);
            if (result == null) {
                // beyond the end of the iterator
                nullSources.add(deferred);
            } else {
                T transformed = transformer.transform(result);
                headMap.put(transformed, deferred);
            }
        }
        
        return nullSources;
    }
    
    /**
     *
     * @param t
     * @param deferredIterators
     * @param headMap
     * @param transformer
     * @return null if unmatched, otherwise the highest that came out of the transforms, or t if no deferredIterators exist
     */
    public static <T> T evalAnd(T t, List<NestedIterator<T>> deferredIterators, TreeMultimap<T,NestedIterator<T>> headMap,
                    TreeMultimap<T,NestedIterator<T>> nullHeadMap, Util.Transformer<T> transformer) {
        // there is nothing to compare against, so accept
        if (headMap == null) {
            return t;
        }
        
        Set<NestedIterator<T>> sourcesToMove = getIteratorsToMove(t, deferredIterators, headMap, nullHeadMap);
        Set<NestedIterator<T>> nullSources = processMoves(t, sourcesToMove, headMap, transformer);
        
        // get this key before pushing sources back in for null
        T headMapKey = headMap.keySet().size() > 0 ? headMap.keySet().last() : null;
        
        // check for nulls first
        if (nullSources.size() > 0) {
            nullHeadMap.putAll(t, nullSources);
            
            // at least one deferred source didn't pass
            return null;
        }
        
        // if there were transforms that didn't match t, return the largest
        if (headMap.keySet().size() > 0) {
            return headMapKey;
        }
        
        // if we got here there were no deferredIterators, so accept
        return t;
    }
    
    /**
     *
     * @param t
     * @param deferredIterators
     * @param headMap
     * @param nullHeadMap
     * @param transformer
     * @param <T>
     * @return null if all deferredIterators do not match with no return key or there are none, the first key that wasn't null otherwise
     */
    public static <T> T evalAndNegated(T t, List<NestedIterator<T>> deferredIterators, TreeMultimap<T,NestedIterator<T>> headMap,
                    TreeMultimap<T,NestedIterator<T>> nullHeadMap, Util.Transformer<T> transformer) {
        if (headMap == null) {
            // no deferredIterators
            return null;
        }
        
        if (headMap.containsKey(t)) {
            // its already known to be excluded
            return t;
        }
        
        // grab anything less than the current or that has never been initialized
        Set<NestedIterator<T>> sourcesToMove = getIteratorsToMove(t, deferredIterators, headMap, nullHeadMap);
        Set<NestedIterator<T>> nullSources = processMoves(t, sourcesToMove, headMap, transformer);
        
        // get this key before pushing sources back in for null
        T headMapKey = headMap.keySet().size() > 0 ? headMap.keySet().first() : null;
        
        // check for nulls first
        if (nullSources.size() > 0) {
            nullHeadMap.putAll(t, nullSources);
            
            // if all the sources were null short circuit
            if (nullSources.size() == sourcesToMove.size()) {
                return null;
            }
        }
        
        // if there were transforms that didn't match t, return the smallest
        if (headMap.keySet().size() > 0) {
            return headMapKey;
        }
        
        // if we got here each move was successful and equal to t or there were none
        return t;
    }
    
    /**
     *
     * @param t
     * @param deferredIterators
     * @param headMap
     * @param nullHeadMap
     * @param transformer
     * @param <T>
     * @return null if filtered or cannot be processed, otherwise the lowest key returned by processing sources
     */
    public static <T> T evalOr(T t, List<NestedIterator<T>> deferredIterators, TreeMultimap<T,NestedIterator<T>> headMap,
                    TreeMultimap<T,NestedIterator<T>> nullHeadMap, Util.Transformer<T> transformer) {
        if (t == null) {
            return null;
        }
        
        // there is nothing to compare against, so nothing
        if (headMap == null) {
            return null;
        }
        
        Set<NestedIterator<T>> sourcesToMove = getIteratorsToMove(t, deferredIterators, headMap, nullHeadMap);
        // null iterators from previous passes should always be moved again
        Set<NestedIterator<T>> nullSources = processMoves(t, sourcesToMove, headMap, transformer);
        
        // get this key before pushing sources back in for null
        T headMapKey = headMap.keySet().size() > 0 ? headMap.keySet().first() : null;
        
        // check for nulls first
        if (nullSources.size() > 0) {
            // add them to the nullHeads with the current key
            nullHeadMap.putAll(t, nullSources);
            
            // if all the sources were null short circuit
            if (nullSources.size() == sourcesToMove.size()) {
                return null;
            }
        }
        
        // if there were transforms that didn't match t, return the largest
        if (headMap.keySet().size() > 0) {
            return headMapKey;
        }
        
        // check for filtered
        if (nullHeadMap.get(t).size() > 0) {
            return null;
        }
        
        // if we got here there were no
        return t;
    }
    
    /**
     * Get the lowest acceptable T that is not filtered
     * 
     * @param t
     * @param deferredIterators
     * @param headMap
     * @param nullHeadMap
     * @param transformer
     * @param <T>
     * @return null if filtered or cannot be computed, t if not filtered
     */
    public static <T> T evalOrNegated(T t, List<NestedIterator<T>> deferredIterators, TreeMultimap<T,NestedIterator<T>> headMap,
                    TreeMultimap<T,NestedIterator<T>> nullHeadMap, Util.Transformer<T> transformer) {
        if (t == null) {
            return null;
        }
        
        if (headMap == null) {
            // no deferredIterators
            return null;
        }
        
        // if there is at least one deferredExhausted we don't need to check anything else
        if (nullHeadMap.keySet().contains(t)) {
            return t;
        }
        
        // grab anything less than the current or that has never been initialized
        Set<NestedIterator<T>> sourcesToMove = getIteratorsToMove(t, deferredIterators, headMap, nullHeadMap);
        Set<NestedIterator<T>> nullSources = processMoves(t, sourcesToMove, headMap, transformer);
        
        // check for nulls first
        if (nullSources.size() > 0) {
            nullHeadMap.putAll(t, nullSources);
            
            // if all the sources were null short circuit
            if (nullSources.size() > 0) {
                return t;
            }
        }
        
        // in the negated case as long as at least one key doesn't match return t
        if (headMap.keySet().stream().anyMatch(k -> !k.equals(t))) {
            return t;
        }
        
        // filtered out
        return null;
    }
}
