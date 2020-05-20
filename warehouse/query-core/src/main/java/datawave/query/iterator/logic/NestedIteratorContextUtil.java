package datawave.query.iterator.logic;

import com.google.common.collect.TreeMultimap;
import datawave.query.iterator.NestedIterator;
import datawave.query.iterator.Util;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Class contains utilities for use on iterators that require context for evaluation
 */
public class NestedIteratorContextUtil {
    
    /**
     * Given t, get all bound iterators that are less than t in headMap, or have never been evaluated. Will remove iterators from the headMap
     * 
     * @param context
     * @param contextRequiredIterators
     * @param headMap
     * @param <T>
     * @return a non-null Set of initialized NestedIterators which should be moved
     */
    private static <T> Set<NestedIterator<T>> getIteratorsToMove(T context, List<NestedIterator<T>> contextRequiredIterators,
                    TreeMultimap<T,NestedIterator<T>> headMap, TreeMultimap<T,NestedIterator<T>> nullHeadMap) {
        
        Set<NestedIterator<T>> sourcesToMove = new HashSet<>();
        
        // find all the sources that have never been moved
        for (NestedIterator<T> source : contextRequiredIterators) {
            if (!headMap.values().contains(source) && !nullHeadMap.values().contains(source)) {
                // first time touching this source, initialize it
                if (source.isContextRequired()) {
                    source.setContext(context);
                }
                source.initialize();
                sourcesToMove.add(source);
            }
        }
        // take all previously seen keys that are less than t and move them again
        Set<T> keysToMove = headMap.keySet().headSet(context);
        
        // find and remove these sources, note that they have now been removed from the headMap nothing should return early until they are re-added
        for (T key : keysToMove) {
            sourcesToMove.addAll(headMap.removeAll(key));
        }
        
        // take all the nullHead map that need to be moved as well
        Set<T> nullKeysToMove = nullHeadMap.keySet().headSet(context);
        for (T key : nullKeysToMove) {
            sourcesToMove.addAll(nullHeadMap.removeAll(key));
        }
        
        return sourcesToMove;
    }
    
    /**
     * Process moves against the set of sources. Set the context and move each. Will update the headMap with new references except for null responses. A null
     * response indicates that the iterator had no key's left when moving to context
     * 
     * @param context
     * @param sourcesToMove
     * @param headMap
     * @param transformer
     * @param <T>
     * @return non-null Set of null response iterators
     */
    private static <T> Set<NestedIterator<T>> processMoves(T context, Set<NestedIterator<T>> sourcesToMove, TreeMultimap<T,NestedIterator<T>> headMap,
                    Util.Transformer<T> transformer) {
        Set<NestedIterator<T>> nullSources = new HashSet<>();
        for (NestedIterator<T> contextRequiredIterator : sourcesToMove) {
            contextRequiredIterator.setContext(context);
            T result = contextRequiredIterator.move(context);
            if (result == null) {
                // beyond the end of the iterator
                nullSources.add(contextRequiredIterator);
            } else {
                T transformed = transformer.transform(result);
                headMap.put(transformed, contextRequiredIterator);
            }
        }
        
        return nullSources;
    }
    
    /**
     * Intersect the List of NestedIterator with the context and return the highest T of the intersection or null if no intersection is possible
     *
     * @param context
     * @param contextRequiredIterators
     * @param headMap
     * @param nullHeadMap
     * @param transformer
     *
     * @return
     */
    public static <T> T intersect(T context, List<NestedIterator<T>> contextRequiredIterators, TreeMultimap<T,NestedIterator<T>> headMap,
                    TreeMultimap<T,NestedIterator<T>> nullHeadMap, Util.Transformer<T> transformer) {
        // no intersection with nothing
        if (headMap == null) {
            return null;
        }
        
        Set<NestedIterator<T>> sourcesToMove = getIteratorsToMove(context, contextRequiredIterators, headMap, nullHeadMap);
        Set<NestedIterator<T>> nullSources = processMoves(context, sourcesToMove, headMap, transformer);
        
        // get the latest key in the head map or null if there is nothing
        T headMapKey = headMap.keySet().size() > 0 ? headMap.keySet().last() : null;
        
        // nulls indicate that a source had no match to the context so cannot be intersected
        if (nullSources.size() > 0) {
            nullHeadMap.putAll(context, nullSources);
            
            // at least one contextRequiredIterator source could not be intersected
            return null;
        }
        
        // if there were no null sources then take the last entry in the head map which will match context if intersected, or be the highest value across all
        // iterators that was non-null if not intersected
        return headMapKey;
    }
    
    /**
     * Union the context with the contextRequiredIterators and return the lowest T of the intersection or null if no union is possible
     *
     * @param context
     * @param contextRequiredIterators
     * @param headMap
     * @param nullHeadMap
     * @param transformer
     * 
     * @return
     */
    public static <T> T union(T context, List<NestedIterator<T>> contextRequiredIterators, TreeMultimap<T,NestedIterator<T>> headMap,
                    TreeMultimap<T,NestedIterator<T>> nullHeadMap, Util.Transformer<T> transformer) {
        if (context == null) {
            return null;
        }
        
        // no union with nothing
        if (headMap == null) {
            return null;
        }
        
        Set<NestedIterator<T>> sourcesToMove = getIteratorsToMove(context, contextRequiredIterators, headMap, nullHeadMap);
        // null iterators from previous passes should always be moved again
        Set<NestedIterator<T>> nullSources = processMoves(context, sourcesToMove, headMap, transformer);
        
        // get the lowest key in the headMap since only one match is required for union
        T headMapKey = headMap.keySet().size() > 0 ? headMap.keySet().first() : null;
        
        // check for nulls first, since a null source means that the move on the iterator yielded no matches
        if (nullSources.size() > 0) {
            // add them to the nullHeads with the current key
            nullHeadMap.putAll(context, nullSources);
            
            // if all the sources were null there is no union
            if (nullSources.size() == sourcesToMove.size()) {
                return null;
            }
        }
        
        // if all the sources were not null there will be a headMapKey and it is the union
        return headMapKey;
    }
}
