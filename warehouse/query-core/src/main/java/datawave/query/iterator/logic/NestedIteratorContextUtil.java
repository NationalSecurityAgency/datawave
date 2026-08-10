package datawave.query.iterator.logic;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import com.google.common.collect.TreeMultimap;

import datawave.query.iterator.NestedIterator;
import datawave.query.iterator.Util;

/**
 * Class contains utilities for use on iterators that require context for evaluation
 */
public class NestedIteratorContextUtil {

    /**
     * Given a context for evaluation, get all contextRequiredIterators that need to be moved based upon the current head/null maps. Any contextRequiredIterator
     * with a headMap entry less than context or which has an entry in the nullHeadMap needs to be moved. If the iterator hasn't been used at all it will be
     * initialized.
     *
     * The headMap and nullHeadMap will have all entries removed correlating with with NestedIterators which should be moved based on context
     *
     * @param context
     *            the context to be used to move the contextRequiredIterators
     * @param contextRequiredIterators
     *            the iterators to intersect against that require context
     * @param headMap
     *            the contextRequiredIterators head map
     * @param nullHeadMap
     *            the null head map
     * @param <T>
     *            type of the iterator
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
        // take all previously seen keys that are less than context and move them again
        Set<T> keysToMove = new TreeSet<>(headMap.keySet().headSet(context));

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
     *            the context to be used
     * @param sourcesToMove
     *            the sources to move
     * @param headMap
     *            the sourcesToMove head map
     * @param transformer
     *            transformer to apply to all results
     * @param <T>
     *            type of the set
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
     * Intersect the List of context required NestedIterator with the context and return the highest T of the intersection or null if no intersection is
     * possible
     *
     * @param context
     *            the context to be used to move the contextRequiredIterators
     * @param contextRequiredIterators
     *            the iterators to intersect against that require context
     * @param headMap
     *            the contextRequiredIterators head map
     * @param nullHeadMap
     *            the contextRequiredIterators null head map. This is required to track when a move resulted in no intersection with a given context which will
     *            need to always be moved
     * @param transformer
     *            transformer to apply to all results from the contextRequiredIterators
     * @param <T>
     *            type of the iterator
     * @return the highest T of the intersection
     */
    public static <T> T intersect(T context, List<NestedIterator<T>> contextRequiredIterators, TreeMultimap<T,NestedIterator<T>> headMap,
                    TreeMultimap<T,NestedIterator<T>> nullHeadMap, Util.Transformer<T> transformer) {
        return applyContext(context, contextRequiredIterators, headMap, nullHeadMap, transformer, false);
    }

    /**
     * Union the context with the contextRequiredIterators and return the lowest T of the intersection or null if no union is possible
     *
     * @param context
     *            the context to be used to move the contextRequiredIterators
     * @param contextRequiredIterators
     *            the iterators to union against that require context
     * @param headMap
     *            the contextRequiredIterators head map
     * @param nullHeadMap
     *            the contextRequiredIterators null head map. This is required to track when a move resulted in no union with a given context which will need to
     *            always be moved
     * @param transformer
     *            transformer to apply to all results from the contextRequiredIterators
     * @param <T>
     *            type of the iterator
     * @return lowest T of the intersection
     */
    public static <T> T union(T context, List<NestedIterator<T>> contextRequiredIterators, TreeMultimap<T,NestedIterator<T>> headMap,
                    TreeMultimap<T,NestedIterator<T>> nullHeadMap, Util.Transformer<T> transformer) {
        return applyContext(context, contextRequiredIterators, headMap, nullHeadMap, transformer, true);
    }

    /**
     * Apply a context to a List of contextRequiredIterators as either a union or intersection, taking into account previous state of the headmap and
     * nullHeadMap
     *
     * @param context
     *            the context to be used to move the contextRequiredIterators
     * @param contextRequiredIterators
     *            the iterators to union against that require context
     * @param headMap
     *            the contextRequiredIterators head map
     * @param nullHeadMap
     *            the contextRequiredIterators null head map. This is required to track when a move resulted in no result from the iterator and will always be
     *            moved
     * @param transformer
     *            transformer to apply to all results from the contextRequiredIterators
     * @param <T>
     *            type of the iterator
     * @param union
     *            if set to true apply the context as a union with the contextRequiredIterators, otherwise apply as an intersection
     * @return the modified context
     */
    private static <T> T applyContext(T context, List<NestedIterator<T>> contextRequiredIterators, TreeMultimap<T,NestedIterator<T>> headMap,
                    TreeMultimap<T,NestedIterator<T>> nullHeadMap, Util.Transformer<T> transformer, boolean union) {
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

        T headMapKey;
        if (union) {
            // union
            headMapKey = headMap.isEmpty() ? null : headMap.keySet().first();
        } else {
            // intersection
            headMapKey = headMap.isEmpty() ? null : headMap.keySet().last();
        }

        // grab any existing null heads that match the context (not requiring a move due to already being there)
        nullSources.addAll(nullHeadMap.get(context));

        // check for nulls first, since a null source means that the move on the iterator yielded no matches
        if (nullSources.size() > 0) {
            // add them to the nullHeads with the current key
            nullHeadMap.putAll(context, nullSources);

            if (union && nullSources.size() == sourcesToMove.size() || !union) {
                return null;
            }
        }

        // valid headMapKey to return
        return headMapKey;
    }
}
