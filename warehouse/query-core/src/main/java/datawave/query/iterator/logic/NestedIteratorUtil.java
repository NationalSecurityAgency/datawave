package datawave.query.iterator.logic;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import com.google.common.collect.TreeMultimap;

import datawave.query.iterator.NestedIterator;
import datawave.query.iterator.Util;
import datawave.query.iterator.Util.Transformer;

/**
 * Collection of utility methods for NestedIterators
 * <p>
 * These common methods help initialize, advance, and move collections of nested iterators
 */
public class NestedIteratorUtil {

    private static final Logger log = Logger.getLogger(NestedIteratorUtil.class);

    private NestedIteratorUtil() {
        // utility class
    }

    /**
     * Initialize sources by calling next (without a move)
     *
     * @param headMap
     *            the map of iterators to top keys
     * @param sources
     *            the source iterators
     * @param expiredSources
     *            collection of iterators whose context expired
     * @param exhaustedSources
     *            collection of iterators with no more elements
     * @param <T>
     *            type param
     */
    public static <T> void initializeSources(TreeMultimap<T,NestedIterator<T>> headMap, List<NestedIterator<T>> sources, List<NestedIterator<T>> expiredSources,
                    List<NestedIterator<T>> exhaustedSources, Transformer<T> transformer) {
        if (sources.isEmpty()) {
            return;
        }

        for (NestedIterator<T> source : sources) {
            source.initialize(); // some iterators still require this
            if (source.hasNext()) {
                T result = source.next();
                if (result != null) {
                    T transform = transformer.transform(result);
                    headMap.put(transform, source);
                } else if (Util.isIteratorExhausted(source)) {
                    exhaustedSources.add(source);
                } else {
                    expiredSources.add(source);
                }
            } else {
                exhaustedSources.add(source);
            }
        }
    }

    /**
     * Initialize sources by moving to the provided minimum
     *
     * @param minimum
     *            the move target
     * @param headMap
     *            the map of iterators to top keys
     * @param sources
     *            the source iterators
     * @param expiredSources
     *            collection of iterators whose context has expired
     * @param exhaustedSources
     *            collection of iterators with no more elements
     * @param <T>
     *            type param
     */
    public static <T> void initializeSources(T minimum, TreeMultimap<T,NestedIterator<T>> headMap, List<NestedIterator<T>> sources,
                    List<NestedIterator<T>> expiredSources, List<NestedIterator<T>> exhaustedSources, Transformer<T> transformer) {
        if (sources.isEmpty()) {
            return;
        }

        for (NestedIterator<T> source : sources) {
            source.initialize(); // some iterators require this
            T result = source.move(minimum);
            if (result != null) {
                T transformed = transformer.transform(result);
                headMap.put(transformed, source);
            } else if (Util.isIteratorExhausted(source)) {
                exhaustedSources.add(source);
            } else {
                // no exact match found for a source like (A || !B)
                expiredSources.add(source);
            }
        }
    }

    /**
     * Advance source iterators
     *
     * @param headMap
     *            the map of iterators to top keys
     * @param sources
     *            the source iterators
     * @param exhaustedSources
     *            collection of iterators with no more elements
     * @param <T>
     *            the type param
     */
    public static <T> void advanceSources(TreeMultimap<T,NestedIterator<T>> headMap, Collection<NestedIterator<T>> sources,
                    List<NestedIterator<T>> exhaustedSources, Transformer<T> transformer) {
        if (sources.isEmpty()) {
            return;
        }

        for (NestedIterator<T> source : sources) {
            if (source.hasNext()) {
                T result = source.next();
                if (result != null) {
                    T transformed = transformer.transform(result);
                    headMap.put(transformed, source);
                } else if (Util.isIteratorExhausted(source)) {
                    exhaustedSources.add(source);
                } else {
                    log.error("advancing a source should not expire it");
                    throw new IllegalStateException("expired source while advancing");
                }
            } else {
                exhaustedSources.add(source);
            }
        }
    }

    /**
     * Move sources to the provided minimum. The caller handles any context-specific results of the move (i.e., an intersection clearing all sources if any one
     * source is exhausted, a union taking the lowest element)
     *
     * @param minimum
     *            the move target
     * @param headMap
     *            map of iterators to top keys
     * @param expiredSources
     *            collection of iterators whose context has expired
     * @param exhaustedSources
     *            collection of iterators with no more elements
     * @param <T>
     *            type param
     */
    public static <T> void moveSources(T minimum, TreeMultimap<T,NestedIterator<T>> headMap, List<NestedIterator<T>> expiredSources,
                    List<NestedIterator<T>> exhaustedSources, Transformer<T> transformer) {
        if (headMap.isEmpty()) {
            return;
        }

        for (T key : new TreeSet<>(headMap.keySet().headSet(minimum))) {
            for (NestedIterator<T> source : headMap.removeAll(key)) {
                T result = source.move(minimum);
                if (result != null) {
                    T transformed = transformer.transform(result);
                    headMap.put(transformed, source);
                } else if (Util.isIteratorExhausted(source)) {
                    exhaustedSources.add(source);
                } else {
                    expiredSources.add(source);
                }
            }
        }
    }

    /**
     * Move expired sources to the provided minimum
     *
     * @param minimum
     *            the move target
     * @param headMap
     *            map of iterators to top keys
     * @param expiredSources
     *            collection of iterators whose context has expired
     * @param exhaustedSources
     *            collection of iterators with no more elements
     * @param <T>
     *            type param
     */
    public static <T> void moveExpiredSources(T minimum, TreeMultimap<T,NestedIterator<T>> headMap, List<NestedIterator<T>> expiredSources,
                    List<NestedIterator<T>> exhaustedSources, Transformer<T> transformer) {
        if (expiredSources.isEmpty()) {
            return;
        }

        List<NestedIterator<T>> sources = new LinkedList<>(expiredSources);
        expiredSources.clear();
        for (NestedIterator<T> source : sources) {
            T result = source.move(minimum);
            if (result != null) {
                T transformed = transformer.transform(result);
                headMap.put(transformed, source);
            } else if (Util.isIteratorExhausted(source)) {
                exhaustedSources.add(source);
            } else {
                expiredSources.add(source);
            }
        }
    }
}
