package datawave.query.iterator.logic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedSet;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.log4j.Logger;

import com.google.common.collect.TreeMultimap;

import datawave.query.attributes.Document;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.iterator.NestedIterator;
import datawave.query.iterator.SeekableIterator;
import datawave.query.iterator.Util;
import datawave.query.iterator.Util.Transformer;

/**
 * Performs a merge join of the child iterators. It is expected that all child iterators return values in sorted order.
 */
public class AndIterator<T extends Comparable<T>> implements SeekableIterator, NestedIterator<T> {
    private static final Logger log = Logger.getLogger(AndIterator.class);
    private final Transformer<T> transformer = Util.keyTransformer();

    // temporary stores of uninitialized streams of iterators
    private final List<NestedIterator<T>> includes;
    private final List<NestedIterator<T>> excludes;
    private final List<NestedIterator<T>> contextIncludes;
    private final List<NestedIterator<T>> contextExcludes;

    // head maps
    private final TreeMultimap<T,NestedIterator<T>> includeHeads;
    private final TreeMultimap<T,NestedIterator<T>> excludeHeads;
    private final TreeMultimap<T,NestedIterator<T>> contextIncludeHeads;
    private final TreeMultimap<T,NestedIterator<T>> contextExcludeHeads;

    // expired sources
    private final List<NestedIterator<T>> expiredIncludes = new LinkedList<>();
    private final List<NestedIterator<T>> expiredExcludes = new LinkedList<>();
    private final List<NestedIterator<T>> expiredContextIncludes = new LinkedList<>();
    private final List<NestedIterator<T>> expiredContextExcludes = new LinkedList<>();

    // exhausted sources
    private final List<NestedIterator<T>> exhaustedIncludes = new LinkedList<>();
    private final List<NestedIterator<T>> exhaustedExcludes = new LinkedList<>();
    private final List<NestedIterator<T>> exhaustedContextIncludes = new LinkedList<>();
    private final List<NestedIterator<T>> exhaustedContextExcludes = new LinkedList<>();

    private T next;
    private Document document;

    public AndIterator(Iterable<NestedIterator<T>> sources) {
        this(sources, null);
    }

    public AndIterator(Iterable<NestedIterator<T>> sources, Iterable<NestedIterator<T>> filters) {
        includes = new LinkedList<>();
        contextIncludes = new LinkedList<>();
        for (NestedIterator<T> src : sources) {
            if (src.isContextRequired()) {
                contextIncludes.add(src);
            } else {
                includes.add(src);
            }
        }

        if (filters == null) {
            excludes = Collections.emptyList();
            contextExcludes = Collections.emptyList();
        } else {
            excludes = new LinkedList<>();
            contextExcludes = new LinkedList<>();
            for (NestedIterator<T> filter : filters) {
                if (filter.isContextRequired()) {
                    contextExcludes.add(filter);
                } else {
                    excludes.add(filter);
                }
            }
        }

        // initialize head maps
        includeHeads = createHeadMap();
        excludeHeads = createHeadMap();
        contextIncludeHeads = createHeadMap();
        contextExcludeHeads = createHeadMap();
    }

    public void initialize() {
        // no-op
    }

    /**
     * Create a sorted map of nested iterators mapped by their top keys.
     *
     * @return a map of nested iterators
     */
    private TreeMultimap<T,NestedIterator<T>> createHeadMap() {
        // nestedIteratorComparator will keep a deterministic ordering, unlike hashCodeComparator
        return TreeMultimap.create(Util.keyComparator(), Util.nestedIteratorComparator());
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        for (NestedIterator<T> child : children()) {
            try {
                child.seek(range, columnFamilies, inclusive);
            } catch (Exception e) {
                if (child.isNonEventField()) {
                    // dropping a non-event term from the query means that the accuracy of the query
                    // cannot be guaranteed. Thus, a fatal exception.
                    log.error("Lookup of a non-event field failed, failing query");
                    throw new DatawaveFatalQueryException("Lookup of non-event field failed", e);
                } else {
                    // otherwise we can safely drop this term from the intersection as the field will get re-introduced
                    // to the context when the event is aggregated
                    // Note: even though the precision of the query is affected the accuracy is not. i.e., documents that
                    // would have been defeated at the field index will now be defeated at evaluation time
                    removeIterator(child);

                    if (includes.isEmpty()) {
                        // no iterators remain
                        throw new DatawaveFatalQueryException("Query failed because all terms within an intersection were dropped", e);
                    } else {
                        log.warn("Lookup of event field failed, precision of query reduced.");
                    }
                }
            }
        }
    }

    /**
     * Is there a next element
     *
     * @return true if there is a next element
     */
    public boolean hasNext() {
        if (isContextRequired()) {
            throw new IllegalStateException("Cannot call hasNext() on an iterator that requires context");
        }

        if (!isIncludeInitialized()) {
            initializeIncludes();
            findNext();
        } else {
            nextIncludes();
            findNext();
        }

        return next != null;
    }

    /**
     * return the previously found next and set its document. If there are more head references, advance until the lowest and highest match that is not
     * filtered, advancing all iterators tied to lowest and set next/document for the next call
     * <p>
     *
     * @return the previously found next
     */
    public T next() {
        if (isContextRequired()) {
            throw new IllegalStateException("cannot call 'next' on an intersection that requires context");
        }

        T nextValue = next;
        next = null;
        return nextValue;
    }

    /**
     * Calculates the next element
     */
    private void findNext() {
        next = null;
        document = Util.emptyDocument();

        // look through includes for candidates if there are any
        while (!includeHeads.isEmpty()) {
            SortedSet<T> topKeys = includeHeads.keySet();
            T lowest = topKeys.first();
            T highest = topKeys.last();

            if (!lowest.equals(highest)) {
                advanceIncludes();
                continue;
            }

            if (!expiredIncludes.isEmpty()) {
                moveExpiredIncludeSources(highest);
                // if expired sources still exist after moving, then we must advance any includes again
                if (!expiredIncludes.isEmpty()) {
                    advanceIncludes();
                }
                continue;
            }

            T exclude = moveExcludes(lowest);
            if (exclude == null) {
                nextIncludes();
                continue;
            }

            T contextInclude = moveContextIncludes(lowest);
            if (contextInclude == null) {
                // context expired, need a new one from parent iterator
                nextIncludes();
                continue;
            }

            T contextExclude = moveContextExcludes(lowest);
            if (contextExclude == null) {
                nextIncludes();
                continue;
            }

            // found a match, set next/document and advance
            next = includeHeads.keySet().first();
            document = Util.buildNewDocument(includeHeads, contextIncludeHeads, lowest);
            break;
        }
    }

    /**
     * Advance all include iterators to the next intersection
     */
    private void advanceIncludes() {
        while (!includeHeads.isEmpty()) {
            SortedSet<T> topKeys = includeHeads.keySet();
            T lowest = topKeys.first();
            T highest = topKeys.last();

            // adding an expired include for the AndIterator causes an infinite loop somewhere..
            if (lowest.equals(highest) && expiredIncludes.isEmpty()) {
                return;
            }

            if (!includeHeads.keySet().headSet(highest).isEmpty()) {
                moveIncludeSources(highest);
            } else if (includeHeads.keySet().contains(highest)) {
                // need to advance all sources that map to the highest key
                SortedSet<NestedIterator<T>> sources = includeHeads.removeAll(highest);
                for (NestedIterator<T> source : sources) {
                    if (source.hasNext()) {
                        T result = source.next();
                        if (result != null) {
                            T transformed = transformer.transform(result);
                            includeHeads.put(transformed, source);
                            // optimization, if the returned element is beyond the highest element, reset highest
                            if (transformed.compareTo(highest) > 0) {
                                highest = transformed;
                            }
                        } else if (!Util.isIteratorExhausted(source)) {
                            // an include source can expire in a situation like
                            // CONTEXT && (A || (B && !C)) which will end up having an expired iterator
                            expiredIncludes.add(source);
                        } else {
                            // iterator exhausted, we're done
                            exhaustedIncludes.addAll(includeHeads.values());
                            exhaustedIncludes.addAll(sources);
                            exhaustedIncludes.addAll(expiredIncludes);
                            includeHeads.clear();
                            return;
                        }
                    } else {
                        // iterator exhausted, we're done
                        exhaustedIncludes.addAll(includeHeads.values());
                        exhaustedIncludes.addAll(sources);
                        exhaustedIncludes.addAll(expiredIncludes);
                        includeHeads.clear();
                        return;
                    }
                }
            }

            // moving expired sources must happen last. Otherwise, we could move into a valid hit
            // and immediately call next on the sources.
            moveExpiredIncludeSources(highest);
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("This iterator does not support remove.");
    }

    /**
     * Handles removing an iterator from the underlying collections if an exception occurs.
     *
     * @param iterator
     *            the iterator to remove
     */
    private void removeIterator(NestedIterator<T> iterator) {
        log.error("removing iterator");
        if (includes != null) {
            includes.remove(iterator);
        }
        if (contextIncludes != null) {
            contextIncludes.remove(iterator);
        }
        if (excludes != null) {
            excludes.remove(iterator);
        }
        if (contextExcludes != null) {
            contextExcludes.remove(iterator);
        }
    }

    /**
     * Move all iterators to the supplied minimum without converging beyond it, even if context is not required.
     * <p>
     * The minimum is returned IFF
     * <ol>
     * <li>ALL includes match</li>
     * <li>ALL context includes match</li>
     * <li>NO excludes match</li>
     * <li>NO context includes match</li>
     * </ol>
     *
     * @param minimum
     *            the minimum to return
     * @return the first greater than or equal to minimum or null if none exists
     * @throws IllegalStateException
     *             if prev is greater than or equal to minimum
     */
    public T move(T minimum) {
        // 0. test if the cached next is already beyond the minimum
        if (next != null) {
            int result = next.compareTo(minimum);
            if (result == 0) {
                return minimum; // matching
            } else if (result > 0) {
                return null; // next lies beyond the minimum, no match possible
            }
        }

        next = null;
        document = Util.emptyDocument();

        // 1. move include sources
        T include = moveIncludes(minimum);
        if (!exhaustedIncludes.isEmpty()) {
            return null; // include sources exhausted by move, no match possible
        } else if (include != null && include.compareTo(minimum) > 0) {
            minimum = include; // did not match minimum
        }

        // 2. sources for context include cannot return a higher element, example: (A || !B)
        T contextInclude = moveContextIncludes(minimum);
        if (contextInclude == null) {
            return null; // no match
        } // implicit match

        // explicit check to make sure results match
        if (include != null && !include.equals(contextInclude)) {
            throw new IllegalStateException("include and context include did not match");
        }

        // 3. if excludes exist, they must not exclude the minimum
        T exclude = moveExcludes(minimum);
        if (exclude == null) {
            return null; // minimum was excluded
        }

        // 4. if contextExcludes exist, they must not exclude the minimum
        T contextExclude = moveContextExcludes(minimum);
        if (contextExclude == null) {
            return null; // minimum was excluded
        }

        // build document from include and context include iterators
        document = Util.buildNewDocument(includeHeads, contextIncludeHeads, minimum);

        // minimum may have been set to a higher value, if this point is reached it was not excluded
        return minimum;
    }

    public Collection<NestedIterator<T>> leaves() {
        LinkedList<NestedIterator<T>> leaves = new LinkedList<>();
        // treat this node as a leaf to allow us to pass through the seek method and appropriately drop branches if possible.
        leaves.add(this);
        return leaves;
    }

    @Override
    public Collection<NestedIterator<T>> children() {
        ArrayList<NestedIterator<T>> children = new ArrayList<>(includes.size() + excludes.size() + contextIncludes.size() + contextExcludes.size());

        children.addAll(includes);
        children.addAll(excludes);
        children.addAll(contextIncludes);
        children.addAll(contextExcludes);

        return children;
    }

    /**
     * Advance the include iterators to the supplied minimum
     *
     * @param minimum
     *            the context
     * @return the minimum, or the next lowest intersection
     */
    private T moveIncludes(T minimum) {

        // 0. check for short circuit
        if (includes.isEmpty() && includeHeads.isEmpty() && expiredIncludes.isEmpty() && exhaustedIncludes.isEmpty()) {
            return minimum; // no includes, match by default
        }

        if (!isIncludeInitialized()) {
            initializeIncludes();
        }

        T highest = minimum;
        while (!includeHeads.isEmpty()) {

            // get the highest key
            if (includeHeads.keySet().last().compareTo(minimum) > 0) {
                highest = includeHeads.keySet().last();
            }

            // move the iterators
            moveIncludeSources(highest);

            // test for intersection
            if (!includeHeads.isEmpty()) {
                T first = includeHeads.keySet().first();
                T last = includeHeads.keySet().last();
                if (first.equals(last)) {
                    return first;
                }
            }
        }

        return null;
    }

    /**
     * Move all include sources to the provided minimum
     *
     * @param minimum
     *            the minimum element
     */
    private void moveIncludeSources(T minimum) {
        NestedIteratorUtil.moveSources(minimum, includeHeads, expiredIncludes, exhaustedIncludes, transformer);

        if (!exhaustedIncludes.isEmpty()) {
            exhaustedIncludes.addAll(includeHeads.values());
            includeHeads.clear();
        }
    }

    /**
     * Move all expired include sources to the provided minimum
     *
     * @param minimum
     *            the move target
     */
    private void moveExpiredIncludeSources(T minimum) {
        NestedIteratorUtil.moveExpiredSources(minimum, includeHeads, expiredIncludes, exhaustedIncludes, transformer);
    }

    /**
     * Move all excludes to the supplied minimum context. If all exclude iterators are exhausted then simply return the minimum.
     * <ol>
     * <li>Initialize excludes if necessary</li>
     * <li>Move expired excludes if necessary (might not be possible)</li>
     * <li>Move exclude sources</li>
     * </ol>
     * <p>
     * For example: <code>(!A &amp;&amp; !B)</code> or <code>(!A &amp;&amp; !(B || C))</code>
     *
     * @param minimum
     *            the minimum context
     * @return the minimum if it was not excluded
     */
    private T moveExcludes(T minimum) {

        // 0. check for short circuit
        if (excludes.isEmpty() && excludeHeads.isEmpty() && expiredExcludes.isEmpty() && exhaustedExcludes.isEmpty()) {
            return minimum; // nothing to do
        }

        // 1. Initialize excludes if necessary
        if (!isExcludesInitialized()) {
            initializeExcludeSources(minimum);
        } else {
            // 2. Move expired exclude sources if necessary
            moveExpiredExcludeSources(minimum);

            // 3. Move exclude sources
            moveExcludeSources(minimum);
        }

        if (log.isTraceEnabled()) {
            log.trace("move exclude to " + uidFromKey(minimum) + " heads: " + excludeHeads.size() + " expired: " + expiredExcludes.size());
        }

        // the minimum is matched IFF none of the sources match. Exhausted = automatic non-match
        if (!excludeHeads.containsKey(minimum)) {
            return minimum;
        } else {
            return null;
        }
    }

    private String uidFromKey(T minimum) {
        if (!(minimum instanceof Key)) {
            return "no  uid";
        }
        return ((Key) minimum).getColumnFamily().toString().split("\u0000")[1];
    }

    private boolean isExcludesInitialized() {
        return !(!excludes.isEmpty() && excludeHeads.isEmpty() && expiredExcludes.isEmpty() && exhaustedExcludes.isEmpty());
    }

    /**
     * Initialize all exclude sources
     */
    private void initializeExcludeSources(T minimum) {
        NestedIteratorUtil.initializeSources(minimum, excludeHeads, excludes, expiredIncludes, exhaustedExcludes, transformer);
    }

    /**
     * Move all expired exclude sources to the provided minimum
     *
     * @param minimum
     *            the minimum element
     */
    private void moveExpiredExcludeSources(T minimum) {
        NestedIteratorUtil.moveExpiredSources(minimum, excludeHeads, expiredExcludes, exhaustedExcludes, transformer);
    }

    /**
     * Move the exclude sources to the provided minimum
     *
     * @param minimum
     *            the minimum element
     */
    private void moveExcludeSources(T minimum) {
        NestedIteratorUtil.moveSources(minimum, excludeHeads, expiredExcludes, exhaustedExcludes, transformer);
    }

    /**
     * Move all context includes to the supplied minimum. Returns the context if it matches or null otherwise
     * <ol>
     * <li>Initialize context includes if necessary</li>
     * <li>Move expired iterators if necessary</li>
     * <li>Move context includes</li>
     * </ol>
     *
     * @param minimum
     *            the minimum context
     * @return the context if matches, otherwise null
     */
    private T moveContextIncludes(T minimum) {

        // 0. check for short circuit
        if (contextIncludes.isEmpty() && contextIncludeHeads.isEmpty() && expiredContextIncludes.isEmpty() && exhaustedContextIncludes.isEmpty()) {
            return minimum;
        }

        // 1. Initialize context include sources if necessary
        if (!isContextIncludesInitialized()) {
            initializeContextIncludeSources(minimum);
        } else if (!expiredContextIncludes.isEmpty()) {
            // 2. Move expired context includes if necessary
            moveExpiredContextIncludeSources(minimum);
        }

        if (contextIncludes.isEmpty() && !expiredContextIncludes.isEmpty()) {
            return minimum; // no context includes is an automatic match
        }

        // 3. Move context include sources
        moveContextIncludeSources(minimum);

        // TODO -- need to check source return type (leaf vs. junction) to get the real answer
        if (contextIncludeHeads.keySet().size() == 1) {
            T top = contextIncludeHeads.keySet().iterator().next();
            if (!top.equals(minimum)) {
                // should not be possible to return a 'next higher' element
                throw new IllegalStateException("top did not match minimum for AndIterator context include");
            }
            return top;
        } else {
            return null;
        }
    }

    private boolean isContextIncludesInitialized() {
        return !(!contextIncludes.isEmpty() && contextIncludeHeads.isEmpty() && expiredContextIncludes.isEmpty());
    }

    /**
     * Initialize context includes using the provided minimum
     *
     * @param minimum
     *            the minimum element
     */
    private void initializeContextIncludeSources(T minimum) {
        NestedIteratorUtil.initializeSources(minimum, contextIncludeHeads, contextIncludes, expiredContextIncludes, exhaustedContextIncludes, transformer);
    }

    /**
     * Move all expired context include sources to the provided minimum
     *
     * @param minimum
     *            the minimum element
     */
    private void moveExpiredContextIncludeSources(T minimum) {
        NestedIteratorUtil.moveExpiredSources(minimum, contextIncludeHeads, expiredContextIncludes, exhaustedContextIncludes, transformer);
    }

    /**
     * Move context include sources to the provided minimum
     *
     * @param minimum
     *            the minimum element
     */
    private void moveContextIncludeSources(T minimum) {
        NestedIteratorUtil.moveSources(minimum, contextIncludeHeads, expiredContextIncludes, exhaustedContextIncludes, transformer);
    }

    /**
     * Move all context exclude sources to the provided minimum
     * <ol>
     * <li>initialize excludes if necessary</li>
     * <li>move expired excludes if necessary</li>
     * <li>move exclude sources</li>
     * </ol>
     *
     * @param minimum
     *            the minimum element
     * @return the context if it was not excluded, or null otherwise
     */
    private T moveContextExcludes(T minimum) {

        // 0. check for short circuit
        if (contextExcludes.isEmpty() && contextExcludeHeads.isEmpty() && expiredContextExcludes.isEmpty() && exhaustedContextExcludes.isEmpty()) {
            return minimum; // no context excludes is an automatic match
        }

        // 1. initialize exclude sources if necessary
        if (!isContextExcludesInitialized()) {
            initializeContextExcludes(minimum);
        }

        // 2. move expired context excludes
        if (!expiredContextExcludes.isEmpty()) {
            moveExpiredContextExcludeSources(minimum);
        }

        // 3. move context exclude sources
        moveContextExcludeSources(minimum);

        if (log.isTraceEnabled()) {
            log.trace("move context exclude to " + uidFromKey(minimum) + " heads: " + contextExcludeHeads.size() + " expired: "
                            + expiredContextExcludes.size());
        }

        // TODO -- need to check source return type (leaf vs. junction) to get the real answer
        if (contextExcludeHeads.keySet().size() == 1) {
            if (contextExcludeHeads.keySet().contains(minimum)) {
                return minimum;
            } else {
                return null;
            }
        }

        if (!expiredContextExcludes.isEmpty()) {
            return null;
        } else {
            return minimum;
        }
    }

    private boolean isContextExcludesInitialized() {
        return !(!contextExcludes.isEmpty() && contextExcludeHeads.isEmpty() && expiredContextExcludes.isEmpty());
    }

    private void initializeContextExcludes(T minimum) {
        NestedIteratorUtil.initializeSources(minimum, contextExcludeHeads, contextExcludes, expiredContextExcludes, exhaustedContextExcludes, transformer);
    }

    private void moveExpiredContextExcludeSources(T minimum) {
        NestedIteratorUtil.moveExpiredSources(minimum, contextExcludeHeads, expiredContextExcludes, exhaustedContextExcludes, transformer);
    }

    private void moveContextExcludeSources(T minimum) {
        NestedIteratorUtil.moveSources(minimum, contextExcludeHeads, expiredContextExcludes, exhaustedContextExcludes, transformer);

        if (!exhaustedContextExcludes.isEmpty()) {
            throw new IllegalStateException("exhausted context exclude");
        }
    }

    /**
     * Call next on all includes
     */
    private void nextIncludes() {
        if (!isIncludeInitialized()) {
            initializeIncludes();
        }

        for (T key : includeHeads.keySet()) {
            nextIncludes(key);
        }
    }

    /**
     * Call next on all includes that map to the provided key
     *
     * @param key
     *            a top key in the head map
     */
    private void nextIncludes(T key) {
        SortedSet<NestedIterator<T>> sources = includeHeads.removeAll(key);
        for (NestedIterator<T> source : sources) {
            try {
                if (source.hasNext()) {
                    T result = source.next();
                    if (result != null) {
                        T transformed = transformer.transform(result);
                        includeHeads.put(transformed, source);
                    } else {
                        exhaustedIncludes.add(source);
                        exhaustedIncludes.addAll(sources);
                        exhaustedIncludes.addAll(includeHeads.values());
                        includeHeads.clear();
                        return;
                    }
                } else if (!Util.isIteratorExhausted(source)) {
                    expiredIncludes.add(source);
                } else {
                    exhaustedIncludes.add(source);
                    exhaustedIncludes.addAll(sources);
                    exhaustedIncludes.addAll(includeHeads.values());
                    includeHeads.clear();
                    return;
                }
            } catch (Exception e) {
                removeIterator(source);
                if (source.isNonEventField()) {
                    log.error("Lookup of a non-event field failed, failing query");
                    throw new DatawaveFatalQueryException("Lookup of non-event field failed", e);
                } else if (includes.isEmpty()) {
                    throw new DatawaveFatalQueryException("Query failed because all terms within an intersection were dropped", e);
                } else {
                    log.warn("Lookup of event field failed, precision of query reduced.");
                }
            }
        }
    }

    // === include utilities ===

    private boolean isIncludeInitialized() {
        return !includes.isEmpty() && (!includeHeads.isEmpty() || !exhaustedIncludes.isEmpty());
    }

    private void initializeIncludes() {
        NestedIteratorUtil.initializeSources(includeHeads, includes, expiredIncludes, exhaustedIncludes, transformer);

        if (!exhaustedIncludes.isEmpty()) {
            exhaustedIncludes.addAll(includeHeads.values());
            includeHeads.clear();
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("AndIterator: ");

        sb.append("Includes: ");
        sb.append(includes);
        sb.append(", Deferred Includes: ");
        sb.append(contextIncludes);
        sb.append(", Excludes: ");
        sb.append(excludes);
        sb.append(", Deferred Excludes: ");
        sb.append(contextExcludes);

        return sb.toString();
    }

    public Document document() {
        return document;
    }

    /**
     * As long as there is at least one sourced included no context is required
     *
     * @return true if there are no includes, false otherwise
     */
    @Override
    public boolean isContextRequired() {
        return includes.isEmpty();
    }

    @Override
    public boolean isNonEventField() {
        for (NestedIterator<T> itr : includes) {
            if (itr.isNonEventField()) {
                return true;
            }
        }
        for (NestedIterator<T> itr : contextIncludes) {
            if (itr.isNonEventField()) {
                return true;
            }
        }
        for (NestedIterator<T> itr : excludes) {
            if (itr.isNonEventField()) {
                return true;
            }
        }
        for (NestedIterator<T> itr : contextExcludes) {
            if (itr.isNonEventField()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Distinct from a {@link Iterator#hasNext()} call, this method determines if a next element is possible.
     *
     * @return true if this iterator is exhausted
     */
    public boolean isIteratorExhausted() {
        return next == null && includeHeads.isEmpty() && excludeHeads.isEmpty() && contextIncludeHeads.isEmpty() && contextExcludeHeads.isEmpty();
    }
}
