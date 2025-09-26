package datawave.query.iterator.logic;

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
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iteratorsImpl.system.IterationInterruptedException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import com.google.common.collect.TreeMultimap;

import datawave.query.attributes.Document;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.exceptions.WaitWindowOverrunException;
import datawave.query.iterator.NestedIterator;
import datawave.query.iterator.Util;
import datawave.query.iterator.Util.Transformer;
import datawave.query.iterator.waitwindow.WaitWindowObserver;

/**
 * Performs a merge join of the child iterators. It is expected that all child iterators return values in sorted order.
 */
public class AndIterator<T extends Comparable<T>> implements NestedIterator<T> {
    // an id intended to be consistent across rebuilds
    private final String id;

    // temporary stores of uninitialized streams of iterators
    private List<NestedIterator<T>> includes, excludes, contextIncludes, contextExcludes;

    private WaitWindowObserver waitWindowObserver;
    private Map<T,T> transforms;
    private Transformer<T> transformer;

    private TreeMultimap<T,NestedIterator<T>> includeHeads, excludeHeads, contextIncludeHeads, contextExcludeHeads, contextIncludeNullHeads,
                    contextExcludeNullHeads;
    private T prev;
    private T next;

    private Document prevDocument, document;
    private T evaluationContext;

    private static final Logger log = Logger.getLogger(AndIterator.class);

    public AndIterator(Iterable<NestedIterator<T>> sources) {
        this(sources, null, null);
    }

    public AndIterator(Iterable<NestedIterator<T>> sources, Iterable<NestedIterator<T>> filters) {
        this(sources, filters, null);
    }

    public AndIterator(Iterable<NestedIterator<T>> sources, Iterable<NestedIterator<T>> filters, WaitWindowObserver waitWindowObserver) {
        this.waitWindowObserver = waitWindowObserver;
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
        id = String.valueOf((long) includes.toString().hashCode() + excludes.toString().hashCode() + contextIncludes.toString().hashCode()
                        + contextExcludes.toString().hashCode());
    }

    public void initialize() {
        Comparator<T> keyComp = Util.keyComparator();
        // nestedIteratorComparator will keep a deterministic ordering, unlike hashCodeComparator
        Comparator<NestedIterator<T>> itrComp = Util.nestedIteratorComparator();

        try {

            transformer = Util.keyTransformer();
            transforms = new HashMap<>();

            includeHeads = TreeMultimap.create(keyComp, itrComp);
            includeHeads = initSubtree(includeHeads, includes, transformer, transforms, true);

            if (excludes.isEmpty()) {
                excludeHeads = Util.getEmpty();
            } else {
                excludeHeads = TreeMultimap.create(keyComp, itrComp);
                // pass null in for transforms as excludes are not returned
                try {
                    excludeHeads = initSubtree(excludeHeads, excludes, transformer, null, false);
                } catch (WaitWindowOverrunException e) {
                    // excludes could be farther than the includes, so we don't want to use a yieldKey from the exception
                    e.setYieldKey(Pair.of(null, id + ": yield while calling initSubtree for excludes in AndIterator.initialize()"));
                    throw e;
                }
            }

            if (!contextIncludes.isEmpty()) {
                contextIncludeHeads = TreeMultimap.create(keyComp, itrComp);
                contextIncludeNullHeads = TreeMultimap.create(keyComp, itrComp);
            }

            if (contextExcludes != null && !contextExcludes.isEmpty()) {
                contextExcludeHeads = TreeMultimap.create(keyComp, itrComp);
                contextExcludeNullHeads = TreeMultimap.create(keyComp, itrComp);
            }

            next();
        } catch (WaitWindowOverrunException e) {
            Pair<Key,String> possibleYieldKey = null;
            if (prev != null) {
                // if prev != null then it's a match from the previous next() call that has not yet been returned
                // set the exception yieldKey so that it is the only option to consider
                // this shouldn't be possible during initialize
                e.setYieldKey(this.waitWindowObserver.createYieldKey((Key) prev, true, id + ": prev in AndIterator.initialize()"));
            } else if (next != null) {
                // if next != null then it's a match from this next() call that has not yet been returned
                // set the exception yieldKey so that it is the only option to consider
                // this shouldn't be possible during initialize
                e.setYieldKey(this.waitWindowObserver.createYieldKey((Key) next, true, id + ": next in AndIterator.initialize()"));
            } else {
                if (!includeHeads.isEmpty()) {
                    // if no keys were waiting to be returned, then our options are either the
                    // highest key from includeHeads or the yieldKey that is in the exception
                    possibleYieldKey = this.waitWindowObserver.createYieldKey((Key) includeHeads.keySet().last(), true,
                                    "highest includeHead in AndIterator.initialize()");
                }
            }
            // When comparing possible yield keys in the AndIterator, we choose the highest
            // key because the uids of the sources need to be equal to return a match
            // and keys need to be returned in order
            this.waitWindowObserver.propagateException(possibleYieldKey, true, false, e);
        }
    }

    public boolean isInitialized() {
        return includeHeads != null;
    }

    /**
     * Apply a candidate as a context against both contextIncludes and contextExcludes.
     *
     * @param candidate
     *            to be used as context against contextIncludes and contextExcludes
     * @return true if candidate is included in all contextIncludes and excluded in all contextExcludes, false otherwise
     */
    private boolean applyContextRequired(T candidate) {
        if (contextIncludes.size() > 0) {
            T highestContextInclude = NestedIteratorContextUtil.intersect(candidate, contextIncludes, contextIncludeHeads, contextIncludeNullHeads,
                            transformer);
            // if there wasn't an intersection here move to the next one
            if (!candidate.equals(highestContextInclude)) {
                if (highestContextInclude != null) {
                    // move to the next highest key
                    includeHeads = moveIterators(candidate, highestContextInclude);
                    return false;
                } else {
                    // all we know is they didn't intersect advance to next
                    includeHeads = advanceIterators(candidate);
                    return false;
                }
            }
        }

        // test any contextExcludes against candidate
        if (contextExcludes.size() > 0) {
            // DeMorgans Law: (~A) AND (~B) == ~(A OR B)
            // for an exclude union lowest with the set
            try {
                T unionExclude = NestedIteratorContextUtil.union(candidate, contextExcludes, contextExcludeHeads, contextExcludeNullHeads, transformer);
                // if the union matched it is not a hit
                if (candidate.equals(unionExclude)) {
                    // advance and try again
                    includeHeads = advanceIterators(candidate);
                    return false;
                }
            } catch (WaitWindowOverrunException e) {
                // contextExcludes could be farther than the includes, so we don't want to use a yieldKey from the exception
                e.setYieldKey(Pair.of(null, id + ": yield while advancing contextExcludes in AndIterator.applyContextRequired()"));
                throw e;
            }
        }

        return true;
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
        if (isContextRequired() && evaluationContext == null) {
            throw new IllegalStateException("evaluationContext must be set prior to calling next");
        }

        if (log.isDebugEnabled()) {
            log.debug(id + ": next will return " + next + " ; heads at " + includeHeads.keySet());
        }

        prev = next;
        prevDocument = document;
        T lowest = null;
        T highest = null;

        try {
            // look through includes for candidates if there are any
            while (!includeHeads.isEmpty()) {
                SortedSet<T> topKeys = includeHeads.keySet();
                lowest = topKeys.first();
                highest = topKeys.last();

                // short circuit if possible from a supplied evaluation context
                if (evaluationContext != null) {
                    checkWaitWindow(evaluationContext, "evaluationContext short circuit", "AndIterator.next()");
                    int lowestCompare = lowest.compareTo(evaluationContext);
                    int highestCompare = highest.compareTo(evaluationContext);

                    if (lowestCompare > 0 || highestCompare > 0) {
                        // if any value is beyond the evaluationContext it's not possible to intersect
                        break;
                    }

                    // advance anything less than the evaluation context to the evaluation context
                    SortedSet<T> toMove = topKeys.headSet(evaluationContext);
                    if (!toMove.isEmpty()) {
                        includeHeads = moveIterators(toMove, evaluationContext);
                        continue;
                    }
                }
                checkWaitWindow(highest, "highest includeHead", "AndIterator.next()");
                // if the highest and lowest are the same we are currently intersecting
                if (lowest.equals(highest)) {
                    boolean isFiltered;
                    try {
                        // make sure this value isn't filtered
                        isFiltered = NegationFilter.isFiltered(lowest, excludeHeads, transformer);
                    } catch (WaitWindowOverrunException e) {
                        // excludeHeads could be farther than the includes, so we don't want to use a yieldKey from the exception
                        e.setYieldKey(Pair.of(null, id + ": yield while calling NegationFilter.isFiltered with lowest/excludeHeads in AndIterator.next()"));
                        throw e;
                    }
                    if (!isFiltered) {
                        // use this value as a candidate against any includes/excludes that require context
                        if (applyContextRequired(lowest)) {
                            // found a match, set next/document and advance
                            next = transforms.get(lowest);
                            document = Util.buildNewDocument(includeHeads, contextIncludeHeads, lowest);
                            includeHeads = advanceIterators(lowest);
                            break;
                        }
                    } else {
                        // filtered, advance the iterators (which are all currently pointing at the same point)
                        includeHeads = advanceIterators(lowest);
                    }
                } else {
                    // haven't converged yet, take the next highest and move it
                    T nextHighest = topKeys.headSet(highest).last();
                    includeHeads = moveIterators(nextHighest, highest);
                }
            }

            // for cases where there are no sources the only source for a candidate is the evaluationContext.
            if (isContextRequired()) {
                checkWaitWindow(evaluationContext, "evaluationContext", "AndIterator.next()");
                // test exclude for the candidate in case there are excludes
                boolean isFiltered;
                try {
                    // make sure this value isn't filtered
                    isFiltered = NegationFilter.isFiltered(evaluationContext, excludeHeads, transformer);
                } catch (WaitWindowOverrunException e) {
                    // excludeHeads could be farther than the includes, so we don't want to use a yieldKey from the exception
                    e.setYieldKey(Pair.of(null, id + ": yield while calling NegationFilter.isFiltered with excludeHeads in AndIterator.next()"));
                    throw e;
                }
                if (!isFiltered) {
                    if (applyContextRequired(evaluationContext)) {
                        next = evaluationContext;
                        document = Util.buildNewDocument(Collections.emptyList());
                    }
                }
            }
        } catch (WaitWindowOverrunException e) {
            Pair<Key,String> possibleYieldKey = null;
            if (prev != null) {
                // if prev != null then it's a match from the previous next() call that has not yet been returned
                // set the exception yieldKey so that it is the only option to consider
                e.setYieldKey(this.waitWindowObserver.createYieldKey((Key) prev, true, id + ": prev in AndIterator.next()"));
            } else if (next != null) {
                // if next != null then it's a match from this next() call that has not yet been returned
                // set the exception yieldKey so that it is the only option to consider
                e.setYieldKey(this.waitWindowObserver.createYieldKey((Key) next, true, id + ": next in AndIterator.next()"));
            } else {
                // if no keys were waiting to be returned, then our options are either the
                // highest key from includeHeads or the yieldKey that is in the exception
                possibleYieldKey = this.waitWindowObserver.createYieldKey((Key) highest, true, id + ": highest includeHead in AndIterator.next()");
            }
            // When comparing possible yield keys in the AndIterator, we choose the highest
            // key because the uids of the sources need to be equal to return a match
            // and keys need to be returned in order
            this.waitWindowObserver.propagateException(possibleYieldKey, true, false, e);
        }

        // if we didn't move after the loop, then we don't have a next after this
        if (prev == next) {
            next = null;
        }

        if (log.isDebugEnabled()) {
            log.debug(id + ": next returning " + prev + "; cached next is " + next + " ; heads at " + includeHeads.keySet());
        }

        return prev;
    }

    private void checkWaitWindow(T key, String keyDescription, String location) {
        if (this.waitWindowObserver != null) {
            if (prev != null) {
                // if prev != null then it's a match from the previous next() call that has not yet been returned
                this.waitWindowObserver.checkWaitWindow((Key) prev, true, id + ": prev in " + location);
            } else if (next != null) {
                // if next != null then it's a match from this next() call that has not yet been returned
                this.waitWindowObserver.checkWaitWindow((Key) next, true, id + ": next in " + location);
            } else {
                this.waitWindowObserver.checkWaitWindow((Key) key, true, id + ": " + keyDescription + " in " + location);
            }
        }
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
        if (log.isDebugEnabled()) {
            log.debug(id + ": seeking to " + range);
        }

        // seek all iterators. Drop those that fail, as long as we have at least one include left
        Iterator<NestedIterator<T>> include = includes.iterator();
        while (include.hasNext()) {
            NestedIterator<T> child = include.next();
            try {
                try {
                    child.seek(range, columnFamilies, inclusive);
                } catch (IterationInterruptedException e2) {
                    // throw IterationInterrupted exceptions as-is with no modifications so the QueryIterator can handle it
                    throw e2;
                } catch (WaitWindowOverrunException e) {
                    log.warn(id + ": AndIterator.seek() passing through WaitWindowOverrunException: " + e.getMessage());
                    throw e;
                } catch (Exception e2) {
                    if (child.isNonEventField()) {
                        // dropping a non-event term from the query means that the accuracy of the query
                        // cannot be guaranteed. Thus, a fatal exception.
                        log.error(id + ": Lookup of a non-event field failed, failing query");
                        throw new DatawaveFatalQueryException("Lookup of non-event field failed", e2);
                    }
                    // otherwise we can safely drop this term from the intersection as the field will get re-introduced
                    // to the context when the event is aggregated
                    // Note: even though the precision of the query is affected the accuracy is not. i.e., documents that
                    // would have been defeated at the field index will now be defeated at evaluation time
                    throw e2;
                }
            } catch (WaitWindowOverrunException e) {
                log.warn(id + ": AndIterator.seek() passing through WaitWindowOverrunException: " + e.getMessage());
                // When comparing possible yield keys in the AndIterator, we choose the highest
                // key because the uids of the sources need to be equal to return a match
                this.waitWindowObserver.propagateException(null, true, false, e);
            } catch (IterationInterruptedException iie) {
                // allow the QueryIterator to handle these exceptions
                throw iie;
            } catch (Exception e) {
                include.remove();
                if (includes.isEmpty() || e instanceof DatawaveFatalQueryException) {
                    throw e;
                } else {
                    log.warn(id + ": Lookup of event field failed, precision of query reduced.");
                }
            }
        }

        for (NestedIterator<T> contextInclude : contextIncludes) {
            contextInclude.seek(range, columnFamilies, inclusive);
        }

        try {
            for (NestedIterator<T> exclude : excludes) {
                exclude.seek(range, columnFamilies, inclusive);
            }
        } catch (WaitWindowOverrunException e) {
            // excludes could be farther than the includes, so we don't want to use a yieldKey from the exception
            e.setYieldKey(Pair.of(null, id + ": yield while calling seek on excludes in AndIterator.seek()"));
            throw e;
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

        if (log.isDebugEnabled()) {
            log.debug(id + ": moving to " + minimum);
        }

        // test if the cached next is already beyond the minimum
        if (next != null && next.compareTo(minimum) >= 0) {
            // simply advance to next
            return next();
        }

        SortedSet<T> headSet = includeHeads.keySet().headSet(minimum);
        includeHeads = moveIterators(headSet, minimum);

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
     * Advances all iterators associated with the supplied key and adds them back into the sorted multimap. If any of the sub-trees returns false, this method
     * immediately returns false to indicate that a sub-tree has been exhausted.
     *
     * @param key
     *            a key
     * @return a sorted map
     */
    private TreeMultimap<T,NestedIterator<T>> advanceIterators(T key) {
        if (log.isDebugEnabled()) {
            log.debug(id + ": advancing to " + key + ": " + includeHeads.keySet());
        }

        boolean seenException = false;
        T highest = null;
        transforms.remove(key);
        SortedSet<NestedIterator<T>> itrs = new TreeSet<>(includeHeads.removeAll(key)).descendingSet();
        for (NestedIterator<T> itr : itrs) {
            T next;
            try {
                // if there is already a known highest go straight there instead of next
                if (highest != null) {
                    next = itr.move(highest);
                } else if (itr.hasNext()) {
                    next = itr.next();
                } else {
                    return Util.getEmpty();
                }

                if (next == null) {
                    return Util.getEmpty();
                }

                T transform = transformer.transform(next);
                transforms.put(transform, next);
                includeHeads.put(transform, itr);

                // move the highest if the new key is higher than the current key and the highest seen so far
                if ((highest == null && transform.compareTo(key) > 0) || (highest != null && transform.compareTo(highest) > 0)) {
                    highest = transform;
                }
            } catch (WaitWindowOverrunException wwoe) {
                log.warn(id + ": AndIterator.advanceIterators() passing through WaitWindowOverrunException: " + wwoe.getMessage());
                throw wwoe;
            } catch (IterationInterruptedException ie) {
                // allow the QueryIterator to handle these exceptions
                throw ie;
            } catch (Exception e) {
                seenException = true;
                if (itr.isNonEventField()) {
                    // dropping a non-event term from the query means that the accuracy of the query
                    // cannot be guaranteed. Thus, a fatal exception.
                    throw new DatawaveFatalQueryException("Lookup of non-event term failed", e);
                } else {
                    log.warn(id + ": Lookup of event field failed, precision of query reduced.");
                }
            }
        }

        // only need to actually fail if we have nothing left in the AND clause
        if (seenException && includeHeads.isEmpty()) {
            log.error(id + ": Failing query because all iterators within an intersection failed");
            throw new DatawaveFatalQueryException("Exception in underlying iterator was destructive");
        }

        if (log.isDebugEnabled()) {
            log.debug(id + ": advanced to " + key + ": " + includeHeads.keySet());
        }

        return includeHeads;
    }

    /**
     * Similar to <code>advanceIterators</code>, but instead of calling <code>next</code> on each sub-tree, this calls <code>move</code> with the supplied
     * <code>to</code> parameter.
     *
     * @param key
     *            a key
     * @param to
     *            the destination
     * @return a sorted map
     */
    private TreeMultimap<T,NestedIterator<T>> moveIterators(T key, T to) {
        if (log.isDebugEnabled()) {
            log.debug(id + ": moving iterators for " + key + " to " + to + ": " + includeHeads.keySet());
        }
        transforms.remove(key);
        for (NestedIterator<T> itr : includeHeads.removeAll(key)) {
            T next = itr.move(to);
            if (next == null) {
                return Util.getEmpty();
            } else {
                T transform = transformer.transform(next);
                transforms.put(transform, next);
                includeHeads.put(transform, itr);

                if (transform.compareTo(to) > 0) {
                    to = transform;
                }
            }
        }
        if (log.isDebugEnabled()) {
            log.debug(id + ": moved iterators for " + key + " to " + to + ": " + includeHeads.keySet());
        }
        return includeHeads;
    }

    private TreeMultimap<T,NestedIterator<T>> moveIterators(SortedSet<T> toMove, T to) {
        if (log.isDebugEnabled()) {
            log.debug(id + ": moving iterators for " + toMove + " to " + to + ": " + includeHeads.keySet());
        }
        T highest = null;

        // get the highest key of toMove since it is likely to be the lowest cardinality
        if (!toMove.isEmpty()) {
            highest = toMove.last();
        }

        while (highest != null && !includeHeads.isEmpty()) {
            // if include heads is higher than the target key, adjust the target key to the highest since an intersection is not possible at anything but the
            // highest
            T highestInternal = includeHeads.keySet().last();
            if (highestInternal.compareTo(to) > 0) {
                to = highestInternal;
            }

            // advance the iterators
            includeHeads = moveIterators(highest, to);

            // get the next highest key in the set to move
            toMove = toMove.headSet(highest);
            if (!toMove.isEmpty()) {
                highest = toMove.last();
            } else {
                highest = null;
            }
        }

        if (log.isDebugEnabled()) {
            log.debug(id + ": moved iterators for " + toMove + " to " + to + ": " + includeHeads.keySet());
        }
        return includeHeads;
    }

    /**
     * Creates a sorted mapping of values to iterators.
     *
     * @param subtree
     *            a subtree
     * @param <T>
     *            type for the tree
     * @param anded
     *            boolean flag for anded
     * @param transformer
     *            the transformer
     * @param transforms
     *            mapping of transforms
     * @param sources
     *            nested iterator of sources
     * @return a sorted map
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
        StringBuilder sb = new StringBuilder(id + ": AndIterator: ");

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
        return prevDocument;
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

    /**
     * This context will be used even if isContextRequired is false as an anchor point for highest/lowest during next calls
     *
     * @param context
     *            a context
     */
    @Override
    public void setContext(T context) {
        this.evaluationContext = context;
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
}
