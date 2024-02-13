package datawave.query.iterator.logic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Range;
import org.apache.log4j.Logger;

import com.google.common.collect.TreeMultimap;

import datawave.query.attributes.Document;
import datawave.query.iterator.NestedIterator;
import datawave.query.iterator.Util;
import datawave.query.iterator.Util.Transformer;

/**
 * Performs a de-duping merge of iterators.
 * <p>
 * A brief explanation of the OrIterator lexicon
 * <ol>
 * <li>includes: a positive single term, an intersection of positive terms, or an intersection of both positive and negative terms</li> For example:
 * <code>A, (A &amp;&amp; B), or (A &amp;&amp; !B)</code>
 * <li>contextIncludes: an intersection of unions that contain positive and negative terms</li> For example: <code>(A || !B) &amp;&amp; (C || !D)</code>
 * <li>contextExcludes: a negative term or an intersection of negative terms</li> For example: <code>!A or (!A &amp;&amp; !B)</code>
 * </ol>
 * Context is required to fully evaluate a negated term. Thus, the OrIterator requires context if ANY iterator in the union requires context.
 * <p>
 * Iterators whose underlying source is exhausted are tracked so that the OrIterator can apply short circuit logic in special cases. An exclude that is
 * exhausted means that this union always matches.
 * <p>
 * 'Expired' iterators are a special case. In most cases an iterator has a distinct top element that acts as a key in the various 'head maps'. In the case of an
 * intersection of negated terms it is difficult to determine the true top key. Consider the following case: "(!A and !B)".
 * <p>
 * Both the A-term and the B-term map to elements [1, 2, 8, 9]
 * <p>
 * When a move is issued to element 5 the intersection matches via negation. The underlying sources both map to element 8, but the intersection just matched
 * against element 5. This iterator is now in an 'expired' state and will receive special handling on subsequent move calls.
 */
public class OrIterator<T extends Comparable<T>> implements NestedIterator<T> {

    private static final Logger log = Logger.getLogger(OrIterator.class);

    private final Transformer<T> transformer = Util.keyTransformer();

    // temporary stores of uninitialized streams of iterators
    private final List<NestedIterator<T>> includes = new LinkedList<>();
    private final List<NestedIterator<T>> contextIncludes = new LinkedList<>();
    private final List<NestedIterator<T>> contextExcludes = new LinkedList<>();

    // headmaps for storing iterators when a top element is calculated
    private final TreeMultimap<T,NestedIterator<T>> includeHeads;
    private final TreeMultimap<T,NestedIterator<T>> contextIncludeHeads;
    private final TreeMultimap<T,NestedIterator<T>> contextExcludeHeads;

    // for context iterators that do not have a top element after a move or next is called
    // move contract implies expiration for includes, not just context iterators
    private final List<NestedIterator<T>> expiredIncludes = new LinkedList<>();
    private final List<NestedIterator<T>> expiredContextIncludes = new LinkedList<>();
    private final List<NestedIterator<T>> expiredContextExcludes = new LinkedList<>();

    // union has special handling for an exhausted iterator
    private final List<NestedIterator<T>> exhaustedIncludes = new LinkedList<>();
    private final List<NestedIterator<T>> exhaustedContextIncludes = new LinkedList<>();
    private final List<NestedIterator<T>> exhaustedContextExcludes = new LinkedList<>();

    private T next;
    private Document document;

    private boolean nonEventField;

    public OrIterator(Iterable<NestedIterator<T>> includes) {
        this(includes, Collections.emptyList());
    }

    public OrIterator(Iterable<NestedIterator<T>> includes, Iterable<NestedIterator<T>> excludes) {
        for (NestedIterator<T> include : includes) {
            if (include.isContextRequired()) {
                contextIncludes.add(include);
            } else {
                this.includes.add(include);
            }
        }

        for (NestedIterator<T> exclude : excludes) {
            contextExcludes.add(exclude);
        }

        // initialize head maps
        includeHeads = createHeadMap();
        contextIncludeHeads = createHeadMap();
        contextExcludeHeads = createHeadMap();
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
            child.seek(range, columnFamilies, inclusive);
        }
    }

    /**
     * Determines if a next element exists. If a next element exists, handles setting the top element and populating the document.
     *
     * @return true if there is a next element
     */
    public boolean hasNext() {
        if (isContextRequired()) {
            throw new IllegalStateException("Cannot call hasNext() on an iterator that requires context");
        }

        // initialize or advance
        if (!isIncludeInitialized()) {
            initializeIncludes();
        } else {
            advanceIncludes();

            // advance all expired iterators that do not require context.
            // an iterator without context may expire if it does not match a context.
            if (!expiredIncludes.isEmpty()) {
                Collection<NestedIterator<T>> sources = new LinkedList<>(expiredIncludes);
                expiredIncludes.clear();
                NestedIteratorUtil.advanceSources(includeHeads, sources, exhaustedIncludes, transformer);
            }
        }

        if (!includeHeads.isEmpty()) {
            next = includeHeads.keySet().first();
            document = Util.buildNewDocument(includeHeads, contextIncludeHeads, next);
        } else {
            next = null;
            document = Util.emptyDocument();
        }

        return next != null;
    }

    /**
     * Return the previously found next. The document was populated during the previous move or hasNext call.
     * <p>
     * If context is not required all iterators tied to the lowest element are advanced.
     * <p>
     * If context is required then any next element is calculated during the call to <code>'move(context)'</code>
     *
     * @return the previously found next
     */
    public T next() {
        if (isContextRequired()) {
            throw new IllegalStateException("cannot call 'next' on a union that requires context");
        }

        T nextValue = next;
        next = null;
        return nextValue;
    }

    /**
     * Move all iterators to the provided minimum and return an element according to the include/exclude rules of a union. This method may be called even if
     * context is not required.
     * <p>
     * The minimum is returned IFF
     * <ol>
     * <li>ANY include matches</li>
     * <li>ANY context include matches</li>
     * <li>ANY contextExclude does NOT match</li>
     * </ol>
     * It may seem better to attempt an exclude first and save some seeks on the include and context include iterators, but the possibility of a non-event field
     * prevents this optimization from being used -- that is, include and context include sources must be advanced.
     *
     * @param minimum
     *            the minimum to return
     * @return the minimum if this iterator matches or null if none exists
     * @throws IllegalStateException
     *             if prev is greater than or equal to minimum
     */
    public T move(T minimum) {

        if (next != null && next.equals(minimum)) {
            // already at the context
            return minimum;
        }

        next = null;
        document = Util.emptyDocument();

        // move includes first due to possibility of a non-event field that is required for document aggregation
        // i.e., failure to fully aggregate a non-event field at this stage results in a loss of accuracy in the
        // case where we short circuit on the context excludes
        T include = moveIncludes(minimum);
        T contextInclude = moveContextIncludes(minimum);

        // make sure the minimum was not excluded
        T contextExclude = moveContextExcludes(minimum);
        if (contextExclude == null && !(includeHeads.containsKey(minimum) || contextIncludeHeads.containsKey(minimum))) {
            return null;
        }

        if (isContextRequired()) {
            // the minimum is not excluded, so return that
            // the includes or contextIncludes may lie at the proposed minimum
            next = minimum;
            document = Util.buildNewDocument(includeHeads, contextIncludeHeads, minimum);
            return minimum;
        }

        // take the lowest include or contextIncludes and return if it matches
        SortedSet<T> results = new TreeSet<>();
        if (include != null) {
            results.add(include);
        }
        if (contextInclude != null) {
            results.add(contextInclude);
        }

        if (!results.isEmpty()) {
            T lowest = results.first();
            document = Util.buildNewDocument(includeHeads, contextIncludeHeads, lowest);

            // either this is a normal union, so we can return the next highest element
            // or this union requires context, and we can only set next if it matched
            if (!isContextRequired() || lowest.equals(minimum)) {
                next = lowest;
            }
        }

        // If we got here then there's no match. Next is null.
        return next;
    }

    /**
     * Advance all includes that match the lowest key
     *
     * @return the new lowest element, or null if no such element exists
     */
    private T advanceIncludes() {
        if (includeHeads.keySet().isEmpty()) {
            return null;
        }
        T lowest = includeHeads.keySet().first();
        SortedSet<NestedIterator<T>> sources = includeHeads.removeAll(lowest);

        NestedIteratorUtil.advanceSources(includeHeads, sources, exhaustedIncludes, transformer);

        if (!includeHeads.isEmpty()) {
            return includeHeads.keySet().first();
        } else {
            return null;
        }
    }

    /**
     * Move all includes to the provided minimum context, taking into account any iterator with an expired context
     * <p>
     * Although include iterators do not explicitly have a context, there is an implicit context when a move is called.
     * <ol>
     * <li>Initialize sources if necessary</li>
     * <li>Move expired sources</li>
     * <li>Move include sources</li>
     * </ol>
     *
     * @param minimum
     *            the context
     * @return the lowest element after moving, or null if
     */
    private T moveIncludes(T minimum) {

        // 0. Check for short circuit
        if (includes.isEmpty() && includeHeads.isEmpty() && expiredIncludes.isEmpty() && exhaustedIncludes.isEmpty()) {
            return minimum;
        }

        // 1. Initialize sources if necessary
        if (!isIncludeInitialized()) {
            initializeIncludes(minimum);
        } else {
            // 2. Move expired sources if necessary
            moveExpiredIncludeSources(minimum);

            // 3. Move include sources
            moveIncludeSources(minimum);
        }

        // finally, calculate the lowest element to return, taking into account expired sources
        if (!includeHeads.isEmpty()) {
            T lowest = includeHeads.keySet().first();
            if (lowest.equals(minimum) || expiredIncludes.isEmpty()) {
                return lowest; // at least one include matched
            }
        }
        return null; // no match
    }

    private void moveExpiredIncludeSources(T minimum) {
        NestedIteratorUtil.moveExpiredSources(minimum, includeHeads, expiredIncludes, exhaustedIncludes, transformer);
    }

    private void moveIncludeSources(T minimum) {
        NestedIteratorUtil.moveSources(minimum, includeHeads, expiredIncludes, exhaustedIncludes, transformer);
    }

    /**
     * Moves all context includes to the provided minimum. Flow roughly follows this order
     * <ol>
     * <li>Initialize context include heads if necessary</li>
     * <li>Move expired iterators if necessary</li>
     * <li>Move context includes</li>
     * </ol>
     * Special note: any exhausted context exclude is an automatic match for the minimum. This check cannot be done first.
     *
     * @param minimum
     *            the minimum context
     * @return the lowest element after moving, or null if no match was possible
     */
    private T moveContextIncludes(T minimum) {

        // 1. Initialize context includes if necessary
        if (!isContextIncludeInitialized()) {
            initializeContextIncludeSources(minimum);
        } else {
            // 2. Move expired context includes if necessary
            moveExpiredContextIncludeSources(minimum);

            // 3. Move context includes
            moveContextIncludeSources(minimum);
        }

        // at least one include needs to match the minimum
        if (contextIncludeHeads.containsKey(minimum)) {
            return minimum;
        } else if (!contextIncludeHeads.isEmpty()) {
            return contextIncludeHeads.keySet().first();
        } else {
            return null;
        }
    }

    private boolean isContextIncludeInitialized() {
        return !(!contextIncludes.isEmpty() && contextIncludeHeads.isEmpty() && expiredContextIncludes.isEmpty() && exhaustedContextIncludes.isEmpty());
    }

    /**
     * Initialization of context include sources is a simple move
     *
     * @param minimum
     *            the move target
     */
    private void initializeContextIncludeSources(T minimum) {
        NestedIteratorUtil.initializeSources(minimum, contextIncludeHeads, contextIncludes, expiredContextIncludes, exhaustedContextIncludes, transformer);
    }

    /**
     * Move all expired context include iterators to the provided minimum
     *
     * @param minimum
     *            the minimum element to move to
     */
    private void moveExpiredContextIncludeSources(T minimum) {
        NestedIteratorUtil.moveExpiredSources(minimum, contextIncludeHeads, expiredContextIncludes, exhaustedContextIncludes, transformer);
    }

    /**
     * Move the context include sources to the provided minimum
     *
     * @param minimum
     *            the minimum element
     */
    private void moveContextIncludeSources(T minimum) {
        NestedIteratorUtil.moveSources(minimum, contextIncludeHeads, expiredContextIncludes, exhaustedContextIncludes, transformer);
    }

    /**
     * Move all context excludes to the provided minimum. The top element is returned and the parent 'move' method handles the negation piece.
     * <ol>
     * <li>Initialize context exclude heads if necessary</li>
     * <li>Move expired iterators if necessary</li>
     * <li>Move context excludes</li>
     * </ol>
     * Typical context exclude is <code>!(A &amp;&amp; B)</code>
     *
     * @param minimum
     *            the minimum context
     * @return the minimum if matched, otherwise null
     */
    private T moveContextExcludes(T minimum) {

        // 0. short circuit
        if (contextExcludes.isEmpty() && contextExcludeHeads.isEmpty() && expiredContextExcludes.isEmpty() && exhaustedContextExcludes.isEmpty()) {
            return minimum;
        } else if (!exhaustedContextExcludes.isEmpty()) {
            return minimum;
        }

        // 1. Initialize context exclude sources if necessary
        if (!isContextExcludeInitialized()) {
            initializeContextExcludeSources(minimum);
        } else if (!expiredContextExcludes.isEmpty()) {
            // 2. Move expired context exclude sources if necessary
            moveExpiredContextExcludeSources(minimum);

            // if any context excludes are still expired after moving, then the minimum was not matched via exclude
            if (!expiredContextExcludes.isEmpty()) {
                return null;
            }
        }

        // 3. Move context exclude
        moveContextExcludeSources(minimum);

        // test again for exhausted iterator
        if (!exhaustedContextExcludes.isEmpty()) {
            return minimum; // no match was possible
        }

        // discrepancy in how junctions are handled vs. leaf nodes
        // a leaf will always have a top element
        // a junction will never have a top element, unless it matches

        // junction = (!B && !C) == after move(x) with no match it's mapped as {x=AND}
        // leaves = {!B, !C} == after move(X) they are in the headmap as {Y=!B, Z=!C}

        // a context exclude for an OrIterator is something like (!B && !C)
        // matching via exclusion means the iterator will be present in the headmap

        // union matches if ANY non-leaf node matches the minimum
        // union matches if ANY leaf node does NOT match the minimum
        boolean anyLeafMisses = false;
        boolean junctionMatches = false;
        for (Map.Entry<T,NestedIterator<T>> entry : contextExcludeHeads.entries()) {
            if (entry.getKey().equals(minimum) && !Util.isLeaf(entry.getValue())) {
                junctionMatches = true;
                break;
                // don't care about leaves that match
            } else if (!entry.getKey().equals(minimum) && Util.isLeaf(entry.getValue())) {
                anyLeafMisses = true;
                break;
                // don't care about junctions that hit later
            }
        }

        if (anyLeafMisses || junctionMatches) {
            next = minimum;
            document = Util.buildNewDocument(includeHeads, contextIncludeHeads, minimum);
            return next;
        }

        return null;
    }

    private boolean isContextExcludeInitialized() {
        return !(!contextExcludes.isEmpty() && contextExcludeHeads.isEmpty() && expiredContextExcludes.isEmpty() && exhaustedContextExcludes.isEmpty());
    }

    private void initializeContextExcludeSources(T minimum) {
        NestedIteratorUtil.initializeSources(minimum, contextExcludeHeads, contextExcludes, expiredContextExcludes, exhaustedContextExcludes, transformer);
    }

    /**
     * Move any expired context exclude sources to the provided minimum
     *
     * @param minimum
     *            the minimum element
     */
    private void moveExpiredContextExcludeSources(T minimum) {
        NestedIteratorUtil.moveExpiredSources(minimum, contextExcludeHeads, expiredContextExcludes, exhaustedContextExcludes, transformer);
    }

    /**
     * Move context exclude sources to the provided minimum
     *
     * @param minimum
     *            the minimum element
     */
    private void moveContextExcludeSources(T minimum) {
        NestedIteratorUtil.moveSources(minimum, contextExcludeHeads, expiredContextExcludes, exhaustedContextExcludes, transformer);
    }

    public Collection<NestedIterator<T>> leaves() {
        LinkedList<NestedIterator<T>> leaves = new LinkedList<>();
        for (NestedIterator<T> itr : includes) {
            leaves.addAll(itr.leaves());
        }

        // these do not include contextIncludes/contextExcludes because they will be initialized on demand

        return leaves;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("This iterator does not support remove.");
    }

    public Document document() {
        return document;
    }

    @Override
    public Collection<NestedIterator<T>> children() {
        ArrayList<NestedIterator<T>> children = new ArrayList<>(includes.size() + contextIncludes.size() + contextExcludes.size());

        children.addAll(includes);
        children.addAll(contextIncludes);
        children.addAll(contextExcludes);

        return children;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("OrIterator: ");

        sb.append("Includes: ");
        sb.append(includes);
        sb.append(", Deferred Includes: ");
        sb.append(contextIncludes);
        sb.append(", Deferred Excludes: ");
        sb.append(contextExcludes);

        return sb.toString();
    }

    /**
     * If there are contextIncludes or contextExcludes this iterator requires context
     *
     * @return boolean
     */
    @Override
    public boolean isContextRequired() {
        return !contextExcludes.isEmpty() || !contextIncludes.isEmpty();
    }

    @Override
    public boolean isNonEventField() {
        return nonEventField;
    }

    /**
     * Distinct from a {@link Iterator#hasNext()} call, this method determines if a next element is possible.
     *
     * @return true if this iterator is exhausted
     */
    public boolean isIteratorExhausted() {
        return includeHeads.isEmpty() && contextIncludeHeads.isEmpty() && contextExcludeHeads.isEmpty() && expiredIncludes.isEmpty()
                        && expiredContextIncludes.isEmpty() && expiredContextExcludes.isEmpty();
    }

    // === include utilities ===
    private boolean isIncludeInitialized() {
        return !(!includes.isEmpty() && includeHeads.isEmpty() && expiredIncludes.isEmpty() && exhaustedIncludes.isEmpty());
    }

    private void initializeIncludes() {
        NestedIteratorUtil.initializeSources(includeHeads, includes, expiredIncludes, exhaustedIncludes, transformer);
    }

    private void initializeIncludes(T minimum) {
        NestedIteratorUtil.initializeSources(minimum, includeHeads, includes, expiredIncludes, exhaustedIncludes, transformer);
    }
}
