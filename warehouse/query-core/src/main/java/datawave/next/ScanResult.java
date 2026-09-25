package datawave.next;

import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * This class allows us to retain knowledge of the min and max keys from a scan without requiring an expensive sorted set. Future scan ranges are restricted by
 * the min and max
 * <p>
 * A ScanResult is a bound on the documents a query subtree can match, not simply a set of hits. The {@link Polarity} says which side of the key set the
 * candidates lie on, and a null ScanResult means no bound at all. Compose them with {@link #and(ScanResult, ScanResult)}, {@link #or(ScanResult, ScanResult)}
 * and {@link #negate(ScanResult)}, which are null tolerant and always widen rather than narrow when precision is lost, so a bound never excludes a document the
 * query can match.
 */
public class ScanResult {

    private static final Logger log = LoggerFactory.getLogger(ScanResult.class);

    /** Which side of the key set the candidate documents lie on */
    public enum Polarity {
        /** the candidates are the keys in this result */
        INCLUDE,
        /** the candidates are every document except the keys in this result, so on its own this result bounds nothing */
        EXCLUDE
    }

    private Key min;
    private Key max;
    private final Set<Key> results;

    public enum SOURCE {
        EQ, ER, RANGE, LIST
    }

    private SOURCE source;

    private Polarity polarity = Polarity.INCLUDE;

    /**
     * Whether the key set is the subtree's true match set, rather than merely containing it. Only an exact result can be negated, since complementing a set
     * that is merely a superset yields a subset of the truth, which would drop candidates.
     */
    private boolean exact = true;

    private boolean timeout = false;
    private boolean allowPartialIntersections = false;

    /**
     * Default constructor does not allow partial intersections
     */
    public ScanResult() {
        this(false);
    }

    /**
     * Constructor accepts a boolean to enable partial intersections
     *
     * @param allowPartialIntersections
     *            flag to enable partial intersection
     */
    public ScanResult(boolean allowPartialIntersections) {
        this.results = new HashSet<>();
        this.setAllowPartialIntersections(allowPartialIntersections);
    }

    /**
     * Bounds the documents matching an intersection of two subtrees.
     * <p>
     * A null operand contributes no bound, leaving the other operand as a superset of the truth.
     *
     * @param left
     *            one operand, possibly null
     * @param right
     *            the other operand, possibly null
     * @return the bound on the intersection, or null when neither operand bounds anything
     */
    public static ScanResult and(ScanResult left, ScanResult right) {
        if (left == null) {
            return loosen(right);
        }
        if (right == null) {
            return loosen(left);
        }

        if (left.timeout || right.timeout) {
            return andPartial(left, right);
        }

        boolean exact = left.exact && right.exact;

        if (left.polarity == Polarity.INCLUDE && right.polarity == Polarity.INCLUDE) {
            ScanResult result = copy(left);
            result.intersect(right);
            return bound(result, Polarity.INCLUDE, exact);
        }

        if (left.polarity == Polarity.INCLUDE) {
            return difference(left, right, exact);
        }

        if (right.polarity == Polarity.INCLUDE) {
            return difference(right, left, exact);
        }

        // neither side can match what the other excludes, so the exclusions accumulate
        ScanResult result = copy(left);
        result.addKeys(right.getResults());
        return bound(result, Polarity.EXCLUDE, exact);
    }

    /**
     * Bounds the documents matching a union of two subtrees.
     * <p>
     * A null operand could be true for any document, so the union bounds nothing.
     *
     * @param left
     *            one operand, possibly null
     * @param right
     *            the other operand, possibly null
     * @return the bound on the union, or null when either operand bounds nothing
     */
    public static ScanResult or(ScanResult left, ScanResult right) {
        if (left == null || right == null) {
            return null;
        }

        if (left.timeout || right.timeout) {
            return orPartial(left, right);
        }

        boolean exact = left.exact && right.exact;

        if (left.polarity == Polarity.INCLUDE && right.polarity == Polarity.INCLUDE) {
            ScanResult result = copy(left);
            result.union(right);
            return bound(result, Polarity.INCLUDE, exact);
        }

        if (left.polarity == Polarity.EXCLUDE && right.polarity == Polarity.EXCLUDE) {
            // only a document both sides exclude is excluded by the union
            ScanResult result = copy(left);
            result.results.retainAll(right.getResults());
            result.updateMinMax();
            return bound(result, Polarity.EXCLUDE, exact);
        }

        // a document the positive side matches is no longer excluded by the union
        ScanResult include = left.polarity == Polarity.INCLUDE ? left : right;
        ScanResult exclude = left.polarity == Polarity.INCLUDE ? right : left;
        ScanResult result = copy(exclude);
        result.results.removeAll(include.getResults());
        result.updateMinMax();
        return bound(result, Polarity.EXCLUDE, exact);
    }

    /**
     * Bounds the documents matching the negation of a subtree.
     * <p>
     * Negation needs the operand's match set from below, so a result that merely contains the match set cannot be negated at all. A partial scan is the one
     * case where an inexact result still negates: its keys are known matches, and removing fewer documents than the query would is always safe.
     *
     * @param value
     *            the operand, possibly null
     * @return the bound on the negation, or null when the operand cannot be complemented
     */
    public static ScanResult negate(ScanResult value) {
        if (value == null) {
            return null;
        }

        if (value.timeout) {
            ScanResult result = copy(value);
            result.timeout = false;
            return bound(result, opposite(value.polarity), false);
        }

        if (!value.exact) {
            return null;
        }

        return bound(copy(value), opposite(value.polarity), true);
    }

    /**
     * Intersects when at least one operand is a partial scan. A partial scan holds only some of its matches, so it bounds nothing on its own, but two positive
     * operands can still go through {@link #intersect(ScanResult)}, which knows a partial scan is complete within its own key range.
     *
     * @param left
     *            one operand
     * @param right
     *            the other operand
     * @return the bound on the intersection, or null
     */
    private static ScanResult andPartial(ScanResult left, ScanResult right) {
        if (left.polarity == Polarity.INCLUDE && right.polarity == Polarity.INCLUDE) {
            // intersect requires a whole set on the left
            ScanResult whole = left.timeout ? right : left;
            ScanResult partial = left.timeout ? left : right;
            if (!whole.timeout) {
                ScanResult result = copy(whole);
                result.intersect(partial);
                return bound(result, Polarity.INCLUDE, false);
            }
        }

        // anywhere else a partial scan is no bound at all
        return and(left.timeout ? null : left, right.timeout ? null : right);
    }

    /**
     * Unions when at least one operand is a partial scan. Two positive operands stay a partial union, but a partial scan cannot bound a union that also has to
     * exclude: the missing hits would enlarge the exclusion and drop candidates the query can match.
     *
     * @param left
     *            one operand
     * @param right
     *            the other operand
     * @return the bound on the union, or null
     */
    private static ScanResult orPartial(ScanResult left, ScanResult right) {
        if (left.polarity == Polarity.INCLUDE && right.polarity == Polarity.INCLUDE) {
            ScanResult result = copy(left);
            result.union(right);
            return bound(result, Polarity.INCLUDE, false);
        }
        return null;
    }

    /**
     * Removes the excluded keys from the included keys
     *
     * @param include
     *            the positive operand
     * @param exclude
     *            the negative operand
     * @param exact
     *            whether the difference is the true match set
     * @return a positive ScanResult
     */
    private static ScanResult difference(ScanResult include, ScanResult exclude, boolean exact) {
        ScanResult result = copy(include);
        result.results.removeAll(exclude.getResults());
        result.updateMinMax();
        return bound(result, Polarity.INCLUDE, exact);
    }

    /**
     * Weakens a result to a superset of the truth, for when it is combined with something this visitor could not decide
     *
     * @param source
     *            the result to weaken, possibly null
     * @return a weakened copy, or null if there is nothing left to bound
     */
    private static ScanResult loosen(ScanResult source) {
        if (source == null || source.timeout) {
            // a partial scan holds only some of its matches, so on its own it bounds nothing
            return null;
        }
        return bound(copy(source), source.polarity, false);
    }

    private static ScanResult bound(ScanResult result, Polarity polarity, boolean exact) {
        result.polarity = polarity;
        result.exact = exact;
        return result;
    }

    private static Polarity opposite(Polarity polarity) {
        return polarity == Polarity.INCLUDE ? Polarity.EXCLUDE : Polarity.INCLUDE;
    }

    /**
     * Copies a ScanResult's keys and state into a new, independent instance, so that composing results never mutates an operand.
     * <p>
     * The source term is deliberately not carried over: it is only read to decide whether a timed out scan still permits a partial intersection, which a set
     * derived from several terms cannot answer.
     *
     * @param source
     *            the ScanResult to copy
     * @return a new ScanResult with the same keys
     */
    private static ScanResult copy(ScanResult source) {
        ScanResult result = new ScanResult(source.allowPartialIntersections);
        result.addKeys(source.getResults());
        result.timeout = source.timeout;
        result.polarity = source.polarity;
        result.exact = source.exact;
        return result;
    }

    /**
     * Performs a union with another scan result
     * <p>
     * If either scan result is partial (i.e., timed out) then the result is considered partial
     *
     * @param other
     *            another ScanResult
     */
    public void union(ScanResult other) {
        if (other.isTimeout()) {
            // a union can have one or both sides timeout, the entire union
            // is considered a timeout
            this.timeout = true;
        }
        addKeys(other.getResults());
    }

    /**
     * Performs an intersection with another scan result
     * <p>
     * If the other scan result is partial (i.e., timed out) then a partial intersection is performed if sorted order is guaranteed as in the case of an
     * equality term.
     *
     * @param other
     *            another ScanResult
     */
    public void intersect(ScanResult other) {

        Preconditions.checkArgument(!isTimeout(), "Left side of intersection should never be a timeout");

        if (other.isTimeout()) {

            if (!allowPartialIntersections) {
                return;
            }

            if (other.getSource() != SOURCE.EQ) {
                // regex, range, list timed out. no partial intersection possible
                log.trace("Cannot perform partial intersection with {}", other.getSource());
                return;
            }

            partialIntersection(other);
            return;
        }

        if (!isIntersectionPossible(other)) {
            log.info("Intersection not possible, skipping");
            results.clear();
            return;
        }

        this.results.retainAll(other.getResults());
        updateMinMax();
    }

    /**
     * Attempt a partial intersection with an external set.
     *
     * @param other
     *            the external set
     */
    protected void partialIntersection(ScanResult other) {

        // special case where the external set sorts entirely before this one
        boolean externalFullyBefore = other.getMax().compareTo(this.min) < 0;
        if (externalFullyBefore) {
            log.info("Partial external set sorts before first key, no intersection will be done");
            return;
        }

        boolean externalFullyAfter = other.getMin().compareTo(this.max) > 0;
        if (externalFullyAfter) {
            results.clear();
            return;
        }

        int originalSize = results.size();
        results.removeIf(result -> nominate(result, other.getMin(), other.getMax(), other.getResults()));

        int newSize = results.size();
        int delta = originalSize - newSize;
        log.info("Partial intersection eliminated {} candidates", delta);
    }

    /**
     * A candidate is nominated for removal if it lies before the start of the external set, or if the candidate is within the bounds of the external set but
     * not found in the external set.
     * <p>
     * A candidate is NOT nominated for removal if it sorts after the end of the external set.
     *
     * @param candidate
     *            the candidate
     * @param min
     *            the minimum value of the external set
     * @param max
     *            the maximum value of the external set
     * @param otherResults
     *            the external set
     * @return true if the candidate should be removed
     */
    private boolean nominate(Key candidate, Key min, Key max, Set<Key> otherResults) {
        return candidate.compareTo(min) < 0 || (candidate.compareTo(max) <= 0 && !otherResults.contains(candidate));
    }

    protected boolean isIntersectionPossible(ScanResult other) {
        if (results.isEmpty() || other.getResults().isEmpty()) {
            return false;
        }

        if (results.size() == 1 && other.results.size() != 1) {
            // in the case where ScanResult is a singleton and the other is not we must call isIntersectionPossible using the other ScanResult
            return other.isIntersectionPossible(this);
        }

        boolean otherMinInBounds = withinBounds(other.getMin());
        boolean otherMaxInBounds = withinBounds(other.getMax());
        return otherMinInBounds || otherMaxInBounds || this.isContainedBy(other);
    }

    private boolean withinBounds(Key key) {
        return min.compareTo(key) <= 0 && max.compareTo(key) >= 0;
    }

    /**
     * Does this ScanResult fall entirely within the other?
     *
     * @param other
     *            another ScanResult
     * @return true if this is fully contained
     */
    private boolean isContainedBy(ScanResult other) {
        return min.compareTo(other.getMin()) >= 0 && max.compareTo(other.getMax()) <= 0;
    }

    /**
     * Bulk add method
     *
     * @param keys
     *            the set of keys to add
     */
    public void addKeys(Set<Key> keys) {
        for (Key key : keys) {
            addKey(key);
        }
    }

    /**
     * Adds a key to the result set, checking the key against the existing min or max value.
     */
    public void addKey(Key key) {
        if (min == null) {
            min = key;
        } else if (key.compareTo(min) < 0) {
            min = key;
        }

        if (max == null) {
            max = key;
        } else if (key.compareTo(max) > 0) {
            max = key;
        }

        results.add(key);
    }

    /**
     * Iterate through the collection of results and
     */
    protected void updateMinMax() {
        switch (results.size()) {
            case 0:
                min = null;
                max = null;
                break;
            case 1:
                Key key = results.iterator().next();
                min = key;
                max = key;
                break;
            default:
                Key localMin = null;
                Key localMax = null;
                for (Key result : results) {
                    if (localMin == null || result.compareTo(localMin) <= 0) {
                        localMin = result;
                    }
                    if (localMax == null || result.compareTo(localMax) >= 0) {
                        localMax = result;
                    }
                }
                this.min = localMin;
                this.max = localMax;
        }
    }

    /**
     * Update the source given an iterator
     *
     * @param iter
     *            an implementation of a {@link BaseDocIdIterator}
     */
    public void updateSource(BaseDocIdIterator iter) {
        if (iter instanceof RegexDocIdIterator) {
            source = SOURCE.ER;
        } else if (iter instanceof RangeDocIdIterator) {
            source = SOURCE.RANGE;
        } else if (iter instanceof ListDocIdIterator) {
            source = SOURCE.LIST;
        } else if (iter instanceof DocIdIterator) {
            source = SOURCE.EQ;
        }
    }

    public SOURCE getSource() {
        return source;
    }

    protected void setSource(SOURCE source) {
        this.source = source;
    }

    /**
     * Marks this result as a partial scan. A partial scan holds only some of its matches, which is never the true match set, so this also clears
     * {@link #isExact()}.
     *
     * @param timeout
     *            whether the scan was cut short
     */
    public void setTimeout(boolean timeout) {
        this.timeout = timeout;
        if (timeout) {
            this.exact = false;
        }
    }

    public Polarity getPolarity() {
        return polarity;
    }

    /**
     * Whether these keys are the documents the query matches rather than a superset of them. An inexact result is safe to generate candidates from, since
     * evaluating each document discards the extras, but must not be counted.
     * <p>
     * Exactness is relative to the context the scan was given. A result scanned inside another result's key range is exact within that range, which is the only
     * place its enclosing intersection uses it.
     *
     * @return true if the keys are exactly the documents the query matches
     */
    public boolean isExact() {
        return exact;
    }

    /**
     * Whether this result names the candidate documents. A negative result names documents that cannot match, leaving the candidates unbounded.
     *
     * @return true if the keys are the candidates
     */
    public boolean isBounded() {
        return polarity == Polarity.INCLUDE;
    }

    public void setAllowPartialIntersections(boolean allowPartialIntersections) {
        this.allowPartialIntersections = allowPartialIntersections;
    }

    public boolean isTimeout() {
        return timeout;
    }

    public Key getMin() {
        return min;
    }

    public Key getMax() {
        return max;
    }

    public Set<Key> getResults() {
        return results;
    }
}
