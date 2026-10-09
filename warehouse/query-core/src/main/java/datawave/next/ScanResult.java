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
 */
public class ScanResult {

    private static final Logger log = LoggerFactory.getLogger(ScanResult.class);

    private Key min;
    private Key max;
    private final Set<Key> results;

    public enum SOURCE {
        EQ, ER, RANGE, LIST
    }

    private SOURCE source;

    private boolean timeout = false;
    private boolean allowPartialIntersections = false;

    // True when every key in results is known to satisfy the expression represented by this result.
    private boolean subtractionSafe = true;

    // True when the result set contains every match in the scan context. The set may still contain conservative candidates.
    private boolean matchComplete = true;

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
            setTimeout(true);
        }
        this.subtractionSafe &= other.isSubtractionSafe();
        this.matchComplete &= other.isMatchComplete();
        this.source = null;
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

        if (!isMatchComplete()) {
            // An incomplete left side is not a safe universe for an intersection. A complete right side is: every real intersection match must occur there,
            // although the retained keys still require event-level confirmation against the incomplete expression.
            this.subtractionSafe = false;
            this.source = null;
            if (other.isMatchComplete()) {
                this.results.clear();
                updateMinMax();
                addKeys(other.getResults());
                this.matchComplete = true;
                this.timeout = false;
            } else {
                this.timeout |= other.isTimeout();
            }
            return;
        }

        Preconditions.checkArgument(!isTimeout(), "Left side of intersection should never be a timeout");

        if (other.isTimeout()) {

            // Retaining the left side, either in full or in part, preserves all possible intersection
            // matches but cannot prove that every retained key matched the partial right side.
            this.subtractionSafe = false;

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

        if (!other.isMatchComplete()) {
            // The right side is only a confirmed subset. Retaining it would discard candidates that may match an unevaluated branch, so keep the complete
            // left-side candidate universe and defer confirmation to event evaluation.
            this.subtractionSafe = false;
            return;
        }

        this.subtractionSafe &= other.isSubtractionSafe();

        if (!isIntersectionPossible(other)) {
            log.info("Intersection not possible, skipping");
            results.clear();
            return;
        }

        this.results.retainAll(other.getResults());
        updateMinMax();
    }

    /**
     * Removes only keys that are proven to match {@code other}. The result remains complete because unconfirmed candidates are retained. It is safe to use the
     * remaining keys as confirmed matches of the negation only when {@code other} was both subtraction-safe and complete.
     *
     * @param other
     *            the result to subtract
     */
    public void subtractConfirmedMatches(ScanResult other) {
        boolean exact = other.isSubtractionSafe() && other.isMatchComplete();
        if (other.isSubtractionSafe()) {
            results.removeAll(other.getResults());
            updateMinMax();
        }
        this.subtractionSafe &= exact;
        this.source = null;
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

    public void setTimeout(boolean timeout) {
        this.timeout = timeout;
        if (timeout) {
            this.matchComplete = false;
        }
    }

    public void setAllowPartialIntersections(boolean allowPartialIntersections) {
        this.allowPartialIntersections = allowPartialIntersections;
    }

    public boolean isTimeout() {
        return timeout;
    }

    /**
     * @return true when every returned key is a confirmed match and may safely be subtracted by a negating parent
     */
    public boolean isSubtractionSafe() {
        return subtractionSafe;
    }

    protected void setSubtractionSafe(boolean subtractionSafe) {
        this.subtractionSafe = subtractionSafe;
    }

    /**
     * @return true when the result contains every possible match in its scan context
     */
    public boolean isMatchComplete() {
        return matchComplete;
    }

    protected void setMatchComplete(boolean matchComplete) {
        this.matchComplete = matchComplete;
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
