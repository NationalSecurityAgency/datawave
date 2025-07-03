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
        this.allowPartialIntersections = allowPartialIntersections;
        this.results = new HashSet<>();
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

    private boolean isIntersectionPossible(ScanResult other) {
        boolean otherMinInBounds = withinBounds(other.getMin());
        boolean otherMaxInBounds = withinBounds(other.getMax());
        return otherMinInBounds || otherMaxInBounds;
    }

    private boolean isFullIntersection(ScanResult other) {
        boolean otherMinInBounds = withinBounds(other.getMin());
        boolean otherMaxInBounds = withinBounds(other.getMax());
        return otherMinInBounds && otherMaxInBounds;
    }

    private boolean withinBounds(Key key) {
        return min.compareTo(key) <= 0 && max.compareTo(key) >= 0;
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
