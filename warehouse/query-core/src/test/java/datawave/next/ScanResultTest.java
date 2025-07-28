package datawave.next;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.junit.jupiter.api.Test;

import datawave.next.ScanResult.SOURCE;

/**
 * Test for the {@link ScanResult} class.
 * <p>
 * Most on exercising the {@link ScanResult#union(ScanResult)} and {@link ScanResult#intersect(ScanResult)} methods.
 * <p>
 * Special attention is given to {@link ScanResult#partialIntersection(ScanResult)}.
 */
public class ScanResultTest {

    private static final String ROW = "row";

    // a base that allows integer values to appear in sorted order
    private final int base = 1_000_000;

    @Test
    public void testAddKeys() {
        ScanResult result = new ScanResult();
        Set<Key> keys = createKeys(5);
        result.addKeys(keys);
        assertEquals(5, result.getResults().size());
    }

    @Test
    public void testMinMax() {
        ScanResult result = new ScanResult();
        Set<Key> keys = createKeys(50);
        result.addKeys(keys);

        assertResult(result.getMin(), 1);
        assertResult(result.getMax(), 50);
    }

    @Test
    public void testUnion() {
        ScanResult left = createScanResult(1, 3);
        ScanResult right = createScanResult(2, 4);
        left.union(right);
        assertResults(Set.of(1, 2, 3, 4), left.getResults());
        assertFalse(left.isTimeout());
        assertFalse(right.isTimeout());
    }

    @Test
    public void testUnionOfPartialResult() {
        ScanResult left = createScanResult(1, 3);
        ScanResult right = createScanResult(2, 4);
        right.setTimeout(true);
        assertFalse(left.isTimeout());
        assertTrue(right.isTimeout());

        left.union(right);
        assertResults(Set.of(1, 2, 3, 4), left.getResults());
        assertTrue(left.isTimeout());
        assertTrue(right.isTimeout());
    }

    @Test
    public void testStandardIntersection_otherSubSet() {
        ScanResult left = createScanResult(1, 2, 3, 4, 5);
        ScanResult right = createScanResult(2, 3, 4);
        testStandardIntersection(left, right, Set.of(2, 3, 4));
    }

    @Test
    public void testStandardIntersection_otherSuperSetBefore() {
        ScanResult left = createScanResult(4, 5, 6);
        ScanResult right = createScanResult(2, 3, 4);
        testStandardIntersection(left, right, Set.of(4));
    }

    @Test
    public void testStandardIntersection_otherSuperSetAfter() {
        ScanResult left = createScanResult(4, 5, 6);
        ScanResult right = createScanResult(6, 7, 8);
        testStandardIntersection(left, right, Set.of(6));
    }

    @Test
    public void testStandardIntersection_otherDisjointSetBefore() {
        ScanResult left = createScanResult(4, 5, 6);
        ScanResult right = createScanResult(1, 2, 3);
        testStandardIntersection(left, right, 0);
    }

    @Test
    public void testStandardIntersection_otherDisjointSetAfter() {
        ScanResult left = createScanResult(4, 5, 6);
        ScanResult right = createScanResult(7, 8, 9);
        testStandardIntersection(left, right, 0);
    }

    // some partial intersections to illustrate the difference

    @Test
    public void testPartialIntersection_otherSubSet() {
        ScanResult left = createScanResult(1, 2, 3, 4, 5);
        ScanResult right = createScanResult(2, 4);
        // partial intersection should remove element 1 and 3
        testPartialIntersection(left, right, Set.of(2, 4, 5));
    }

    @Test
    public void testPartialIntersection_otherSuperSetBefore() {
        ScanResult left = createScanResult(4, 5, 6);
        ScanResult right = createScanResult(2, 3, 4);
        testPartialIntersection(left, right, Set.of(4, 5, 6));
    }

    @Test
    public void testPartialIntersection_otherSuperSetAfter() {
        ScanResult left = createScanResult(4, 5, 6);
        ScanResult right = createScanResult(5, 7, 9);
        // partial intersection should remove 4 and 6, functionally equivalent to a standard intersection
        testPartialIntersection(left, right, Set.of(5));
    }

    @Test
    public void testPartialIntersection_otherDisjointSetBefore() {
        ScanResult left = createScanResult(4, 5, 6);
        ScanResult right = createScanResult(1, 2, 3);
        // partial intersection *might* overlap with the left hand side, left remains intact because of the unknown
        testPartialIntersection(left, right, Set.of(4, 5, 6));
    }

    @Test
    public void testPartialIntersection_otherDisjointSetAfter() {
        ScanResult left = createScanResult(4, 5, 6);
        ScanResult right = createScanResult(7, 8, 9);
        testPartialIntersection(left, right, 0);
    }

    // test some bulk operations

    @Test
    public void testBulkPartialIntersection_otherSubSet() {
        ScanResult left = createScanResultRange(1, 90);
        ScanResult right = createScanResultRange(21, 30);
        // partial intersection eliminates [1 to 20]
        testPartialIntersection(left, right, 70);
    }

    @Test
    public void testBulkPartialIntersection_otherSuperSetBefore() {
        ScanResult left = createScanResultRange(21, 40);
        ScanResult right = createScanResultRange(11, 30);
        testPartialIntersection(left, right, 20);
    }

    @Test
    public void testBulkPartialIntersection_otherSuperSetAfter() {
        ScanResult left = createScanResultRange(21, 60);
        ScanResult right = createScanResultRange(51, 90);
        // partial intersection removes everything before the first key [21-50]
        testPartialIntersection(left, right, 10);
    }

    @Test
    public void testBulkPartialIntersection_otherDisjointSetBefore() {
        ScanResult left = createScanResultRange(41, 80);
        ScanResult right = createScanResultRange(11, 21);
        // partial intersection *might* overlap with the left hand side, left remains intact because of the unknown
        testPartialIntersection(left, right, 40);
    }

    @Test
    public void testBulkPartialIntersection_otherDisjointSetAfter() {
        ScanResult left = createScanResultRange(21, 40);
        ScanResult right = createScanResultRange(51, 60);
        testPartialIntersection(left, right, 0);
    }

    @Test
    public void testPartialIntersectionNotPossibleWithTimeoutRegex() {
        ScanResult left = createScanResult(3, 4);
        ScanResult right = createScanResult(1, 2);
        right.setTimeout(true);
        right.setSource(SOURCE.ER);

        left.intersect(right);

        // a regex timeout cannot be intersected
        assertResults(Set.of(3, 4), left.getResults());
        assertFalse(left.isTimeout());
        assertTrue(right.isTimeout());
    }

    @Test
    public void testPartialIntersectionNotPossibleWithTimeoutRange() {
        ScanResult left = createScanResult(3, 4);
        ScanResult right = createScanResult(1, 2);
        right.setTimeout(true);
        right.setSource(SOURCE.RANGE);

        left.intersect(right);

        // a bounded range timeout cannot be intersected
        assertResults(Set.of(3, 4), left.getResults());
        assertFalse(left.isTimeout());
        assertTrue(right.isTimeout());
    }

    @Test
    public void testPartialIntersectionNotPossibleWithTimeoutList() {
        ScanResult left = createScanResult(3, 4);
        ScanResult right = createScanResult(1, 2);
        right.setTimeout(true);
        right.setSource(SOURCE.LIST);

        left.intersect(right);

        // a list ivarator timeout cannot be intersected
        assertResults(Set.of(3, 4), left.getResults());
        assertFalse(left.isTimeout());
        assertTrue(right.isTimeout());
    }

    private void testStandardIntersection(ScanResult left, ScanResult right, int expected) {
        left.intersect(right);
        assertEquals(expected, left.getResults().size());
    }

    private void testStandardIntersection(ScanResult left, ScanResult right, Set<Integer> expected) {
        left.intersect(right);
        assertResults(expected, left.getResults());
    }

    private void testPartialIntersection(ScanResult left, ScanResult right, int expected) {
        setPartial(left, right);
        left.intersect(right);
        assertEquals(expected, left.getResults().size());
    }

    private void testPartialIntersection(ScanResult left, ScanResult right, Set<Integer> expected) {
        setPartial(left, right);
        left.intersect(right);
        assertResults(expected, left.getResults());
    }

    /**
     * Utility method that configures the ScanResult so it can be considered for a partial intersection
     *
     * @param left
     *            the left ScanResult
     * @param right
     *            the right ScanResult
     */
    private void setPartial(ScanResult left, ScanResult right) {
        left.setAllowPartialIntersections(true);
        right.setAllowPartialIntersections(true);
        right.setSource(SOURCE.EQ);
        right.setTimeout(true);
    }

    /**
     * Creates a ScanResult for the given indices
     *
     * @param indices
     *            a list of indices
     * @return a ScanResult
     */
    private ScanResult createScanResult(int... indices) {
        ScanResult result = new ScanResult();
        result.addKeys(createKeys(indices));
        return result;
    }

    /**
     * Creates a ScanResult for the given range
     *
     * @param start
     *            the range start
     * @param stop
     *            the range stop
     * @return a ScanResult
     */
    private ScanResult createScanResultRange(int start, int stop) {
        ScanResult result = new ScanResult();
        result.addKeys(createKeysForRange(start, stop));
        return result;
    }

    /**
     * Creates a number of keys from 1 to the upper bound
     *
     * @param num
     *            the upper bound
     * @return a set of Keys
     */
    private Set<Key> createKeys(int num) {
        Set<Key> keys = new HashSet<>();
        for (int i = 1; i <= num; i++) {
            keys.add(createKey(i));
        }
        return keys;
    }

    /**
     * Creates a number of keys, one for each index
     *
     * @param indices
     *            a list of indices
     * @return a set of Keys
     */
    private Set<Key> createKeys(int... indices) {
        Set<Key> keys = new HashSet<>();
        for (int i : indices) {
            keys.add(createKey(i));
        }
        return keys;
    }

    /**
     * Create a number of keys for the given range
     *
     * @param start
     *            the range start
     * @param end
     *            the range stop
     * @return a set of keys
     */
    private Set<Key> createKeysForRange(int start, int end) {
        Set<Key> keys = new HashSet<>();
        for (int i = start; i <= end; i++) {
            keys.add(createKey(i));
        }
        return keys;
    }

    /**
     * Create a key from the {@link #base} and the provided index
     *
     * @param index
     *            the index
     * @return a key
     */
    private Key createKey(int index) {
        return new Key(ROW, String.valueOf(base + index));
    }

    /**
     * Asserts that the key contains the provided index
     *
     * @param key
     *            the key
     * @param expected
     *            the expected index
     */
    private void assertResult(Key key, int expected) {
        int index = indexFromKey(key);
        assertEquals(expected, index);
    }

    /**
     * Assert that the results match the expectation, accounting for the base offset
     *
     * @param expected
     *            the set of expected indices
     * @param results
     *            the set of results
     */
    private void assertResults(Set<Integer> expected, Set<Key> results) {
        Set<Integer> resultIds = transformResults(results);
        assertEquals(expected, resultIds);
    }

    /**
     * Transform a set of keys into a set of integer indices
     *
     * @param results
     *            the set of results
     * @return a set of integers
     */
    private Set<Integer> transformResults(Set<Key> results) {
        Set<Integer> ids = new HashSet<>();
        for (Key key : results) {
            int index = indexFromKey(key);
            ids.add(index);
        }
        return ids;
    }

    /**
     * Extract the index from the provided key, extracting the base portion
     *
     * @param key
     *            the key
     * @return the index
     */
    private int indexFromKey(Key key) {
        String cf = key.getColumnFamily().toString();
        return Integer.parseInt(cf) - base;
    }
}
