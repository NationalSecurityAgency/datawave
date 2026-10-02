package datawave.next;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.data.Key;
import org.junit.jupiter.api.Test;

import datawave.next.ScanResult.Polarity;

/**
 * Tests {@link ScanResult#and(ScanResult, ScanResult)}, {@link ScanResult#or(ScanResult, ScanResult)} and {@link ScanResult#negate(ScanResult)} directly,
 * against the full table of operand shapes: a positive bound, a negative bound and no bound at all, each exact or merely containing the truth.
 * <p>
 * A ScanResult bounds the documents a subtree can match. The operators must never narrow that bound below the truth, since a document dropped here is never
 * evaluated, so every case where precision is lost widens instead, ultimately to no bound at all.
 */
public class ScanResultAlgebraTest {

    private static final String ROW = "row";

    // a base that keeps integer values in sorted order
    private static final int BASE = 1_000_000;

    // +---------------------------------------+
    // | and: candidates satisfy both operands |
    // +---------------------------------------+

    @Test
    public void testAndOfTwoPositiveBounds() {
        assertInclude(ScanResult.and(include(1, 2, 3), include(2, 3, 4)), 2, 3);
    }

    @Test
    public void testAndOfPositiveAndNegativeBoundRemovesTheNegative() {
        assertInclude(ScanResult.and(include(1, 2, 3), exclude(2, 3, 4)), 1);
    }

    @Test
    public void testAndOfNegativeAndPositiveBoundIsOrderIndependent() {
        assertInclude(ScanResult.and(exclude(2, 3, 4), include(1, 2, 3)), 1);
    }

    @Test
    public void testAndOfTwoNegativeBoundsAccumulatesExclusions() {
        assertExclude(ScanResult.and(exclude(1, 2), exclude(3, 4)), 1, 2, 3, 4);
    }

    /**
     * An undecided operand narrows nothing, so the other operand still bounds the intersection, just no longer exactly.
     */
    @Test
    public void testAndWithNoBoundKeepsTheOtherOperand() {
        assertInclude(ScanResult.and(include(1, 2), null), 1, 2);
        assertInclude(ScanResult.and(null, include(1, 2)), 1, 2);
        assertExclude(ScanResult.and(exclude(1, 2), null), 1, 2);
    }

    @Test
    public void testAndOfTwoUndecidedOperandsIsUndecided() {
        assertNull(ScanResult.and(null, null));
    }

    // +--------------------------------------------+
    // | or: candidates satisfy at least one operand |
    // +--------------------------------------------+

    @Test
    public void testOrOfTwoPositiveBounds() {
        assertInclude(ScanResult.or(include(1, 2), include(3, 4)), 1, 2, 3, 4);
    }

    /**
     * De Morgan: {@code B || !C} is false exactly where {@code !B && C} holds, so only the documents C matches and B does not are excluded.
     */
    @Test
    public void testOrOfPositiveAndNegativeBoundShrinksTheExclusion() {
        assertExclude(ScanResult.or(include(3, 4), exclude(2, 3, 4)), 2);
    }

    @Test
    public void testOrOfNegativeAndPositiveBoundIsOrderIndependent() {
        assertExclude(ScanResult.or(exclude(2, 3, 4), include(3, 4)), 2);
    }

    /**
     * De Morgan: {@code !B || !C} excludes only what both operands exclude.
     */
    @Test
    public void testOrOfTwoNegativeBoundsKeepsTheCommonExclusion() {
        assertExclude(ScanResult.or(exclude(1, 2, 3), exclude(3, 4, 5)), 3);
    }

    /**
     * An undecided disjunct could be true for any document, so nothing bounds the union.
     */
    @Test
    public void testOrWithNoBoundIsUndecided() {
        assertNull(ScanResult.or(include(1, 2), null));
        assertNull(ScanResult.or(null, include(1, 2)));
        assertNull(ScanResult.or(exclude(1, 2), null));
        assertNull(ScanResult.or(null, null));
    }

    // +--------------------------+
    // | negate: flips the bound |
    // +--------------------------+

    @Test
    public void testNegateFlipsPolarity() {
        assertExclude(ScanResult.negate(include(1, 2)), 1, 2);
        assertInclude(ScanResult.negate(exclude(1, 2)), 1, 2);
        assertNull(ScanResult.negate(null));
    }

    @Test
    public void testNegateIsItsOwnInverse() {
        assertInclude(ScanResult.negate(ScanResult.negate(include(1, 2))), 1, 2);
    }

    /**
     * A bound that merely contains the match set cannot be complemented: the complement would be a subset of the truth, and would drop candidates.
     */
    @Test
    public void testNegateOfAnInexactBoundIsUndecided() {
        ScanResult inexact = ScanResult.and(include(1, 2), null);
        assertFalse(inexact.isExact());
        assertNull(ScanResult.negate(inexact));
    }

    // +------------------------------------------+
    // | exactness, which is what negation needs |
    // +------------------------------------------+

    @Test
    public void testExactnessSurvivesOnlyWhenBothOperandsAreExact() {
        assertTrue(ScanResult.and(include(1, 2), include(2, 3)).isExact());
        assertTrue(ScanResult.or(include(1, 2), include(2, 3)).isExact());

        ScanResult inexact = ScanResult.and(include(1, 2), null);
        assertFalse(ScanResult.and(inexact, include(2, 3)).isExact());
        assertFalse(ScanResult.or(inexact, include(2, 3)).isExact());
    }

    // +------------------------------------------------------------------+
    // | a partial scan holds only some of its matches, so it is a bound |
    // | from below, and only usable where removing fewer is safe |
    // +------------------------------------------------------------------+

    /**
     * A partial scan cannot narrow anything, so the whole operand survives. This is the existing behaviour of {@link ScanResult#intersect(ScanResult)}.
     */
    @Test
    public void testAndWithAPartialScanKeepsTheWholeOperand() {
        assertInclude(ScanResult.and(include(1, 2, 3), partial(2, 3)), 1, 2, 3);
        assertInclude(ScanResult.and(partial(2, 3), include(1, 2, 3)), 1, 2, 3);
    }

    @Test
    public void testAndOfTwoPartialScansIsUndecided() {
        assertNull(ScanResult.and(partial(1, 2), partial(2, 3)));
    }

    /**
     * A union removes documents only through its negative side, and a partial scan's missing hits would enlarge that removal, dropping candidates the query can
     * match. There is no safe bound to give.
     */
    @Test
    public void testOrOfAPartialScanAndANegativeBoundIsUndecided() {
        assertNull(ScanResult.or(partial(1, 2), exclude(3, 4)));
        assertNull(ScanResult.or(exclude(3, 4), partial(1, 2)));
    }

    @Test
    public void testOrOfAPartialScanAndAPositiveBoundStaysPartial() {
        ScanResult result = ScanResult.or(partial(1, 2), include(3, 4));
        assertNotNull(result);
        assertTrue(result.isTimeout());
        assertFalse(result.isExact());
    }

    /**
     * The one case where an inexact bound still negates: the keys a partial scan did find are known matches, so excluding just those removes fewer documents
     * than the query would, which is always safe.
     */
    @Test
    public void testNegateOfAPartialScanIsAnInexactExclusion() {
        ScanResult result = ScanResult.negate(partial(1, 2));
        assertExclude(result, 1, 2);
        assertFalse(result.isExact());
        assertFalse(result.isTimeout());
    }

    /**
     * Together with {@link #testNegateOfAPartialScanIsAnInexactExclusion()}: a negated partial scan still removes documents from an enclosing intersection,
     * which is the behaviour the intersection has always had for a partial negated term.
     */
    @Test
    public void testAndWithANegatedPartialScanStillRemoves() {
        assertInclude(ScanResult.and(include(1, 2, 3), ScanResult.negate(partial(2))), 1, 3);
    }

    // +------------------------+
    // | operands are untouched |
    // +------------------------+

    @Test
    public void testOperandsAreNotMutated() {
        ScanResult left = include(1, 2, 3);
        ScanResult right = include(2, 3, 4);

        ScanResult.and(left, right);
        ScanResult.or(left, right);
        ScanResult.negate(left);

        assertEquals(3, left.getResults().size());
        assertEquals(3, right.getResults().size());
        assertEquals(Polarity.INCLUDE, left.getPolarity());
    }

    /**
     * A derived bound must be able to say where its candidates are, since a later scan is restricted to that range.
     */
    @Test
    public void testDerivedBoundsCarryTheirKeyRange() {
        ScanResult result = ScanResult.and(include(1, 2, 3, 4), exclude(1, 4));
        assertResult(result.getMin(), 2);
        assertResult(result.getMax(), 3);
    }

    private ScanResult include(int... uids) {
        ScanResult result = new ScanResult();
        for (int uid : uids) {
            result.addKey(key(uid));
        }
        return result;
    }

    private ScanResult exclude(int... uids) {
        return ScanResult.negate(include(uids));
    }

    /**
     * A scan cut short by the time limit, holding only some of its matches
     *
     * @param uids
     *            the uids the scan did reach
     * @return a partial ScanResult
     */
    private ScanResult partial(int... uids) {
        ScanResult result = include(uids);
        result.setTimeout(true);
        return result;
    }

    private Key key(int uid) {
        return new Key(ROW, String.valueOf(BASE + uid));
    }

    private void assertInclude(ScanResult actual, int... uids) {
        assertNotNull(actual, "expected a positive bound");
        assertEquals(Polarity.INCLUDE, actual.getPolarity());
        assertUids(actual, uids);
    }

    private void assertExclude(ScanResult actual, int... uids) {
        assertNotNull(actual, "expected a negative bound");
        assertEquals(Polarity.EXCLUDE, actual.getPolarity());
        assertFalse(actual.isBounded(), "a negative bound does not name the candidates");
        assertUids(actual, uids);
    }

    private void assertUids(ScanResult actual, int... uids) {
        Set<Integer> expected = new TreeSet<>();
        for (int uid : uids) {
            expected.add(uid);
        }

        Set<Integer> found = new TreeSet<>();
        for (Key key : actual.getResults()) {
            found.add(Integer.parseInt(key.getColumnFamily().toString()) - BASE);
        }
        assertEquals(expected, found);
    }

    private void assertResult(Key key, int expected) {
        assertEquals(expected, Integer.parseInt(key.getColumnFamily().toString()) - BASE);
    }
}
