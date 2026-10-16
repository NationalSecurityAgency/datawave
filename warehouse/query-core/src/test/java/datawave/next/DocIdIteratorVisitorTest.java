package datawave.next;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.query.exceptions.DatawaveFatalQueryException;

public class DocIdIteratorVisitorTest extends FieldIndexDataTestUtil {

    /** more hits than the visitor's result check interval, so a scan with a time limit reports a partial result */
    private static final int HITS_PAST_TIMEOUT_CHECK = 600;

    private String query;
    private final Range range = new Range(row);

    private final Set<String> indexedFields = Set.of("FIELD_A", "FIELD_B", "FIELD_C", "FIELD_D");

    @BeforeEach
    public void setup() {
        query = null;
        data.clear();
        datatypes.clear();
    }

    @Test
    public void testSingleEQ() {
        writeData("FIELD_A", "value-a", 10);
        withQuery("FIELD_A == 'value-a'");
        drive();
        assertResultSize(10);
    }

    @Test
    public void testUnion() {
        writeData("FIELD_A", "value-a", 10);
        writeData("FIELD_B", "value-b", 15);
        withQuery("FIELD_A == 'value-a' || FIELD_B == 'value-b'");
        drive();
        assertResultSize(15);
    }

    @Test
    public void testIntersection() {
        writeData("FIELD_A", "value-a", 10);
        writeData("FIELD_B", "value-b", 15);
        withQuery("FIELD_A == 'value-a' && FIELD_B == 'value-b'");
        drive();
        assertResultSize(10);
    }

    @Test
    public void testNestedIntersection() {
        writeData("FIELD_A", "value-a", 5);
        writeData("FIELD_B", "value-b", 15);
        writeData("FIELD_C", "value-c", 20);
        withQuery("FIELD_A == 'value-a' || (FIELD_B == 'value-b' && FIELD_C == 'value-c')");
        drive();
        assertResultSize(15);
    }

    @Test
    public void testNestedUnion() {
        writeData("FIELD_A", "value-a", 5);
        writeData("FIELD_B", "value-b", 15);
        writeData("FIELD_C", "value-c", 20);
        withQuery("FIELD_A == 'value-a' && (FIELD_B == 'value-b' || FIELD_C == 'value-c')");
        drive();
        assertResultSize(5);
    }

    @Test
    public void testNestedUnionOneTermNoHits() {
        writeData("FIELD_A", "value-a", 5);
        writeData("FIELD_B", "value-b", 15);
        writeData("FIELD_C", "value-c", 20);
        withQuery("FIELD_A == 'value-a' && (FIELD_Z == 'value-z' || FIELD_C == 'value-c')");
        drive();
        assertResultSize(5);
    }

    @Test
    public void testNestedUnionWithExtraParens() {
        writeData("FIELD_A", "value-a", 5);
        writeData("FIELD_B", "value-b", 15);
        writeData("FIELD_C", "value-c", 20);
        withQuery("FIELD_A == 'value-a' && ((FIELD_B == 'value-b' || FIELD_C == 'value-c'))");
        drive();
        assertResultSize(5);
    }

    @Test
    public void testRegexIntersection() {
        writeData("FIELD_A", "value-a", 10);
        writeData("FIELD_B", "value-b", 15);
        withQuery("FIELD_A == 'value-a' && FIELD_B =~ 'val.*'");
        drive();
        assertResultSize(10);
    }

    @Test
    public void testRegexUnion() {
        writeData("FIELD_A", "value-a", 10);
        writeData("FIELD_B", "value-b", 15);
        withQuery("FIELD_A == 'value-a' || FIELD_B =~ 'val.*'");
        drive();
        assertResultSize(15);
    }

    @Test
    public void testRegexIntersectionMatchesSomeDatatypes() {
        writeData("FIELD_A", "value-a", 10);
        writeData("FIELD_B", "value-b", "datatype-a", 15);
        writeData("FIELD_B", "value-b", "datatype-b", 17);
        writeData("FIELD_B", "value-b", "datatype-c", 19);
        withQuery("FIELD_A == 'value-a' && FIELD_B =~ 'val.*'");
        withDataTypes("datatype-a", "datatype-c");
        drive();
        assertResultSize(10);
    }

    @Test
    public void testRegexUnionMatchesSomeDatatypes() {
        writeData("FIELD_A", "value-a", 10);
        writeData("FIELD_B", "value-b", "datatype-a", 15);
        writeData("FIELD_B", "value-b", "datatype-b", 17);
        writeData("FIELD_B", "value-b", "datatype-c", 19);
        withQuery("FIELD_A == 'value-a' || FIELD_B =~ 'val.*'");
        withDataTypes("datatype-a", "datatype-c");
        drive();
        assertResultSize(34);
    }

    @Test
    public void testAnchorWithNegatedUnion() {
        // model-expanded anchor: (FIELD_A || FIELD_B), model-expanded negation: !(FIELD_C || FIELD_C)
        writeData("FIELD_A", "value-a", 10);
        writeIndex("FIELD_C", "value-c", "datatype-a", 3);
        withQuery("(FIELD_A == 'value-a' || FIELD_B == 'value-b') && !(FIELD_C == 'value-c' || FIELD_C == 'value-c2')");
        drive();
        assertResultSize(9);
    }

    /**
     * Regression test: a negated union whose children are all non-executable (e.g. no synonym field is indexed) previously wiped the entire intersection result
     * instead of leaving it unrestricted.
     */
    @Test
    public void testAnchorWithFullyNonExecutableNegatedUnion() {
        writeData("FIELD_A", "value-a", 10);
        // NON_INDEXED_1 / NON_INDEXED_2 are deliberately absent from indexedFields: this simulates a model-expanded
        // negated field where none of the synonym sub-fields are indexed
        withQuery("FIELD_A == 'value-a' && !(NON_INDEXED_1 == 'value-b' || NON_INDEXED_2 == 'value-c')");
        drive();
        assertResultSize(10);
    }

    /**
     * A union of fully negated terms, e.g. {@code !B || !C}, is equivalent to {@code !(B && C)} and removes candidates matching every de-negated term.
     */
    @Test
    public void testAnchorWithUnionOfFullyNegatedTermsDefeatsCandidates() {
        writeRange("FIELD_A", "value-a", 1, 10);
        writeRange("FIELD_B", "value-b", 3, 5);
        writeRange("FIELD_C", "value-c", 4, 6);

        withQuery("FIELD_A == 'value-a' && (!(FIELD_B == 'value-b') || !(FIELD_C == 'value-c'))");
        drive();
        assertResultSize(8);
    }

    /**
     * Companion to {@link #testAnchorWithUnionOfFullyNegatedTermsDefeatsCandidates()}: no document matches every de-negated term, so nothing is removed.
     */
    @Test
    public void testAnchorWithUnionOfFullyNegatedTermsNoOverlapRemovesNothing() {
        writeRange("FIELD_A", "value-a", 1, 10);
        writeRange("FIELD_B", "value-b", 1, 3);
        writeRange("FIELD_C", "value-c", 8, 10);

        withQuery("FIELD_A == 'value-a' && (!(FIELD_B == 'value-b') || !(FIELD_C == 'value-c'))");
        drive();
        assertResultSize(10);
    }

    /**
     * A non-executable de-negated term (e.g. a non-indexed field) leaves its disjunct undecided, so the union could be true for any candidate and the candidate
     * set is left unrestricted.
     */
    @Test
    public void testAnchorWithUnionOfFullyNegatedTermsOneNonExecutable() {
        writeRange("FIELD_A", "value-a", 1, 10);
        writeRange("FIELD_B", "value-b", 3, 5);
        // NON_INDEXED is deliberately absent from indexedFields

        withQuery("FIELD_A == 'value-a' && (!(FIELD_B == 'value-b') || !(NON_INDEXED == 'value-z'))");
        drive();
        assertResultSize(10);
    }

    /**
     * A union mixing a positive and a negated term, e.g. {@code B || !C}, is false exactly when {@code !B && C} holds, and removes those candidates.
     */
    @Test
    public void testAnchorWithMixedUnionDefeatsCandidates() {
        writeRange("FIELD_A", "value-a", 1, 10);
        writeRange("FIELD_B", "value-b", 1, 5);
        writeRange("FIELD_C", "value-c", 4, 8);

        withQuery("FIELD_A == 'value-a' && (FIELD_B == 'value-b' || !(FIELD_C == 'value-c'))");
        drive();
        assertResultSize(7);
    }

    /**
     * Companion to {@link #testAnchorWithMixedUnionDefeatsCandidates()}: FIELD_C only matches uids already covered by FIELD_B, so nothing is removed.
     */
    @Test
    public void testAnchorWithMixedUnionNoOverlapRemovesNothing() {
        writeRange("FIELD_A", "value-a", 1, 10);
        writeRange("FIELD_B", "value-b", 1, 5);
        writeRange("FIELD_C", "value-c", 1, 3);

        withQuery("FIELD_A == 'value-a' && (FIELD_B == 'value-b' || !(FIELD_C == 'value-c'))");
        drive();
        assertResultSize(10);
    }

    /**
     * When the positive disjunct's union already covers every candidate, {@code !B} is never true, so the union is trivially true everywhere.
     */
    @Test
    public void testAnchorWithMixedUnionPositiveCoversEverythingRemovesNothing() {
        writeRange("FIELD_A", "value-a", 1, 10);
        writeRange("FIELD_B", "value-b", 1, 10);
        writeRange("FIELD_C", "value-c", 4, 6);

        withQuery("FIELD_A == 'value-a' && (FIELD_B == 'value-b' || !(FIELD_C == 'value-c'))");
        drive();
        assertResultSize(10);
    }

    /**
     * A non-executable positive disjunct is undecided rather than false, so the union could be true for any candidate and the candidate set is left
     * unrestricted. Removing the candidates matching FIELD_C would drop any document that satisfies only the disjunct the field index cannot see.
     */
    @Test
    public void testAnchorWithMixedUnionNonExecutablePositiveTerm() {
        writeRange("FIELD_A", "value-a", 1, 10);
        writeRange("FIELD_C", "value-c", 4, 6);
        // NON_INDEXED is deliberately absent from indexedFields

        withQuery("FIELD_A == 'value-a' && (NON_INDEXED == 'value-z' || !(FIELD_C == 'value-c'))");
        drive();
        assertResultSize(10);
    }

    /**
     * A positive disjunct whose scan times out leaves the union undecided. Its missing hits would otherwise enlarge the complement, removing candidates the
     * query can match: here FIELD_B covers every candidate, so nothing should be removed at all.
     */
    @Test
    public void testAnchorWithMixedUnionPositiveTermTimesOut() {
        writeRange("FIELD_A", "value-a", 1, HITS_PAST_TIMEOUT_CHECK);
        writeRange("FIELD_B", "value-b", 1, HITS_PAST_TIMEOUT_CHECK);
        writeRange("FIELD_C", "value-c", 550, 560);

        withQuery("FIELD_A == 'value-a' && (FIELD_B == 'value-b' || !(FIELD_C == 'value-c'))");
        driveWithScanTimeout(0L);
        assertResultSize(HITS_PAST_TIMEOUT_CHECK);
    }

    /**
     * A de-negated disjunct whose scan times out leaves the union undecided. A partial scan cannot narrow the complement -- {@link ScanResult#intersect} skips
     * the intersection entirely -- so the complement stays too large and the remaining term removes candidates that match no disjunct's false condition.
     */
    @Test
    public void testAnchorWithUnionOfFullyNegatedTermsFirstTermTimesOut() {
        writeRange("FIELD_A", "value-a", 1, HITS_PAST_TIMEOUT_CHECK);
        writeRange("FIELD_C", "value-c", 1, 520);
        writeRange("FIELD_B", "value-b", 550, 560);

        // no candidate matches both FIELD_C and FIELD_B, so no candidate has every disjunct false
        withQuery("FIELD_A == 'value-a' && (!(FIELD_C == 'value-c') || !(FIELD_B == 'value-b'))");
        driveWithScanTimeout(0L);
        assertResultSize(HITS_PAST_TIMEOUT_CHECK);
    }

    /**
     * A non-executable negated disjunct is presumed always true, so the union is trivially true for every candidate regardless of the other disjunct.
     */
    @Test
    public void testAnchorWithMixedUnionNonExecutableNegatedTerm() {
        writeRange("FIELD_A", "value-a", 1, 10);
        writeRange("FIELD_B", "value-b", 1, 5);
        // NON_INDEXED is deliberately absent from indexedFields

        withQuery("FIELD_A == 'value-a' && (FIELD_B == 'value-b' || !(NON_INDEXED == 'value-z'))");
        drive();
        assertResultSize(10);
    }

    @Test
    public void testValueMarker() {
        writeData("FIELD_A", "abc", "datatype-a", 2);
        writeData("FIELD_A", "abd", "datatype-b", 3);
        writeData("FIELD_A", "abe", "datatype-c", 5);
        withQuery("((_Value_ = true) && (FIELD_A =~ 'ab.*'))");
        drive();
        assertResultSize(10);
    }

    @Test
    public void testBoundedRangeMarker() {
        writeIndex("FIELD_A", "1", "datatype-a", 1);
        writeIndex("FIELD_A", "2", "datatype-a", 2);
        writeIndex("FIELD_A", "3", "datatype-a", 3);
        writeIndex("FIELD_A", "4", "datatype-a", 4);
        writeIndex("FIELD_A", "5", "datatype-a", 5);
        withQuery("((_Bounded_ = true) && (FIELD_A >= '1' && FIELD_A <= '2'))");
        drive();
        assertResultSize(2);
    }

    @Test
    public void testDoubleMarker() {
        // a bounded range that fails to expand against the global index is marked as value exceeded
        writeIndex("FIELD_A", "1", "datatype-a", 1);
        writeIndex("FIELD_A", "2", "datatype-a", 2);
        writeIndex("FIELD_A", "3", "datatype-a", 3);
        writeIndex("FIELD_A", "4", "datatype-a", 4);
        writeIndex("FIELD_A", "5", "datatype-a", 5);
        withQuery("((_Value = true) && ((_Bounded_ = true) && (FIELD_A >= '1' && FIELD_A <= '2')))");
        drive();
        assertResultSize(2);
    }

    @Test
    public void testAndNonIndexedField() {
        writeData("FIELD_A", "value-a", 10);
        withQuery("FIELD_A == 'value-a' && NON_INDEXED == 'value-zz'");
        drive();
        assertResultSize(10);
    }

    /**
     * Any range that fails to expand should gain a value exceeded marker.
     * <p>
     * However, non-indexed fields will not be marked. Additionally, range expansion could be disabled in the query planner and the user could submit a Jexl
     * query with a correctly formed bounded range.
     */
    @Test
    public void testNonIndexedBoundedRangeAndAnchorTerm() {
        writeData("FIELD_A", "value-a", 10);
        withQuery("FIELD_A == 'value-a' && ((_Bounded_ = true) && (NON_INDEXED >= 'a' && NON_INDEXED <= 'z'))");
        drive();
        assertResultSize(10);
    }

    @Test
    public void testNonIndexedValueExceededBoundedRangeAndAnchorTerm() {
        writeData("FIELD_A", "value-a", 10);
        withQuery("FIELD_A == 'value-a' && ((_Value_ = true) && ((_Bounded_ = true) && (NON_INDEXED >= 'a' && NON_INDEXED <= 'z')))");
        drive();
        assertResultSize(10);
    }

    @Test
    public void testNonIndexedRegexAndAnchorTerm() {
        writeData("FIELD_A", "value-a", 10);
        withQuery("FIELD_A == 'value-a' && NON_INDEXED =~ 'a.*'");
        drive();
        assertResultSize(10);
    }

    /**
     * Technically this should never happen, but a user could submit a query like this
     */
    @Test
    public void testNonIndexedExceededValueRegexAndAnchorTerm() {
        writeData("FIELD_A", "value-a", 10);
        withQuery("FIELD_A == 'value-a' && ((_Value_ = true) && (NON_INDEXED =~ 'a.*'))");
        drive();
        assertResultSize(10);
    }

    @Test
    public void testNonIndexedEvaluationOnlyRegexAndAnchorTerm() {
        writeData("FIELD_A", "value-a", 10);
        withQuery("FIELD_A == 'value-a' && ((_Eval_ = true) && (NON_INDEXED =~ 'a.*'))");
        drive();
        assertResultSize(10);
    }

    @Test
    public void testNonIndexedListMarkerAndAnchorTerm() {
        writeData("FIELD_A", "value-a", 10);
        withQuery("FIELD_A == 'value-a' && ((_List_ = true) && (((id = 'uuid') && (field = 'NON_INDEXED') && (params = '{\"values\":[\"value-a\"]}'))))");
        drive();
        assertResultSize(10);
    }

    /**
     * A negated intersection whose terms are all non-executable restricts nothing. Returning the incoming context instead of null previously had the enclosing
     * intersection subtract its own result from itself and find nothing.
     */
    @Test
    public void testNegatedNonExecutableIntersectionLeavesIntersectionIntact() {
        writeData("FIELD_A", "value-a", 10);
        // NON_INDEXED_1 / NON_INDEXED_2 are deliberately absent from indexedFields
        withQuery("FIELD_A == 'value-a' && !(NON_INDEXED_1 == 'value-b' && NON_INDEXED_2 == 'value-c')");
        drive();
        assertResultSize(10);
    }

    /**
     * Companion to {@link #testNegatedNonExecutableIntersectionLeavesIntersectionIntact()} for a negated union carrying a negation of its own, which reaches
     * the De Morgan path and used to hand back the candidate set as the matches to remove.
     */
    @Test
    public void testNegatedMixedUnionLeavesIntersectionIntact() {
        writeRange("FIELD_A", "value-a", 1, 10);
        writeRange("FIELD_B", "value-b", 3, 5);
        // NON_INDEXED is deliberately absent from indexedFields, leaving the negated disjunct undecided

        withQuery("FIELD_A == 'value-a' && !(FIELD_B == 'value-b' || !(NON_INDEXED == 'value-z'))");
        drive();
        assertResultSize(10);
    }

    /**
     * An undecided disjunct leaves a union unable to restrict anything, so the anchor's candidates all survive. Skipping the disjunct instead would intersect
     * the anchor with the one union branch the field index can see, dropping any document that matches only the branch it cannot.
     */
    @Test
    public void testAnchorWithUnionOfNonExecutableIntersection() {
        writeRange("FIELD_A", "value-a", 1, 10);
        writeRange("FIELD_B", "value-b", 3, 5);
        // NON_INDEXED_1 / NON_INDEXED_2 are deliberately absent from indexedFields

        withQuery("FIELD_A == 'value-a' && (FIELD_B == 'value-b' || (NON_INDEXED_1 == 'x' && NON_INDEXED_2 == 'y'))");
        drive();
        assertResultSize(10);
    }

    /**
     * Companion to {@link #testAnchorWithUnionOfNonExecutableIntersection()} with the undecided disjunct first, which used to make the union adopt the anchor's
     * own ScanResult and then union the second disjunct into it, adding uids the anchor never matched.
     */
    @Test
    public void testAnchorWithUnionLeadingWithNonExecutableIntersection() {
        // uid 5 is deliberately absent from the anchor but present in FIELD_B, and falls inside the anchor's key range
        // so that the second disjunct's scan actually reaches it
        writeIndices("FIELD_A", "value-a", 1, 2, 3, 4, 6, 7, 8, 9, 10);
        writeIndices("FIELD_B", "value-b", 5);
        // NON_INDEXED_1 / NON_INDEXED_2 are deliberately absent from indexedFields

        withQuery("FIELD_A == 'value-a' && ((NON_INDEXED_1 == 'x' && NON_INDEXED_2 == 'y') || FIELD_B == 'value-b')");
        drive();
        assertResultSize(9);
    }

    /**
     * A negated intersection nested in a union, which the visitor previously could not execute at all. {@code B || (!C && !D)} is false exactly where
     * {@code !B && (C || D)} holds, so those documents come out of the anchor.
     */
    @Test
    public void testAnchorWithUnionOfNegatedIntersection() {
        writeRange("FIELD_A", "value-a", 1, 10);
        writeRange("FIELD_B", "value-b", 1, 2);
        writeRange("FIELD_C", "value-c", 3, 5);
        writeRange("FIELD_D", "value-d", 5, 7);

        withQuery("FIELD_A == 'value-a' && (FIELD_B == 'value-b' || (!(FIELD_C == 'value-c') && !(FIELD_D == 'value-d')))");
        drive();
        assertResultSize(5);
    }

    /**
     * A union holding a term the field index cannot decide bounds nothing, wherever it sits. Both of these leave the anchor whole, where the union used to
     * narrow the anchor to the one disjunct it could see when it appeared first, and not when it appeared second.
     */
    @Test
    public void testUndecidedUnionBoundsNothingInEitherPosition() {
        writeRange("FIELD_A", "value-a", 1, 10);
        writeRange("FIELD_B", "value-b", 3, 5);
        // NON_INDEXED is deliberately absent from indexedFields

        withQuery("(FIELD_B == 'value-b' || NON_INDEXED == 'value-z') && FIELD_A == 'value-a'");
        drive();
        assertResultSize(10);

        withQuery("FIELD_A == 'value-a' && (FIELD_B == 'value-b' || NON_INDEXED == 'value-z')");
        drive();
        assertResultSize(10);
    }

    /**
     * A marker makes its source non-executable, so negating one restricts nothing and the anchor's candidates all survive. Returning the incoming context
     * instead of null previously had the intersection subtract its own result from itself and find nothing.
     */
    @Test
    public void testNegatedDelayedMarkerLeavesIntersectionIntact() {
        writeData("FIELD_A", "value-a", 10);
        withQuery("FIELD_A == 'value-a' && !(((_Delayed_ = true) && (NON_INDEXED == 'value-b')))");
        drive();
        assertResultSize(10);
    }

    /**
     * Companion to {@link #testNegatedDelayedMarkerLeavesIntersectionIntact()} where the collapse would take a whole branch of a union with it.
     */
    @Test
    public void testNegatedEvaluationOnlyMarkerLeavesNestedIntersectionIntact() {
        writeData("FIELD_A", "value-a", 5);
        writeData("FIELD_B", "value-b", 15);
        withQuery("FIELD_A == 'value-a' || (FIELD_B == 'value-b' && !(((_Eval_ = true) && (NON_INDEXED =~ 'a.*'))))");
        drive();
        assertResultSize(15);
    }

    /**
     * A union holding a term the field index cannot decide has no candidate set to report. Counting it as zero documents would be a silently wrong answer, so
     * the scan fails instead.
     */
    @Test
    public void testUndecidedTopLevelUnionIsRefused() {
        writeData("FIELD_A", "value-a", 10);
        // NON_INDEXED is deliberately absent from indexedFields
        withQuery("FIELD_A == 'value-a' || NON_INDEXED == 'value-z'");
        assertUnbounded();
    }

    /**
     * Companion to {@link #testUndecidedTopLevelUnionIsRefused()}: the field index can find the documents a term matches, never the documents it does not, so a
     * negation with nothing to be subtracted from is refused.
     */
    @Test
    public void testBareNegationIsRefused() {
        writeData("FIELD_A", "value-a", 10);
        withQuery("!(FIELD_A == 'value-a')");
        assertUnbounded();
    }

    /**
     * The planner rewrites a negated equality before the query reaches the field index, so finding one here means the tree was never planned.
     */
    @Test
    public void testNotEqualsIsRejected() {
        writeData("FIELD_A", "value-a", 10);
        withQuery("FIELD_A == 'value-a' && FIELD_B != 'value-b'");
        assertUnrewrittenNegation();
    }

    /**
     * Companion to {@link #testNotEqualsIsRejected()} for a negated regex.
     */
    @Test
    public void testNotRegexIsRejected() {
        writeData("FIELD_A", "value-a", 10);
        withQuery("FIELD_A == 'value-a' && FIELD_B !~ 'value.*'");
        assertUnrewrittenNegation();
    }

    /**
     * A range operator only reaches the field index unwrapped when it is open ended, and an open ended range cannot be scanned. Deciding it anyway previously
     * had the intersection subtract its own result from itself and return nothing.
     */
    @Test
    public void testNegatedOpenEndedRangeLeavesIntersectionIntact() {
        writeData("FIELD_A", "value-a", 10);
        writeData("FIELD_B", "value-b", 15);
        withQuery("FIELD_A == 'value-a' && !(FIELD_B >= 'value-b')");
        drive();
        assertResultSize(10);
    }

    /**
     * Companion to {@link #testNegatedOpenEndedRangeLeavesIntersectionIntact()} covering the remaining range operators.
     */
    @Test
    public void testEveryNegatedOpenEndedRangeLeavesIntersectionIntact() {
        writeData("FIELD_A", "value-a", 10);
        writeData("FIELD_B", "value-b", 15);

        for (String operator : Set.of(">", ">=", "<", "<=")) {
            withQuery("FIELD_A == 'value-a' && !(FIELD_B " + operator + " 'value-b')");
            drive();
            assertResultSize(10);
        }
    }

    /**
     * An undecided term narrows nothing, so the anchor's candidates all survive and the extra documents are discarded by evaluating them.
     */
    @Test
    public void testOpenEndedRangeInIntersectionDoesNotNarrow() {
        writeData("FIELD_A", "value-a", 10);
        writeData("FIELD_B", "value-b", 5);
        withQuery("FIELD_A == 'value-a' && FIELD_B >= 'value-b'");
        drive();
        assertResultSize(10);
    }

    /**
     * An open ended range is undecided, and a union holding an undecided term bounds nothing.
     */
    @Test
    public void testOpenEndedRangeInUnionIsRefused() {
        writeData("FIELD_A", "value-a", 10);
        writeData("FIELD_B", "value-b", 15);
        withQuery("FIELD_A == 'value-a' || FIELD_B >= 'value-b'");
        assertUnbounded();
    }

    /**
     * A leniency marker only says how a missing field is evaluated, so unlike a delayed marker its source is still scanned and still narrows an intersection.
     */
    @Test
    public void testLenientMarkerIsScanned() {
        writeData("FIELD_A", "value-a", 10);
        writeData("FIELD_B", "value-b", 5);
        withQuery("FIELD_A == 'value-a' && ((_Lenient_ = true) && (FIELD_B == 'value-b'))");
        drive();
        assertResultSize(5);
    }

    /**
     * Companion to {@link #testLenientMarkerIsScanned()} for the strict marker.
     */
    @Test
    public void testStrictMarkerIsScanned() {
        writeData("FIELD_A", "value-a", 10);
        writeData("FIELD_B", "value-b", 5);
        withQuery("FIELD_A == 'value-a' && ((_Strict_ = true) && (FIELD_B == 'value-b'))");
        drive();
        assertResultSize(5);
    }

    /**
     * The query model wraps an expanded union in a leniency marker, so refusing the marker would fail a query the planner accepted.
     */
    @Test
    public void testLenientModelExpansionIsBounded() {
        writeData("FIELD_A", "value-a", 10);
        writeData("FIELD_B", "value-a", 15);
        withQuery("((_Lenient_ = true) && (FIELD_A == 'value-a' || FIELD_B == 'value-a'))");
        drive();
        assertResultSize(15);
    }

    /**
     * Scanning a leniency marker's source does not make an unscannable source decidable, so the anchor's candidates all survive.
     */
    @Test
    public void testLenientMarkerWithNonIndexedSourceStaysUndecided() {
        writeData("FIELD_A", "value-a", 10);
        withQuery("FIELD_A == 'value-a' && ((_Lenient_ = true) && (NON_INDEXED == 'value-b'))");
        drive();
        assertResultSize(10);
    }

    /**
     * Companion to {@link #testLenientMarkerWithNonIndexedSourceStaysUndecided()}, negated, where deciding the marker would collapse the intersection.
     */
    @Test
    public void testNegatedLenientMarkerLeavesIntersectionIntact() {
        writeData("FIELD_A", "value-a", 10);
        withQuery("FIELD_A == 'value-a' && !(((_Lenient_ = true) && (NON_INDEXED == 'value-b')))");
        drive();
        assertResultSize(10);
    }

    private void assertUnrewrittenNegation() {
        ASTJexlScript script = parse(query);
        SortedKeyValueIterator<Key,Value> source = createSource();

        DocIdIteratorVisitor visitor = new DocIdIteratorVisitor(source, range, datatypes, null, indexedFields);
        DatawaveFatalQueryException e = assertThrows(DatawaveFatalQueryException.class, () -> visitor.getDocIds(script));
        assertTrue(e.getMessage().contains("RewriteNegationsVisitor"), e.getMessage());
    }

    public void withQuery(String query) {
        this.query = query;
    }

    protected void drive() {
        // always clear results before each test iteration
        results.clear();

        ASTJexlScript script = parse(query);
        SortedKeyValueIterator<Key,Value> source = createSource();

        DocIdIteratorVisitor visitor = new DocIdIteratorVisitor(source, range, datatypes, null, indexedFields);
        results.addAll(visitor.getDocIds(script));
    }

    /**
     * Asserts that the field index cannot bound the query, which is a failure rather than an empty result
     */
    protected void assertUnbounded() {
        ASTJexlScript script = parse(query);
        SortedKeyValueIterator<Key,Value> source = createSource();

        DocIdIteratorVisitor visitor = new DocIdIteratorVisitor(source, range, datatypes, null, indexedFields);
        DatawaveFatalQueryException e = assertThrows(DatawaveFatalQueryException.class, () -> visitor.getDocIds(script));
        assertTrue(e.getMessage().contains("full table scan"), e.getMessage());
    }

    /**
     * Drives the visitor with a scan time limit. Terms must be written with more hits than the visitor's result check interval for the limit to be reached, and
     * a term scanned without an existing candidate set is never timed out, so only a term nested under an anchor reports a partial scan.
     *
     * @param maxScanTimeMillis
     *            the scan time limit
     */
    protected void driveWithScanTimeout(long maxScanTimeMillis) {
        results.clear();

        ASTJexlScript script = parse(query);
        SortedKeyValueIterator<Key,Value> source = createSource();

        DocIdIteratorVisitor visitor = new DocIdIteratorVisitor(source, range, datatypes, null, indexedFields);
        visitor.setMaxScanTimeMillis(maxScanTimeMillis);
        results.addAll(visitor.getDocIds(script));
    }

    @Override
    protected BaseDocIdIterator createIterator() {
        throw new IllegalStateException("Should never be called");
    }
}
