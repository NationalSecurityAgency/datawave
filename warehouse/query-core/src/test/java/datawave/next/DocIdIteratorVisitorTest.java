package datawave.next;

import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DocIdIteratorVisitorTest extends FieldIndexDataTestUtil {

    private String query;
    private final Range range = new Range(row);

    private final Set<String> indexedFields = Set.of("FIELD_A", "FIELD_B", "FIELD_C");

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
     * A non-executable de-negated term (e.g. a non-indexed field) is presumed to never match, so its negation is presumed always true and the candidate set is
     * left unrestricted.
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
     * A non-executable positive disjunct contributes nothing to the union, same as a non-executable member of any other union, so the remaining negated
     * disjunct still restricts the candidate set.
     */
    @Test
    public void testAnchorWithMixedUnionNonExecutablePositiveTerm() {
        writeRange("FIELD_A", "value-a", 1, 10);
        writeRange("FIELD_C", "value-c", 4, 6);
        // NON_INDEXED is deliberately absent from indexedFields

        withQuery("FIELD_A == 'value-a' && (NON_INDEXED == 'value-z' || !(FIELD_C == 'value-c'))");
        drive();
        assertResultSize(7);
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

    public void withQuery(String query) {
        this.query = query;
    }

    protected void drive() {
        // always clear results before each test iteration
        results.clear();

        ASTJexlScript script = parse(query);
        SortedKeyValueIterator<Key,Value> source = createSource();

        Set<Key> ids = DocIdIteratorVisitor.getDocIds(script, range, source, datatypes, null, indexedFields);
        results.addAll(ids);
    }

    @Override
    protected BaseDocIdIterator createIterator() {
        throw new IllegalStateException("Should never be called");
    }
}
