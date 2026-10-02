package datawave.next;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

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
    public void testNestedNegatedUnionWithUnindexedTermsRetainsAnchorCandidates() {
        writeRange("FIELD_A", "value-a", 1, 4);
        writeIndex("FIELD_B", "value-b", "datatype-a", 1);
        writeIndex("FIELD_X", "value-x", "datatype-a", 2);
        writeIndex("FIELD_Y", "value-y", "datatype-a", 3);

        withQuery("FIELD_A == 'value-a' && (FIELD_B == 'value-b' || !(FIELD_X == 'value-x' || FIELD_Y == 'value-y'))");
        drive();

        assertResultUids(1, 2, 3, 4);
    }

    @Test
    public void testNestedNegatedConjunctionWithUnindexedTermRetainsUnconfirmedCandidates() {
        writeRange("FIELD_A", "value-a", 1, 4);
        writeIndex("FIELD_D", "value-d", "datatype-a", 1);
        writeRange("FIELD_B", "value-b", 2, 3);

        // FIELD_X is intentionally absent from indexedFields, so the FIELD_B hits do not prove the conjunction.
        withQuery("FIELD_A == 'value-a' && (FIELD_D == 'value-d' || !(FIELD_X == 'value-x' && FIELD_B == 'value-b'))");
        drive();

        assertResultUids(1, 2, 3, 4);
    }

    @Test
    public void testIncompleteUnionDoesNotNarrowAnchorCandidates() {
        writeRange("FIELD_A", "value-a", 1, 4);
        writeIndex("FIELD_B", "value-b", "datatype-a", 1);

        // FIELD_X is intentionally absent from indexedFields, so FIELD_B is only a confirmed subset of the union.
        withQuery("FIELD_A == 'value-a' && (FIELD_X == 'value-x' || FIELD_B == 'value-b')");
        drive();

        assertResultUids(1, 2, 3, 4);
    }

    @Test
    public void testIncompleteUnionBeforeAnchorDoesNotNarrowAnchorCandidates() {
        writeRange("FIELD_A", "value-a", 1, 4);
        writeIndex("FIELD_B", "value-b", "datatype-a", 1);

        // FIELD_X is intentionally absent from indexedFields. Child order must not let the incomplete union bound the complete anchor scan.
        withQuery("(FIELD_X == 'value-x' || FIELD_B == 'value-b') && FIELD_A == 'value-a'");
        drive();

        assertResultUids(1, 2, 3, 4);
    }

    @Test
    public void testEmptyIncompleteUnionBeforeAnchorDoesNotShortCircuit() {
        writeRange("FIELD_A", "value-a", 1, 4);

        // FIELD_X is unindexed and FIELD_B has no hits, so the empty union remains incomplete and cannot prove the intersection empty.
        withQuery("(FIELD_X == 'value-x' || FIELD_B == 'value-b') && FIELD_A == 'value-a'");
        drive();

        assertResultUids(1, 2, 3, 4);
    }

    @Test
    public void testMultipleIncompleteUnionsBeforeAnchorDoNotShortCircuit() {
        writeRange("FIELD_A", "value-a", 1, 4);
        writeIndex("FIELD_C", "value-c", "datatype-a", 1);

        withQuery("(FIELD_X == 'value-x' || FIELD_B == 'value-b') && (FIELD_Y == 'value-y' || FIELD_C == 'value-c') && FIELD_A == 'value-a'");
        drive();

        assertResultUids(1, 2, 3, 4);
    }

    @Test
    public void testIncompleteEmptyNestedIntersectionDoesNotBoundNegation() {
        writeRange("FIELD_A", "value-a", 1, 4);
        writeIndex("FIELD_C", "value-c", "datatype-a", 1);

        withQuery("FIELD_A == 'value-a' && ((FIELD_X == 'value-x' || FIELD_B == 'value-b') && !(FIELD_C == 'value-c'))");
        drive();

        assertResultUids(2, 3, 4);
    }

    @Test
    public void testExactEmptyNestedIntersectionKeepsUnionComplete() {
        writeRange("FIELD_D", "value-d", 1, 4);
        writeRange("FIELD_B", "value-b", 1, 2);
        writeIndex("FIELD_C", "value-c", "datatype-a", 2);

        withQuery("FIELD_D == 'value-d' && ((FIELD_A == 'value-a' && FIELD_B == 'value-b') || FIELD_C == 'value-c')");
        drive();

        assertResultUids(2);
    }

    @Test
    public void testNestedComplementsPreserveUnsafeContext() {
        writeRange("FIELD_A", "value-a", 1, 4);
        writeRange("FIELD_B", "value-b", 1, 2);
        writeRange("FIELD_C", "value-c", 2, 3);

        // FIELD_X makes the anchor a conservative candidate set. Nested complements must not turn it back into a subtraction-safe result.
        withQuery("(FIELD_A == 'value-a' && FIELD_X == 'value-x') && !(!(FIELD_B == 'value-b') || !(FIELD_C == 'value-c'))");
        drive();

        assertResultUids(1, 2, 3, 4);
    }

    @Test
    public void testNestedUnionOfNegationsUsesDeMorganSemantics() {
        writeRange("FIELD_A", "value-a", 1, 5);
        writeRange("FIELD_B", "value-b", 1, 2);
        writeRange("FIELD_C", "value-c", 2, 3);

        withQuery("FIELD_A == 'value-a' && (!(FIELD_B == 'value-b') || !(FIELD_C == 'value-c'))");
        drive();

        assertResultUids(1, 3, 4, 5);
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

    private void assertResultUids(Integer... expected) {
        SortedSet<Integer> actual = new TreeSet<>();
        for (Key result : results) {
            String columnFamily = result.getColumnFamily().toString();
            actual.add(Integer.parseInt(columnFamily.substring(columnFamily.lastIndexOf('-') + 1)) - 1_000);
        }
        assertEquals(new TreeSet<>(Set.of(expected)), actual);
    }

    @Override
    protected BaseDocIdIterator createIterator() {
        throw new IllegalStateException("Should never be called");
    }
}
