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
