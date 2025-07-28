package datawave.next;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.base.Joiner;

import datawave.query.iterator.QueryOptions;
import datawave.query.iterator.SourceManagerTest;

public class DocIdQueryIteratorTest extends FieldIndexDataTestUtil {

    private String query = null;

    private final Set<String> indexedFields = Set.of("FIELD_A", "FIELD_B");

    @BeforeEach
    public void setup() {
        query = null;
    }

    @Test
    public void testSingleTerm() {
        writeData("FIELD_A", "value-a", 11);
        withQuery("FIELD_A == 'value-a'");
        drive();
        assertResultSize(11);
    }

    @Test
    public void testUnion() {
        writeData("FIELD_A", "value-a", 11);
        writeData("FIELD_B", "value-b", 11);
        withQuery("FIELD_A == 'value-a' || FIELD_B == 'value-b'");
        drive();
        assertResultSize(11);
    }

    @Test
    public void testIntersection() {
        writeData("FIELD_A", "value-a", 11);
        writeData("FIELD_B", "value-b", 11);
        withQuery("FIELD_A == 'value-a' && FIELD_B == 'value-b'");
        drive();
        assertResultSize(11);
    }

    @Test
    public void testIntersectionWithNonIndexedField() {
        writeData("FIELD_A", "value-a", 11);
        withQuery("FIELD_A == 'value-a' && NON_INDEXED == 'value-b'");
        drive();
        assertResultSize(11);
    }

    @Test
    public void testIntersectionWithNonIndexedNestedUnion() {
        writeData("FIELD_A", "value-a", 11);
        withQuery("FIELD_A == 'value-a' && (NON_INDEXED == 'value-b' || NON_INDEXED == 'value-c')");
        drive();
        assertResultSize(11);
    }

    @Test
    public void testRegexTerm() {
        writeData("FIELD_A", "value-a", 11);
        withQuery("FIELD_A =~ 'val.*'");
        drive();
        assertResultSize(11);
    }

    @Test
    public void testBoundedRangeTerm() {
        writeData("FIELD_A", "value-a", 11);
        writeData("FIELD_A", "value-b", 11);
        writeData("FIELD_A", "value-c", 11);
        withQuery("((_Bounded_ = true) && (FIELD_A >= 'value-a' && FIELD_A <= 'value-c'))");
        drive();
        assertResultSize(11);
    }

    protected void withQuery(String query) {
        this.query = query;
    }

    protected void drive() {
        try {
            Map<String,String> options = new HashMap<>();
            options.put(QueryOptions.QUERY, query);
            options.put(QueryOptions.RANGES, new Range(row).toString());
            options.put(QueryOptions.START_TIME, String.valueOf(5));
            options.put(QueryOptions.END_TIME, String.valueOf(15));
            options.put(QueryOptions.INDEXED_FIELDS, Joiner.on(',').join(indexedFields));

            DocIdQueryIterator iter = new DocIdQueryIterator();
            iter.init(createSource(), options, new SourceManagerTest.MockIteratorEnvironment());
            iter.seek(new Range(row), Collections.emptySet(), true);

            while (iter.hasTop()) {
                Key result = iter.getTopKey();
                results.add(result);
                iter.next();
            }
        } catch (Exception e) {
            Assertions.fail("Saw exception", e);
        }
    }

    @Override
    protected BaseDocIdIterator createIterator() {
        throw new IllegalStateException("Should never be called");
    }
}
