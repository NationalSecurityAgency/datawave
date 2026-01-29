package datawave.query.index.day;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Set;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.query.jexl.JexlASTHelper;

public class IndexedTermVisitorTest {

    private String query;
    private final Set<String> indexedFields = Set.of("A", "B", "C");
    private final Multimap<String,String> expected = HashMultimap.create();

    @BeforeEach
    public void before() {
        query = null;
        expected.clear();
    }

    @Test
    public void testSimpleQuery() {
        setQuery("A == '1'");
        addExpected("A", "1");
        test();
    }

    @Test
    public void testNullLiteral() {
        setQuery("A == null");
        test(); // assert empty
    }

    @Test
    public void testIntersection() {
        setQuery("(A == '1' && B == '2' && C == '3')");
        addExpected("A", "1");
        addExpected("B", "2");
        addExpected("C", "3");
        test();
    }

    @Test
    public void testUnion() {
        setQuery("(A == '1' || A == '2' || B == '3')");
        addExpected("A", Set.of("1", "2"));
        addExpected("B", "3");
        test();
    }

    @Test
    public void testNestedIntersection() {
        setQuery("A == '1' || (B == '2' && C == '3')");
        addExpected("A", "1");
        addExpected("B", "2");
        addExpected("C", "3");
        test();
    }

    @Test
    public void testNestedUnion() {
        setQuery("A == '1' && (B == '2' || C == '3')");
        addExpected("A", "1");
        addExpected("B", "2");
        addExpected("C", "3");
        test();
    }

    @Test
    public void testIntersectNonIndexedNonEquality() {

        // all queries use A term as anchor
        addExpected("A", "1");

        String[] queries = {"A == '1' && D == '2'", // D is not indexed
                "A == '1' && B =~ 'ba.*'", // regex not used to search index
                "A == '1' && B !~ 'ba.*'", // NR nodes shouldn't exist after negation push down, but verify anyway
                "A == '1' && ((_Value_ = true) && (B == '2'))", // artificial value marker
                "A == '1' && ((_Eval_ = true) && (B == '2'))", "A == '1' && ((_Delayed_ = true) && (B == '2'))", "A == '1' && ((_Drop_ = true) && (B == '2'))",
                // content functions
                "A == '1' && content:adjacent(B, termOffsetMap, 'a', 'b')", "A == '1' && content:phrase(B, termOffsetMap, 'a', 'b', 'c')",};

        for (String query : queries) {
            setQuery(query);
            test();
        }
    }

    @Test
    public void testUnionNonIndexedNonEquality() {
        // all queries use A term as anchor
        addExpected("A", "1");

        String[] queries = {"A == '1' || D == '2'", // D is not indexed
                "A == '1' || B =~ 'ba.*'", // regex not used to search index
                "A == '1' || B !~ 'ba.*'", // NR nodes shouldn't exist after negation push down, but verify anyway
                "A == '1' || ((_Value_ = true) && (B == '2'))", // artificial value marker
                "A == '1' || ((_Eval_ = true) && (B == '2'))", "A == '1' || ((_Delayed_ = true) && (B == '2'))", "A == '1' || ((_Drop_ = true) && (B == '2'))",
                // content functions
                "A == '1' || content:adjacent(B, termOffsetMap, 'a', 'b')", "A == '1' || content:phrase(B, termOffsetMap, 'a', 'b', 'c')",};

        for (String query : queries) {
            setQuery(query);
            test();
        }
    }

    private void setQuery(String query) {
        this.query = query;
    }

    private void addExpected(String field, String value) {
        assertNotNull(expected);
        expected.put(field, value);
    }

    private void addExpected(String field, Set<String> values) {
        assertNotNull(expected);
        expected.putAll(field, values);
    }

    private void test() {
        assertNotNull(query);
        assertNotNull(expected);

        ASTJexlScript script = parse(query);
        Multimap<String,String> fieldsAndValues = IndexedTermVisitor.getIndexedFieldsAndValues(script, indexedFields);

        assertEquals(expected.keySet(), fieldsAndValues.keySet());
        for (String key : expected.keySet()) {
            assertEquals(expected.get(key), fieldsAndValues.get(key));
        }
    }

    private ASTJexlScript parse(String query) {
        try {
            return JexlASTHelper.parseAndFlattenJexlQuery(query);
        } catch (ParseException e) {
            fail("Failed to parse query: " + query);
            throw new RuntimeException(e);
        }
    }
}
