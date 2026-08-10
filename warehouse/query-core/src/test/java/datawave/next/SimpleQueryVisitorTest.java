package datawave.next;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Set;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.jupiter.api.Test;

import datawave.query.jexl.JexlASTHelper;

/**
 * Test cases for the {@link SimpleQueryVisitor}, which determines if a query can be handled by the document scheduler
 */
public class SimpleQueryVisitorTest {

    private final Set<String> indexedFields = Set.of("INDEXED", "INDEX_ONLY");
    private final Set<String> indexOnlyFields = Set.of("INDEX_ONLY");

    @Test
    public void testIndexedField() {
        test("INDEXED == 'a'", true);
    }

    @Test
    public void testIndexOnlyField() {
        test("INDEX_ONLY == 'a'", false);
    }

    @Test
    public void testNonEventField() {
        test("NON_EVENT == 'a'", false);
    }

    @Test
    public void testUnions() {
        test("INDEXED == 'a' || INDEXED == 'b'", true);
        test("INDEXED == 'a' || INDEX_ONLY == 'b'", false);
        test("INDEXED == 'a' || NON_EVENT == 'b'", true);
        test("INDEX_ONLY == 'a' || NON_EVENT == 'b'", false);
    }

    @Test
    public void testIntersections() {
        test("INDEXED == 'a' && INDEXED == 'b'", true);
        test("INDEXED == 'a' && INDEX_ONLY == 'b'", false);
        test("INDEXED == 'a' && NON_EVENT == 'b'", true);
        test("INDEX_ONLY == 'a' && NON_EVENT == 'b'", false);
    }

    @Test
    public void testValueMarker() {
        test("(_Value_ = true) && (INDEXED =~ 'ba.*')", true);
        test("(_Value_ = true) && (INDEX_ONLY =~ 'ba.*')", false);
        test("(_Value_ = true) && (NON_EVENT =~ 'ba.*')", false);
    }

    @Test
    public void testDelayedMarker() {
        test("(_Delayed_ = true) && (INDEXED =~ 'ba.*')", false);
        test("(_Delayed_ = true) && (INDEX_ONLY =~ 'ba.*')", false);
        test("(_Delayed_ = true) && (NON_EVENT =~ 'ba.*')", false);
    }

    @Test
    public void testEvaluationOnlyMarker() {
        test("(_Eval_ = true) && (INDEXED =~ 'ba.*')", false);
        test("(_Eval_ = true) && (INDEX_ONLY =~ 'ba.*')", false);
        test("(_Eval_ = true) && (NON_EVENT =~ 'ba.*')", false);
    }

    @Test
    public void testBoundedRangeMarker() {
        test("(_Bounded_ = true) && (INDEXED >= 1 && INDEXED <= 2)", true);
        test("(_Bounded_ = true) && (INDEX_ONLY >= 1 && INDEX_ONLY <= 2)", false);
        test("(_Bounded_ = true) && (NON_EVENT >= 1 && NON_EVENT <= 2)", false);
    }

    @Test
    public void testListMarker() {
        test("((_List_ = true) && (((id = 'uuid') && (field = 'INDEXED') && (params = '{\"values\":[\"value-a\"]}'))))", true);
        test("((_List_ = true) && (((id = 'uuid') && (field = 'INDEX_ONLY') && (params = '{\"values\":[\"value-a\"]}'))))", false);
        test("((_List_ = true) && (((id = 'uuid') && (field = 'NON_EVENT') && (params = '{\"values\":[\"value-a\"]}'))))", false);
    }

    @Test
    public void testIsNull() {
        test("INDEXED == 'a' && filter:isNull(INDEXED)", true);
        test("INDEXED == 'a' && INDEXED == null", true);
    }

    @Test
    public void testIsNotNull() {
        test("INDEXED == 'a' && filter:isNotNull(INDEXED)", true);
        test("INDEXED == 'a' && INDEXED != null", false);
        test("INDEXED == 'a' && !(INDEXED == null)", true);
    }

    @Test
    public void testNegatedEquality() {
        test("!(INDEXED == 'a')", true);
        test("!(INDEX_ONLY == 'a')", false);
        test("!(EVENT_ONLY == 'a')", false);
    }

    @Test
    public void testNegatedRegex() {
        test("INDEXED !~ 'ba.*'", false);
        test("INDEX_ONLY !~ 'ba.*'", false);
        test("EVENT_ONLY !~ 'ba.*'", false);

        test("!(INDEXED =~ 'ba.*')", true);
        test("!(INDEX_ONLY =~ 'ba.*')", false);
        test("!(EVENT_ONLY =~ 'ba.*')", false);
    }

    @Test
    public void testNonIndexedFalseCases() {
        test("NON_INDEXED == 'a'", false);
        test("(NON_INDEXED == 'a' || NON_INDEXED == 'b')", false);
        test("(NON_INDEXED == 'a' || NON_INDEXED == 'b')", false);
        test("INDEXED == 'a' && (NON_INDEXED == 'b' || NON_INDEXED == 'c')", true);
    }

    @Test
    public void testIntersectionWithFilterRegex() {
        test("INDEXED == 'a' && filter:includeRegex(INDEXED, 'ba.*')", true);
        test("INDEXED == 'a' && filter:includeRegex(INDEX_ONLY, 'ba.*')", true);
        test("INDEXED == 'a' && filter:includeRegex(EVENT_ONLY, 'ba.*')", true);
    }

    private void test(String query, boolean expected) {
        ASTJexlScript script = parse(query);
        boolean result = SimpleQueryVisitor.validate(script, indexedFields, indexOnlyFields);
        assertEquals(expected, result);
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
