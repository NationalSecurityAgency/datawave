package datawave.next;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Set;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.jupiter.api.Test;

import datawave.query.jexl.JexlASTHelper;

public class SimpleQueryVisitorTest {

    private final Set<String> indexedFields = Set.of("INDEXED", "INDEX_ONLY");
    private final Set<String> indexOnlyFields = Set.of("INDEX_ONLY");
    private final Set<String> nonEventFields = Set.of("NON_EVENT");

    @Test
    public void testIndexedField() {
        test("INDEXED == 'a'", true);
    }

    @Test
    public void testIndexOnlyField() {
        test("INDEX_ONLY == 'a'", true);
    }

    @Test
    public void testNonEventField() {
        test("NON_EVENT == 'a'", false);
    }

    @Test
    public void testUnions() {
        test("INDEXED == 'a' || INDEXED == 'b'", true);
        test("INDEXED == 'a' || INDEX_ONLY == 'b'", true);
        test("INDEXED == 'a' || NON_EVENT == 'b'", true);
        test("INDEX_ONLY == 'a' || NON_EVENT == 'b'", true);
    }

    @Test
    public void testIntersections() {
        test("INDEXED == 'a' && INDEXED == 'b'", true);
        test("INDEXED == 'a' && INDEX_ONLY == 'b'", true);
        test("INDEXED == 'a' && NON_EVENT == 'b'", true);
        test("INDEX_ONLY == 'a' && NON_EVENT == 'b'", true);
    }

    @Test
    public void testMarkers() {
        test("(_Value_ = true) && (INDEXED =~ 'ba.*')", true);
        test("(_Value_ = true) && (INDEX_ONLY =~ 'ba.*')", true);
        test("(_Value_ = true) && (NON_EVENT =~ 'ba.*')", false);

        test("(_Eval_ = true) && (INDEXED =~ 'ba.*')", false);
        test("(_Eval_ = true) && (INDEX_ONLY =~ 'ba.*')", false);
        test("(_Eval_ = true) && (NON_EVENT =~ 'ba.*')", false);

        test("(_Bounded_ = true) && (INDEXED >= 1 && INDEXED <= 2)", false);
        test("(_Bounded_ = true) && (INDEX_ONLY >= 1 && INDEX_ONLY <= 2)", false);
        test("(_Bounded_ = true) && (NON_EVENT >= 1 && NON_EVENT <= 2)", false);
    }

    @Test
    public void testKnownFalseCases() {
        test("INDEXED == 'a' && filter:isNull(INDEXED)", true);
        test("INDEXED == 'a' && filter:isNotNull(INDEXED)", true);
        test("INDEXED == 'a' && INDEXED == null", false);
        test("INDEXED == 'a' && INDEXED != null", false);
        test("INDEXED == 'a' && !(INDEXED == null)", false);
        test("INDEXED !~ 'ba.*'", false);
    }

    @Test
    public void testNonIndexedFalseCases() {
        test("NON_INDEXED == 'a'", false);
        test("(NON_INDEXED == 'a' || NON_INDEXED == 'b')", false);
        test("(NON_INDEXED == 'a' || NON_INDEXED == 'b')", false);
        test("INDEXED == 'a' && (NON_INDEXED == 'b' || NON_INDEXED == 'c')", true);
    }

    private void test(String query, boolean expected) {
        ASTJexlScript script = parse(query);
        boolean result = SimpleQueryVisitor.validate(script, indexedFields, indexOnlyFields, nonEventFields);
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
