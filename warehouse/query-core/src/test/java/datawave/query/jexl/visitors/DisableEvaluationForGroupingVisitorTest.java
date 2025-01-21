package datawave.query.jexl.visitors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Set;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.jupiter.api.Test;

import datawave.query.jexl.JexlASTHelper;

public class DisableEvaluationForGroupingVisitorTest {

    private final Set<String> indexedFields = Set.of("INDEXED_FIELD", "INDEXED_FIELD2");
    private final Set<String> indexOnlyFields = Set.of("INDEX_ONLY", "INDEX_ONLY2");

    @Test
    public void testEQ() {
        // equals
        test("INDEXED_FIELD == 'value'", true);
        test("INDEX_ONLY == 'value'", true);
        test("NON_INDEXED_FIELD == 'value'", false);

        // equals with null literal
        test("INDEXED_FIELD == null", false);
        test("INDEX_ONLY == null", false);
        test("NON_INDEXED_FIELD == null", false);
    }

    @Test
    public void testNE() {
        // not equals
        test("INDEXED_FIELD != 'value'", true);
        test("INDEX_ONLY != 'value'", true);
        test("NON_INDEXED_FIELD != 'value'", false);

        // not equals with null literal
        test("INDEXED_FIELD != null", false);
        test("INDEX_ONLY != null", false);
        test("NON_INDEXED_FIELD != null", false);
    }

    @Test
    public void testNotEQ() {
        // not equals
        test("!(INDEXED_FIELD == 'value')", true);
        test("!(INDEX_ONLY == 'value')", true);
        test("!(NON_INDEXED_FIELD == 'value')", false);
    }

    @Test
    public void testLt() {
        // less than, not part of a bounded range
        test("INDEXED_FIELD < 'value'", true);
        test("INDEX_ONLY < 'value'", true);
        test("NON_INDEXED_FIELD < 'value'", false);
    }

    @Test
    public void testGt() {
        // greater than, not part of a bounded range
        test("INDEXED_FIELD > 'value'", true);
        test("INDEX_ONLY > 'value'", true);
        test("NON_INDEXED_FIELD > 'value'", false);
    }

    @Test
    public void testLe() {
        // less than equals, not part of a bounded range
        test("INDEXED_FIELD <= 'value'", true);
        test("INDEX_ONLY <= 'value'", true);
        test("NON_INDEXED_FIELD <= 'value'", false);
    }

    @Test
    public void testGe() {
        // less than, not part of a bounded range
        test("INDEXED_FIELD >= 'value'", true);
        test("INDEX_ONLY >= 'value'", true);
        test("NON_INDEXED_FIELD >= 'value'", false);
    }

    @Test
    public void testBoundedRangeMarker() {
        // bounded range
        test("((_Bounded_ = true) && (INDEXED_FIELD > 'aa' && INDEXED_FIELD < 'bb'))", true);
        test("((_Bounded_ = true) && (INDEX_ONLY > 'aa' && INDEX_ONLY < 'bb'))", true);
        test("((_Bounded_ = true) && (NON_INDEXED_FIELD > 'aa' && NON_INDEXED_FIELD < 'bb'))", false);
    }

    @Test
    public void testValueExceededMarker() {
        // exceeded value marker
        test("((_Value_ = true) && (INDEXED_FIELD =~ 'ba.*'))", true);
        test("((_Value_ = true) && (INDEX_ONLY =~ 'ba.*'))", true);
        test("((_Value_ = true) && (NON_INDEXED_FIELD =~ 'ba.*'))", false);
    }

    @Test
    public void testTermExceededMarker() {
        // exceeded term marker
        test("((_Term_ = true) && (INDEXED_FIELD =~ 'ba.*'))", true);
        test("((_Term_ = true) && (INDEX_ONLY =~ 'ba.*'))", true);
        test("((_Term_ = true) && (NON_INDEXED_FIELD =~ 'ba.*'))", false);
    }

    @Test
    public void testDelayedMarker() {
        // delayed leaf
        test("((_Delayed_ = true) && (INDEXED_FIELD =~ 'ba.*'))", false);
        test("((_Delayed_ = true) && (INDEX_ONLY =~ 'ba.*'))", false);
        test("((_Delayed_ = true) && (NON_INDEXED_FIELD =~ 'ba.*'))", false);
        // delayed bounded range
        test("((_Delayed_ = true) && ((_Bounded_ = true) && (INDEXED_FIELD > 'aa' && INDEXED_FIELD < 'bb')))", false);
        test("((_Delayed_ = true) && ((_Bounded_ = true) && (INDEX_ONLY > 'aa' && INDEX_ONLY < 'bb')))", false);
        test("((_Delayed_ = true) && ((_Bounded_ = true) && (NON_INDEXED_FIELD > 'aa' && NON_INDEXED_FIELD < 'bb')))", false);
    }

    @Test
    public void testEvalOnlyMarker() {
        // evaluation only marker
        test("((_Eval_ = true) && (INDEXED_FIELD =~ 'ba.*'))", false);
        test("((_Eval_ = true) && (INDEX_ONLY =~ 'ba.*'))", false);
        test("((_Eval_ = true) && (NON_INDEXED_FIELD =~ 'ba.*'))", false);
    }

    private void test(String query, boolean expected) {
        ASTJexlScript script = parse(query);
        boolean result = DisableEvaluationForGroupingVisitor.canDisableEvaluation(script, indexedFields, indexOnlyFields);
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
