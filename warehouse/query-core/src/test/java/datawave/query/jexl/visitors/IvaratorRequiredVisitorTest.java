package datawave.query.jexl.visitors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.jupiter.api.Test;

import datawave.query.jexl.JexlASTHelper;

public class IvaratorRequiredVisitorTest {

    @Test
    public void testSingleTerms() {
        // equality and not operators
        test("A == '11'", false);
        test("B != '23'", false);
        test("!(C == '34')", false);

        // range operators
        test("A <= '11'", false);
        test("B >= '23'", false);
        test("C < '34'", false);
        test("D > '77'", false);
    }

    @Test
    public void testMarkers() {
        // ivarator required
        test("((_Value_ = true) && (F =~ 'ba.*'))", true);
        test("((_Term_ = true) && (_ANYFIELD_ =~ 'ba.*'))", true);
        test("((_List_ = true) && (((id = 'some-bogus-id') && (field = 'QUOTE') && (params = '{\"values\":[\"a\",\"b\",\"c\"]}'))))", true);

        // ivarator not required
        test("((_Bounded_ = true) && (A >= 1 && A <= 2))", false);
        test("((_Delayed_ = true) && (F =~ 'pushed'))", false);
        test("((_Drop_ = true) && (F =~ 'evaluation'))", false);
        test("((_Eval_ = true) && (F =~ 'evaluation'))", false);
        test("((_Lenient_ = true) && (F =~ 'evaluation'))", false);
    }

    @Test
    public void testDoubleWrappedMarkers() {
        test("((_Value_ = true) && ((_Bounded_ = true) && (A >= 1 && A <= 2)) )", true);
    }

    @Test
    public void testOrIvarators() {
        // ivarator required
        test("A == '1' || ((_Value_ = true) && (F =~ 'ba.*'))", true);
        test("A == '1' || ((_Term_ = true) && (_ANYFIELD_ =~ 'ba.*'))", true);
        test("A == '1' || ((_List_ = true) && (((id = 'some-bogus-id') && (field = 'QUOTE') && (params = '{\"values\":[\"a\",\"b\",\"c\"]}'))))", true);

        // ivarator not required
        test("A == '1' || ((_Bounded_ = true) && (A >= 1 && A <= 2))", false);
        test("A == '1' || ((_Delayed_ = true) && (F =~ 'pushed'))", false);
        test("A == '1' || ((_Drop_ = true) && (F =~ 'evaluation'))", false);
        test("A == '1' || ((_Eval_ = true) && (F =~ 'evaluation'))", false);
        test("A == '1' || ((_Lenient_ = true) && (F =~ 'evaluation'))", false);
    }

    @Test
    public void testAndIvarators() {
        // ivarator required
        test("A == '1' && ((_Value_ = true) && (F =~ 'ba.*'))", true);
        test("A == '1' && ((_Term_ = true) && (_ANYFIELD_ =~ 'ba.*'))", true);
        test("A == '1' && ((_List_ = true) && (((id = 'some-bogus-id') && (field = 'QUOTE') && (params = '{\"values\":[\"a\",\"b\",\"c\"]}'))))", true);

        // ivarator not required
        test("A == '1' && ((_Bounded_ = true) && (A >= 1 && A <= 2))", false);
        test("A == '1' && ((_Eval_ = true) && (F =~ 'evaluation'))", false);
        test("A == '1' && ((_Delayed_ = true) && (F =~ 'pushed'))", false);
        test("A == '1' && ((_Drop_ = true) && (F =~ 'evaluation'))", false);
        test("A == '1' && ((_Lenient_ = true) && (F =~ 'evaluation'))", false);
    }

    @Test
    public void testDistributedNestedUnions() {
        // (A or Ivarator) AND (B or Ivarator)
        test("(A == '1' || ((_Value_ = true) && (F =~ 'ba.*'))) && (B == '2' || ((_Value_ = true) && (F =~ 'ba.*')))", true);
        // order should not matter
        test("(((_Value_ = true) && (F =~ 'ba.*')) || A == '1') && (((_Value_ = true) && (F =~ 'ba.*')) || B == '2')", true);
    }

    @Test
    public void testDistributedNestedIntersections() {
        // (A and ivarator) or (B and ivarator)
        test("(A == '1' && ((_Value_ = true) && (F =~ 'ba.*'))) || (B == '2' || ((_Value_ = true) && (F =~ 'ba.*')))", true);
        // order should not matter
        test("(((_Value_ = true) && (F =~ 'ba.*')) && A == '1') || (((_Value_ = true) && (F =~ 'ba.*')) && B == '2')", true);
    }

    @Test
    public void testDeeplyNestedIvarators() {
        // A and (B or (C and ivarator))
        test("A == '1' && (B == '2' || (C == '3' && ((_Value_ = true) && (F =~ 'ba.*'))))", true);
    }

    private void test(String query, boolean expected) {
        ASTJexlScript script = parse(query);
        assertEquals(expected, IvaratorRequiredVisitor.isIvaratorRequired(script));
    }

    private ASTJexlScript parse(String query) {
        try {
            return JexlASTHelper.parseAndFlattenJexlQuery(query);
        } catch (ParseException e) {
            fail("Failed to parse: " + query);
            throw new RuntimeException(e);
        }
    }
}
