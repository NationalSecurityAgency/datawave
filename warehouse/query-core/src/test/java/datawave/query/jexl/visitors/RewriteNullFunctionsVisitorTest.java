package datawave.query.jexl.visitors;

import datawave.query.exceptions.InvalidQueryTreeException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.validate.ASTValidator;
import datawave.test.JexlNodeAssert;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class RewriteNullFunctionsVisitorTest {

    private final ASTValidator validator = new ASTValidator();

    // test single fielded isNull functions

    @Test
    public void testSingleFieldedIsNull() throws ParseException {
        String query = "filter:isNull(FOO)";
        String expected = "FOO == null";
        test(query, expected);
    }

    @Test
    public void testUnionWithSingleFieldedIsNull() throws ParseException {
        String query = "FOO == 'bar' || filter:isNull(FOO)";
        String expected = "FOO == 'bar' || FOO == null";
        test(query, expected);
    }

    @Test
    public void testIntersectionWithSingleFieldedIsNull() throws ParseException {
        String query = "FOO == 'bar' && filter:isNull(FOO)";
        String expected = "FOO == 'bar' && FOO == null";
        test(query, expected);
    }

    // test multi fielded isNull functions

    @Test
    public void testMultiFieldedIsNull() throws ParseException {
        String query = "filter:isNull(FOO || FOO2)";
        String expected = "(FOO == null && FOO2 == null)";
        test(query, expected);

        query = "filter:isNull((FOO || FOO2))";
        expected = "(FOO == null && FOO2 == null)";
        test(query, expected);
    }

    @Test
    public void testUnionWithMultiFieldedIsNull() throws ParseException {
        String query = "FOO == 'bar' || filter:isNull(FOO || FOO2)";
        String expected = "FOO == 'bar' || (FOO == null && FOO2 == null)";
        test(query, expected);

        query = "FOO == 'bar' || filter:isNull((FOO || FOO2))";
        expected = "FOO == 'bar' || (FOO == null && FOO2 == null)";
        test(query, expected);
    }

    @Test
    public void testIntersectionWithMultiFieldedIsNull() throws ParseException {
        String query = "FOO == 'bar' && filter:isNull(FOO || FOO2)";
        String expected = "FOO == 'bar' && FOO == null && FOO2 == null";
        test(query, expected);

        query = "FOO == 'bar' && filter:isNull((FOO || FOO2))";
        expected = "FOO == 'bar' && FOO == null && FOO2 == null";
        test(query, expected);
    }

    // test single fielded isNotNull functions

    @Test
    public void testSingleFieldedIsNotNull() throws ParseException {
        String query = "filter:isNotNull(FOO)";
        String expected = "!(FOO == null)";
        test(query, expected);
    }

    @Test
    public void testUnionWithSingleFieldedIsNotNull() throws ParseException {
        String query = "FOO == 'bar' || filter:isNotNull(FOO)";
        String expected = "FOO == 'bar' || !(FOO == null)";
        test(query, expected);
    }

    @Test
    public void testIntersectionWithSingleFieldedIsNotNull() throws ParseException {
        String query = "FOO == 'bar' && filter:isNotNull(FOO)";
        String expected = "FOO == 'bar' && !(FOO == null)";
        test(query, expected);
    }

    // test multi fielded isNotNull functions

    @Test
    public void testMultiFieldedIsNotNull() throws ParseException {
        String query = "filter:isNotNull(FOO || FOO2)";
        String expected = "(!(FOO == null) || !(FOO2 == null))";
        test(query, expected);

        query = "filter:isNotNull((FOO || FOO2))";
        expected = "(!(FOO == null) || !(FOO2 == null))";
        test(query, expected);
    }

    @Test
    public void testUnionWithMultiFieldedIsNotNull() throws ParseException {
        String query = "FOO == 'bar' || filter:isNotNull(FOO || FOO2)";
        String expected = "FOO == 'bar' || !(FOO == null) || !(FOO2 == null)";
        test(query, expected);

        query = "FOO == 'bar' || filter:isNotNull((FOO || FOO2))";
        expected = "FOO == 'bar' || !(FOO == null) || !(FOO2 == null)";
        test(query, expected);
    }

    @Test
    public void testIntersectionWithMultiFieldedIsNotNull() throws ParseException {
        String query = "FOO == 'bar' && filter:isNotNull(FOO || FOO2)";
        String expected = "FOO == 'bar' && (!(FOO == null) || !(FOO2 == null))";
        test(query, expected);

        query = "FOO == 'bar' && filter:isNotNull((FOO || FOO2))";
        expected = "FOO == 'bar' && (!(FOO == null) || !(FOO2 == null))";
        test(query, expected);
    }

    // mixed function case

    @Test
    public void testMixOfNullFunctions() throws ParseException {
        String query = "filter:isNull(F1 || F2) && filter:isNotNull(FOO || FOO2)";
        String expected = "F1 == null && F2 == null && (!(FOO == null) || !(FOO2 == null))";
        test(query, expected);

        query = "filter:isNull(F1 || F2) || filter:isNotNull(FOO || FOO2)";
        expected = "(F1 == null && F2 == null) || !(FOO == null) || !(FOO2 == null)";
        test(query, expected);
    }

    // larger edge cases

    @Test
    public void testManyFields() throws ParseException {
        String query = "filter:isNull(F1 || F2 || F3 || F4)";
        String expected = "(F1 == null && F2 == null && F3 == null && F4 == null)";
        test(query, expected);

        query = "filter:isNotNull(F1 || F2 || F3 || F4)";
        expected = "(!(F1 == null) || !(F2 == null) || !(F3 == null) || !(F4 == null))";
        test(query, expected);
    }

    @Test
    public void testWeirdModelExpansion() throws ParseException {
        // this happens when the input query is like #ISNULL((F0 OR F3)) and F0 is expanded to (F1 OR F2)
        String query = "filter:isNull(((F1 || F2) || F3))";
        String expected = "(F1 == null && F2 == null && F3 == null)";
        test(query, expected);
    }

    @Test
    public void testNullFunctionRewriteWithBoundedRange() throws ParseException {
        String query = "((_Bounded_ = true) && (NUM > '1' && NUM < '5')) && filter:isNotNull((F1||F2)) && FOO == 'bar'";
        String expected = "((_Bounded_ = true) && (NUM > '1' && NUM < '5')) && (!(F1 == null) || !(F2 == null)) && FOO == 'bar'";
        test(query, expected);
    }

    private void test(String original, String expected) throws ParseException {
        ASTJexlScript originalScript = JexlASTHelper.parseAndFlattenJexlQuery(original);
        ASTJexlScript actual = RewriteNullFunctionsVisitor.rewriteNullFunctions(originalScript);

        JexlNodeAssert.assertThat(actual).isEqualTo(expected).hasValidLineage();

        try {
            validator.isValid(actual);
        } catch (InvalidQueryTreeException e) {
            fail("IsNotNullIntentVisitor produced an invalid query tree: " + e.getMessage());
        }

        assertEquals(expected, JexlStringBuildingVisitor.buildQueryWithoutParse(actual));
    }
}
