package datawave.query.jexl.visitors;

import static datawave.query.jexl.JexlASTHelper.jexlFeatures;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collections;

import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;
import org.apache.commons.jexl3.parser.ParseException;
import org.apache.commons.jexl3.parser.Parser;
import org.apache.commons.jexl3.parser.ParserTreeConstants;
import org.apache.commons.jexl3.parser.StringProvider;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.visitors.validate.ASTValidator;
import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;
import datawave.test.JexlNodeAssert;

public class TreeFlatteningRebuildingVisitorTest {

    private final ASTValidator validator = new ASTValidator();

    @Test
    public void dontFlattenASTDelayedPredicateAndTest() throws Exception {
        String query = "((_Delayed_ = true) && (GEO == '1f36c71c71c71c71c7' && (WKT_BYTE_LENGTH >= '+AE0' && WKT_BYTE_LENGTH < '+bE8'))) && GEO >= '1f36c71c71c71c71c7\uDBFF\uDFFF+AE0' && GEO < '1f36c71c71c71c71c8\uDBFF\uDFFF+bE8'";
        String expectedAll = "((_Delayed_ = true) && GEO == '1f36c71c71c71c71c7' && WKT_BYTE_LENGTH >= '+AE0' && WKT_BYTE_LENGTH < '+bE8') && GEO >= '1f36c71c71c71c71c7\uDBFF\uDFFF+AE0' && GEO < '1f36c71c71c71c71c8\uDBFF\uDFFF+bE8'";
        assertResult(query, expectedAll, query);
    }

    @Test
    public void testSingleTermExtraParens() throws ParseException {
        String original = "(((a)))";
        String expected = "a";
        assertResult(original, expected, original);
    }

    @Test
    public void testConjunctionExtraParens() throws ParseException {
        String original = "a && (((((b)))))";
        String expected = "a && b";
        assertResult(original, expected, original);
    }

    @Test
    public void testDisjunctionExtraParens() throws ParseException {
        String original = "a || (((((b)))))";
        String expected = "a || b";
        assertResult(original, expected, original);
    }

    @Test
    public void testConjunction() throws ParseException {
        String original = "a && (b && c)";
        String expected = "a && b && c";
        assertResult(expected, expected, original);
    }

    @Test
    public void testDisjunction() throws ParseException {
        String original = "a || (b || c)";
        String expected = "a || b || c";
        assertResult(expected, expected, original);
    }

    @Test
    public void testConjunctionWithNestedExtraParens() throws ParseException {
        String original = "a && ((b && c || d || e))";
        String expected = "a && (((b && c) || d || e))";
        String expectedAll = "a && ((b && c) || d || e)";
        assertResult(expected, expectedAll, original);
    }

    @Test
    public void testDisjunctionWithNestedExtraParens() throws ParseException {
        String original = "a || ((b && c || d || e))";
        String expected = "a || (b && c) || d || e";
        assertResult(expected, expected, original);
    }

    @Test
    public void testRange() throws ParseException {
        String original = "(a > 1 && a < 5)";
        String expected = "a > 1 && a < 5";
        assertResult(original, expected, original);
    }

    @Test
    public void testRangeWithExtraParens() throws ParseException {
        String original = "(((((a > 1 && a < 5)))))";
        String expected = "a > 1 && a < 5";
        assertResult(original, expected, original);
    }

    @Test
    public void testDisjunctionOfTwoRanges() throws ParseException {
        String original = "(a > 1 && a < 5) || (b > 1 && b < 5)";
        assertResult(original, original, original);
    }

    @Test
    public void testDisjunctionOfTwoRangesWithExtraParens() throws ParseException {
        String original = "(((((a > 1 && a < 5))))) || ((b > 1 && b < 5))";
        String expected = "(a > 1 && a < 5) || (b > 1 && b < 5)";
        assertResult(original, expected, original);
    }

    @Test
    public void testNegation() throws ParseException {
        String original = "! ! ! ! a";
        String expected = "!!!!a";
        assertResult(expected, expected, original);
    }

    @Test
    public void testNestedNegation() throws ParseException {
        String original = "a || !((b && c))";
        String expected = "a || !(b && c)";
        assertResult(original, expected, original);
    }

    /*
     * Test cases where no change is expected
     */
    @Test
    public void testFlattenWithNoChange() throws ParseException {
        String original = "a && b && c && d && (e || f || g || h)";
        assertResult(original, original, original);

        original = "a && b && c || d";
        String expected = "(a && b && c) || d";
        assertResult(expected, expected, original);

        original = "a && b && (c || d)";
        assertResult(original, original, original);

        original = "a && b && (b && a || (d && c && a))";
        expected = "a && b && ((b && a) || (d && c && a))";
        assertResult(expected, expected, original);
    }

    @Test
    public void flattenASTDelayedPredicateOrTest() throws Exception {
        String query = "((_Delayed_ = true) || (GEO == '1f36c71c71c71c71c7' && (WKT_BYTE_LENGTH >= '+AE0' && WKT_BYTE_LENGTH < '+bE8'))) || GEO >= '1f36c71c71c71c71c7\uDBFF\uDFFF+AE0' || GEO < '1f36c71c71c71c71c8\uDBFF\uDFFF+bE8'";
        String expected = "(_Delayed_ = true) || (GEO == '1f36c71c71c71c71c7' && (WKT_BYTE_LENGTH >= '+AE0' && WKT_BYTE_LENGTH < '+bE8')) || GEO >= '1f36c71c71c71c71c7\uDBFF\uDFFF+AE0' || GEO < '1f36c71c71c71c71c8\uDBFF\uDFFF+bE8'";
        String expectedAll = "(_Delayed_ = true) || (GEO == '1f36c71c71c71c71c7' && WKT_BYTE_LENGTH >= '+AE0' && WKT_BYTE_LENGTH < '+bE8') || GEO >= '1f36c71c71c71c71c7\uDBFF\uDFFF+AE0' || GEO < '1f36c71c71c71c71c8\uDBFF\uDFFF+bE8'";
        assertResult(expected, expectedAll, query);
    }

    @Test
    public void depthNoStackTraceOrTest() throws Exception {
        final int numTerms = 10000;
        final StringBuilder sb = new StringBuilder(13 * numTerms); // 13 == "abc_" + 5 + " OR "
        sb.append("abc_").append(StringUtils.leftPad(Integer.toString(numTerms, 10), 5, '0'));
        for (int i = 2; i <= numTerms; i++) {
            sb.append(" OR ").append(i);
        }
        assertNotNull(TreeFlatteningRebuildingVisitor.flattenAll(
                        new Parser(new StringProvider(";")).parse(null, jexlFeatures(), new LuceneToJexlQueryParser().parse(sb.toString()).toString(), null)));
    }

    @Test
    public void multipleNestingTest() throws Exception {
        String query = "((a && (b && (c && d))) || b || (c || d || e || (f || g || (h || i || (((j || k)))))))";
        String expected = "((a && b && c && d) || b || c || d || e || f || g || h || i || j || k)";
        String expectedAll = "(a && b && c && d) || b || c || d || e || f || g || h || i || j || k";
        assertResult(expected, expectedAll, query);
    }

    @Test
    public void multipleNestingMixedOpsTest() throws Exception {
        String query = "((a && (b && ((c1 || (c2 || c3)) && d))) || b || (c || d || e || (f || g || ((h1 && (h2 && h3)) || i || (((j || k)))))))";
        String expected = "((a && b && (c1 || c2 || c3) && d) || b || c || d || e || f || g || (h1 && h2 && h3) || i || j || k)";
        String expectedAll = "(a && b && (c1 || c2 || c3) && d) || b || c || d || e || f || g || (h1 && h2 && h3) || i || j || k";
        assertResult(expected, expectedAll, query);
    }

    @Test
    public void singleChildAndOrTest() {
        JexlNode eqNode = JexlNodeFactory.buildEQNode("FIELD", "value");
        JexlNode and1 = JexlNodeFactory.createAndNode(Collections.singleton(eqNode));
        JexlNode or1 = JexlNodeFactory.createOrNode(Collections.singleton(and1));

        JexlNode flattened = TreeFlatteningRebuildingVisitor.flatten(or1);

        assertEquals(ASTEQNode.class, flattened.getClass());
        assertEquals(2, flattened.jjtGetNumChildren());
        assertEquals(JexlStringBuildingVisitor.buildQuery(eqNode), JexlStringBuildingVisitor.buildQuery(flattened));
    }

    @Test
    public void testWrappedUnionWithSingleChild() throws ParseException {
        // have to build this up manually. Creating an Or node via JexlNodeFactory#createOrNode is smart enough
        // to not wrap single children
        JexlNode eq = JexlNodeFactory.buildEQNode("FIELD", "value");

        JexlNode union = new ASTOrNode(ParserTreeConstants.JJTORNODE);
        JexlNodes.setChildren(union, eq);

        JexlNode refExpr = JexlNodes.wrap(union);
        ASTJexlScript script = JexlNodeFactory.createScript(refExpr);
        JexlNode flattened = TreeFlatteningRebuildingVisitor.flatten(script);

        assertEquals("(FIELD == 'value')", JexlStringBuildingVisitor.buildQueryWithoutParse(flattened));
    }

    @Test
    public void testMarker() throws ParseException {
        String query = "((_Bounded_ = true) && (STATE >= 'e' && STATE <= 'r'))";
        String expected = "((_Bounded_ = true) && (STATE >= 'e' && STATE <= 'r'))";
        String expectedAll = "((_Bounded_ = true) && STATE >= 'e' && STATE <= 'r')";
        assertResult(expected, expectedAll, query);

        query = "(CITY == 'london' || CITY == 'london-extra') && ((_Bounded_ = true) && (STATE >= 'e' && STATE <= 'r'))";
        expected = "(CITY == 'london' || CITY == 'london-extra') && ((_Bounded_ = true) && STATE >= 'e' && STATE <= 'r')";
        assertResult(query, expected, query);
    }

    /**
     * Entry point. Asserts the original query string using both the 'flatten' and 'flattenAll' methods.
     *
     * @param expectedFlatten
     *            the expected query after calling 'flatten'
     * @param expectedFlattenAll
     *            the expected query after calling 'flattenAll'
     * @param original
     *            the original query tree
     * @throws ParseException
     *             if one of the input queries fails to parse
     */
    private void assertResult(String expectedFlatten, String expectedFlattenAll, String original) throws ParseException {
        assertFlatten(expectedFlatten, original);
        assertFlatten(expectedFlattenAll, original, true);
    }

    /**
     * Delegates to {@link #assertFlatten(String, String, boolean)}, where the boolean arg is false by default
     *
     * @param expected
     *            the expected query string
     * @param original
     *            the original query string
     * @throws ParseException
     *             if one of the input queries fails to parse
     */
    private void assertFlatten(String expected, String original) throws ParseException {
        assertFlatten(expected, original, false);
    }

    /**
     * Runs 'flatten' or 'flattenAll' and asserts the result against the expected result
     *
     * @param expected
     *            the expected query string after calling 'flatten' or 'flattenAll'
     * @param original
     *            the original query string
     * @param flattenAll
     *            flag indicating if 'flattenAll' should be called
     * @throws ParseException
     *             if one of the input queries fails to parse
     */
    private void assertFlatten(String expected, String original, boolean flattenAll) throws ParseException {
        ASTJexlScript originalScript = JexlASTHelper.parseJexlQuery(original);
        ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery(expected);

        ASTJexlScript flattened;
        if (flattenAll) {
            flattened = TreeFlatteningRebuildingVisitor.flattenAll(originalScript);
        } else {
            flattened = TreeFlatteningRebuildingVisitor.flatten(originalScript);
        }

        JexlNodeAssert.assertThat(flattened).isEqualTo(expectedScript).hasValidLineage();
        JexlNodeAssert.assertThat(originalScript).isEqualTo(original).hasValidLineage();

        try {
            assertTrue(validator.isValid(flattened));
            assertEquals(expected, JexlStringBuildingVisitor.buildQueryWithoutParse(flattened));
        } catch (Exception e) {
            fail("failed additional validation");
        }
    }

}
