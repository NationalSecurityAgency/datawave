package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;
import datawave.test.JexlNodeAssert;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.commons.jexl2.parser.Parser;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.StringReader;
import java.util.Collections;

public class TreeFlatteningRebuildingVisitorTest {
    
    @Test
    public void dontFlattenASTDelayedPredicateAndTest() throws Exception {
        String query = "((_Delayed_ = true) && (GEO == '1f36c71c71c71c71c7' && (WKT_BYTE_LENGTH >= '+AE0' && WKT_BYTE_LENGTH < '+bE8'))) && GEO >= '1f36c71c71c71c71c7\uDBFF\uDFFF+AE0' && GEO < '1f36c71c71c71c71c8\uDBFF\uDFFF+bE8'";
        assertResult(query, query);
    }
    
    @Test
    public void testSingleTermExtraParens() throws ParseException {
        String original = "(((a)))";
        String expected = "a";
        assertResult(expected, original);
    }
    
    @Test
    public void testConjunctionExtraParens() throws ParseException {
        String original = "a && (((((b)))))";
        String expected = "a && b";
        assertResult(expected, original);
    }
    
    @Test
    public void testDisjunctionExtraParens() throws ParseException {
        String original = "a || (((((b)))))";
        String expected = "a || b";
        assertResult(expected, original);
    }
    
    @Test
    public void testConjunction() throws ParseException {
        String original = "a && (b && c)";
        String expected = "a && b && c";
        assertResult(expected, original);
    }
    
    @Test
    public void testDisjunction() throws ParseException {
        String original = "a || (b || c)";
        String expected = "a || b || c";
        assertResult(expected, original);
    }
    
    @Test
    public void testConjunctionWithNestedExtraParens() throws ParseException {
        String original = "a && ((b && c || d || e))";
        String expected = "a && (b && c || d || e)";
        assertResult(expected, original);
    }
    
    @Test
    public void testDisjunctionWithNestedExtraParens() throws ParseException {
        String original = "a || ((b && c || d || e))";
        String expected = "a || (b && c) || d || e";
        assertResult(expected, original);
    }
    
    @Test
    public void testRange() throws ParseException {
        String original = "(a > 1 && a < 5)";
        String expected = "a > 1 && a < 5";
        assertResult(expected, original);
    }
    
    @Test
    public void testRangeWithExtraParens() throws ParseException {
        String original = "(((((a > 1 && a < 5)))))";
        String expected = "a > 1 && a < 5";
        assertResult(expected, original);
    }
    
    @Test
    public void testDisjunctionOfTwoRanges() throws ParseException {
        String original = "(a > 1 && a < 5) || (b > 1 && b < 5)";
        assertResult(original, original);
    }
    
    @Test
    public void testDisjunctionOfTwoRangesWithExtraParens() throws ParseException {
        String original = "(((((a > 1 && a < 5))))) || ((b > 1 && b < 5))";
        String expected = "(a > 1 && a < 5) || (b > 1 && b < 5)";
        assertResult(expected, original);
    }
    
    @Test
    public void testNegation() throws ParseException {
        String original = "! ! ! ! a";
        String expected = "!!!!a";
        assertResult(expected, original);
    }
    
    @Test
    public void testNestedNegation() throws ParseException {
        String original = "a || !((b && c))";
        String expected = "a || !(b && c)";
        assertResult(expected, original);
    }
    
    /*
     * Test cases where no change is expected
     */
    @Test
    public void testFlattenWithNoChange() throws ParseException {
        String original = "a && b && c && d && (e || f || g || h)";
        assertResult(original, original);
        
        original = "a && b && c || d";
        assertResult(original, original);
        
        original = "a && b && (c || d)";
        assertResult(original, original);
        
        original = "a && b && (b && a || (d && c && a))";
        assertResult(original, original);
    }
    
    @Test
    public void flattenASTDelayedPredicateOrTest() throws Exception {
        String query = "((_Delayed_ = true) || (GEO == '1f36c71c71c71c71c7' && (WKT_BYTE_LENGTH >= '+AE0' && WKT_BYTE_LENGTH < '+bE8'))) || GEO >= '1f36c71c71c71c71c7\uDBFF\uDFFF+AE0' || GEO < '1f36c71c71c71c71c8\uDBFF\uDFFF+bE8'";
        String expected = "(_Delayed_ = true) || (GEO == '1f36c71c71c71c71c7' && (WKT_BYTE_LENGTH >= '+AE0' && WKT_BYTE_LENGTH < '+bE8')) || GEO >= '1f36c71c71c71c71c7\uDBFF\uDFFF+AE0' || GEO < '1f36c71c71c71c71c8\uDBFF\uDFFF+bE8'";
        assertResult(expected, query);
    }
    
    @Test
    public void depthNoStackTraceOrTest() throws Exception {
        final int numTerms = 10000;
        final StringBuilder sb = new StringBuilder(13 * numTerms); // 13 == "abc_" + 5 + " OR "
        sb.append("abc_").append(StringUtils.leftPad(Integer.toString(numTerms, 10), 5, '0'));
        for (int i = 2; i <= numTerms; i++) {
            sb.append(" OR ").append(i);
        }
        Assert.assertNotNull(TreeFlatteningRebuildingVisitor.flattenAll(new Parser(new StringReader(";")).parse(new StringReader(new LuceneToJexlQueryParser()
                        .parse(sb.toString()).toString()), null)));
    }
    
    @Test
    public void multipleNestingTest() throws Exception {
        String query = "((a && (b && (c && d))) || b || (c || d || e || (f || g || (h || i || (((j || k)))))))";
        String expected = "((a && b && c && d) || b || c || d || e || f || g || h || i || j || k)";
        assertResult(expected, query);
    }
    
    @Test
    public void multipleNestingMixedOpsTest() throws Exception {
        String query = "((a && (b && ((c1 || (c2 || c3)) && d))) || b || (c || d || e || (f || g || ((h1 && (h2 && h3)) || i || (((j || k)))))))";
        String expected = "((a && b && (c1 || c2 || c3) && d) || b || c || d || e || f || g || (h1 && h2 && h3) || i || j || k)";
        assertResult(expected, query);
    }
    
    @Test
    public void singleChildAndOrTest() throws Exception {
        JexlNode eqNode = JexlASTHelper.parseJexlQuery("FIELD == 'value'");
        JexlNode and1 = JexlNodeFactory.createUnwrappedAndNode(Collections.singleton(eqNode));
        JexlNode or1 = JexlNodeFactory.createUnwrappedOrNode(Collections.singleton(and1));
        
        JexlNode flattened = TreeFlatteningRebuildingVisitor.flatten(or1);
        
        Assert.assertEquals(ASTJexlScript.class, flattened.getClass());
        Assert.assertEquals(1, flattened.jjtGetNumChildren());
        Assert.assertEquals(ASTEQNode.class, flattened.jjtGetChild(0).getClass());
        Assert.assertEquals(JexlStringBuildingVisitor.buildQuery(eqNode), JexlStringBuildingVisitor.buildQuery(flattened));
    }
    
    private void assertResult(String expected, String original) throws ParseException {
        ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery(expected);
        ASTJexlScript originalScript = JexlASTHelper.parseJexlQuery(original);
        
        ASTJexlScript flattened = TreeFlatteningRebuildingVisitor.flattenAll(originalScript);
        
        JexlNodeAssert.assertThat(flattened).isEqualTo(expectedScript).hasValidLineage();
        JexlNodeAssert.assertThat(originalScript).isEqualTo(original).hasValidLineage();
    }
}
