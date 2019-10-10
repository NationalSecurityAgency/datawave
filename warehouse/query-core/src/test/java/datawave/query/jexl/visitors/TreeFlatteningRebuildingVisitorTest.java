package datawave.query.jexl.visitors;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.commons.jexl2.parser.Parser;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.StringReader;

public class TreeFlatteningRebuildingVisitorTest {
    
    @Test
    public void dontFlattenASTDelayedPredicateAndTest() throws Exception {
        String query = "((ASTDelayedPredicate = true) && (GEO == '1f36c71c71c71c71c7' && (WKT_BYTE_LENGTH >= '+AE0' && WKT_BYTE_LENGTH < '+bE8'))) && GEO >= '1f36c71c71c71c71c7\uDBFF\uDFFF+AE0' && GEO < '1f36c71c71c71c71c8\uDBFF\uDFFF+bE8'";
        JexlNode node = TreeFlatteningRebuildingVisitor.flatten(JexlASTHelper.parseJexlQuery(query));
        assertEquals(query, JexlStringBuildingVisitor.buildQuery(node));
    }
    
    @Test
    public void testSingleTermExtraParens() throws ParseException {
        String original = "(((a)))";
        String expected = "a";
        testFlatten(expected, original);
    }
    
    @Test
    public void testConjunctionExtraParens() throws ParseException {
        String original = "a && (((((b)))))";
        String expected = "a && b";
        testFlatten(expected, original);
    }
    
    @Test
    public void testDisjunctionExtraParens() throws ParseException {
        String original = "a || (((((b)))))";
        String expected = "a || b";
        testFlatten(expected, original);
    }
    
    @Test
    public void testConjunction() throws ParseException {
        String original = "a && (b && c)";
        String expected = "a && b && c";
        testFlatten(expected, original);
    }
    
    @Test
    public void testDisjunction() throws ParseException {
        String original = "a || (b || c)";
        String expected = "a || b || c";
        testFlatten(expected, original);
    }
    
    @Test
    public void testConjunctionWithNestedExtraParens() throws ParseException {
        String original = "a && ((b && c || d || e))";
        String expected = "a && (b && c || d || e)";
        testFlatten(expected, original);
    }
    
    @Test
    public void testDisjunctionWithNestedExtraParens() throws ParseException {
        String original = "a || ((b && c || d || e))";
        String expected = "a || (b && c) || d || e";
        testFlatten(expected, original);
    }
    
    @Test
    public void testRange() throws ParseException {
        String original = "(a > 1 && a < 5)";
        String expected = "a > 1 && a < 5";
        testFlatten(expected, original);
    }
    
    @Test
    public void testRangeWithExtraParens() throws ParseException {
        String original = "(((((a > 1 && a < 5)))))";
        String expected = "a > 1 && a < 5";
        testFlatten(expected, original);
    }
    
    @Test
    public void testDisjunctionOfTwoRanges() throws ParseException {
        String original = "(a > 1 && a < 5) || (b > 1 && b < 5)";
        testFlatten(original, original);
    }
    
    @Test
    public void testDisjunctionOfTwoRangesWithExtraParens() throws ParseException {
        String original = "(((((a > 1 && a < 5))))) || ((b > 1 && b < 5))";
        String expected = "(a > 1 && a < 5) || (b > 1 && b < 5)";
        testFlatten(expected, original);
    }
    
    @Test
    public void testNegation() throws ParseException {
        String original = "! ! ! ! a";
        String expected = "!!!!a";
        testFlatten(expected, original);
    }
    
    @Test
    public void testNestedNegation() throws ParseException {
        String original = "a || !((b && c))";
        String expected = "a || !(b && c)";
        testFlatten(expected, original);
    }
    
    /*
     * Test cases where no change is expected
     */
    @Test
    public void testFlattenWithNoChange() throws ParseException {
        String original = "a && b && c && d && (e || f || g || h)";
        testFlatten(original, original);
        
        original = "a && b && c || d";
        testFlatten(original, original);
        
        original = "a && b && (c || d)";
        testFlatten(original, original);
        
        original = "a && b && (b && a || (d && c && a))";
        testFlatten(original, original);
    }
    
    private void testFlatten(String expected, String original) throws ParseException {
        ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery(expected);
        ASTJexlScript originalScript = JexlASTHelper.parseJexlQuery(original);
        
        ASTJexlScript flattened = TreeFlatteningRebuildingVisitor.flattenAll(originalScript);
        
        String expectedQueryString = JexlStringBuildingVisitor.buildQueryWithoutParse(expectedScript);
        String originalQueryString = JexlStringBuildingVisitor.buildQueryWithoutParse(flattened);
        assertEquals(expectedQueryString, originalQueryString);
        
        assertTrue(TreeEqualityVisitor.isEqual(expectedScript, flattened, new TreeEqualityVisitor.Reason()));
    }
    
    @Test
    public void flattenASTDelayedPredicateOrTest() throws Exception {
        String query = "((ASTDelayedPredicate = true) || (GEO == '1f36c71c71c71c71c7' && (WKT_BYTE_LENGTH >= '+AE0' && WKT_BYTE_LENGTH < '+bE8'))) || GEO >= '1f36c71c71c71c71c7\uDBFF\uDFFF+AE0' || GEO < '1f36c71c71c71c71c8\uDBFF\uDFFF+bE8'";
        String expected = "(ASTDelayedPredicate = true) || (GEO == '1f36c71c71c71c71c7' && (WKT_BYTE_LENGTH >= '+AE0' && WKT_BYTE_LENGTH < '+bE8')) || GEO >= '1f36c71c71c71c71c7\uDBFF\uDFFF+AE0' || GEO < '1f36c71c71c71c71c8\uDBFF\uDFFF+bE8'";
        JexlNode node = TreeFlatteningRebuildingVisitor.flatten(JexlASTHelper.parseJexlQuery(query));
        Assert.assertEquals(expected, JexlStringBuildingVisitor.buildQuery(node));
    }
    
    @Test
    public void depthNoStackTraceOrTest() throws Exception {
        final int numTerms = 10_000;
        final StringBuilder sb = new StringBuilder(13 * numTerms); // 13 == "abc_" + 5 + " OR "
        sb.append("abc_" + StringUtils.leftPad(Integer.toString(numTerms, 10), 5, '0'));
        for (int i = 2; i <= numTerms; i++) {
            sb.append(" OR " + i);
        }
        Assert.assertNotNull(TreeFlatteningRebuildingVisitor.flattenAll(new Parser(new StringReader(";")).parse(new StringReader(new LuceneToJexlQueryParser()
                        .parse(sb.toString()).toString()), null)));
    }
    
    @Test
    public void multipleNestingTest() throws Exception {
        String query = "((a && (b && (c && d))) || b || (c || d || e || (f || g || (h || i || (((j || k)))))))";
        String expected = "((a && b && c && d) || b || c || d || e || f || g || h || i || j || k)";
        JexlNode node = TreeFlatteningRebuildingVisitor.flatten(JexlASTHelper.parseJexlQuery(query));
        Assert.assertEquals(expected, JexlStringBuildingVisitor.buildQuery(node));
    }
    
}
