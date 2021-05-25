package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.log4j.Logger;
import org.junit.Test;

import static datawave.query.jexl.JexlASTHelper.parseJexlQuery;
import static org.junit.Assert.assertTrue;

public class BooleanOptimizationRebuildingVisitorTest {
    
    private static final Logger log = Logger.getLogger(BooleanOptimizationRebuildingVisitorTest.class);
    
    @Test
    public void testConjunction() throws ParseException {
        String original = "a && b && c";
        assertResult(original, original, false);
    }
    
    @Test
    public void testConjunctionWithFlatten() throws ParseException {
        String original = "a && b && c";
        assertResult(original, original, true);
    }
    
    @Test
    public void testDisjunction() throws ParseException {
        String original = "a || b || c";
        assertResult(original, original, false);
    }
    
    @Test
    public void testDisjunctionWithFlatten() throws ParseException {
        String original = "a || b || c";
        assertResult(original, original, true);
    }
    
    @Test
    public void testChildDisjunction() throws ParseException {
        String original = "a && b || c";
        String expected = "(a && b) || c";
        assertResult(original, expected, false);
    }
    
    @Test
    public void testDistributeOrTerms() throws ParseException {
        String original = "a && b && c && d && (e || f)";
        String expected = "(a && b && c && d && e) || (a && b && c && d && f)";
        // The OR node's terms are distributed throughout the AND nodes
        assertResult(original, expected, false);
    }
    
    @Test
    public void testDistributeOrTermsWithFlatten() throws ParseException {
        String original = "(a || b) && (c || d) && (e || f)";
        String expected = "((c || d) && (e || f) && a) || ((c || d) && (e || f) && b)";
        // The OR node's terms are distributed throughout the AND nodes
        assertResult(original, expected, true);
    }
    
    @Test
    public void testDistributeLargestOrTerms() throws ParseException {
        String original = "(a || b) && (c || d) && (e || f || g)";
        String expected = "((c || d) && (e || f || g) && a) || ((c || d) && (e || f || g) && b)";
        // Without flatten the first OR node's terms are distributed throughout the query.
        assertResult(original, expected, false);
    }
    
    @Test
    public void testDistributeLargestOrTermsWithFlatten() throws ParseException {
        String original = "(a || b) && (c || d) && (e || f || g)";
        String expected = "((c || d) && (a || b) && e) || ((c || d) && (a || b) && f) || ((c || d) && (a || b) && g)";
        // With flatten the largest OR node's terms are distributed throughout the query.
        assertResult(original, expected, true);
    }
    
    private void assertResult(String original, String expected, boolean flattenScript) throws ParseException {
        ASTJexlScript originalScript = parseJexlQuery(original);
        if (flattenScript) {
            originalScript = TreeFlatteningRebuildingVisitor.flatten(originalScript);
            original = JexlStringBuildingVisitor.buildQuery(originalScript);
        }
        
        ASTJexlScript resultScript = BooleanOptimizationRebuildingVisitor.optimize(originalScript);
        
        // Verify the resulting script is as expected, and has a valid lineage.
        assertScriptEquality(resultScript, expected);
        assertLineage(resultScript);
        
        // Verify the original script was not modified, and has a valid lineage.
        assertScriptEquality(originalScript, original);
        assertLineage(originalScript);
    }
    
    private void assertScriptEquality(JexlNode actual, String expected) throws ParseException {
        ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery(expected);
        TreeEqualityVisitor.Comparison comparison = TreeEqualityVisitor.checkEquality(expectedScript, actual);
        if (!comparison.isEqual()) {
            log.error("Expected " + PrintingVisitor.formattedQueryString(expectedScript));
            log.error("Actual " + PrintingVisitor.formattedQueryString(actual));
        }
        assertTrue(comparison.getReason(), comparison.isEqual());
    }
    
    private void assertLineage(JexlNode node) {
        assertTrue(JexlASTHelper.validateLineage(node, true));
    }
}
