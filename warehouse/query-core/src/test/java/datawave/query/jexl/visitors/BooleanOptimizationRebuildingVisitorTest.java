package datawave.query.jexl.visitors;

import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Test;

import static datawave.query.jexl.JexlASTHelper.parseJexlQuery;
import static org.junit.Assert.assertEquals;

public class BooleanOptimizationRebuildingVisitorTest {
    
    private BooleanOptimizationRebuildingVisitor visitor = new BooleanOptimizationRebuildingVisitor();
    
    @Test
    public void testConjunction() throws ParseException {
        String original = "a && b && c";
        optimize(original, original, false, false);
    }
    
    @Test
    public void testConjunctionWithFlatten() throws ParseException {
        String original = "a && b && c";
        optimize(original, original, false, true);
    }
    
    @Test
    public void testDisjunction() throws ParseException {
        String original = "a || b || c";
        optimize(original, original, true, false);
    }
    
    @Test
    public void testDisjunctionWithFlatten() throws ParseException {
        String original = "a || b || c";
        optimize(original, original, true, true);
    }
    
    @Test
    public void testChildDisjunction() throws ParseException {
        String original = "a && b || c";
        String expected = "(a && b) || c";
        optimize(original, expected, true, false);
    }
    
    @Test
    public void testDistributeOrTerms() throws ParseException {
        String original = "a && b && c && d && (e || f)";
        String expected = "(a && b && c && d && e) || (a && b && c && d && f)";
        // The OR node's terms are distributed throughout the AND nodes
        optimize(original, expected, true, false);
    }
    
    @Test
    public void testDistributeOrTermsWithFlatten() throws ParseException {
        String original = "(a || b) && (c || d) && (e || f)";
        String expected = "((c || d) && (e || f) && a) || ((c || d) && (e || f) && b)";
        // The OR node's terms are distributed throughout the AND nodes
        optimize(original, expected, true, true);
    }
    
    @Test
    public void testDistributeLargestOrTerms() throws ParseException {
        String original = "(a || b) && (c || d) && (e || f || g)";
        String expected = "((c || d) && (e || f || g) && a) || ((c || d) && (e || f || g) && b)";
        // Without flatten the first OR node's terms are distributed throughout the query.
        optimize(original, expected, true, false);
    }
    
    @Test
    public void testDistributeLargestOrTermsWithFlatten() throws ParseException {
        String original = "(a || b) && (c || d) && (e || f || g)";
        String expected = "((c || d) && (a || b) && e) || ((c || d) && (a || b) && f) || ((c || d) && (a || b) && g)";
        // With flatten the largest OR node's terms are distributed throughout the query.
        optimize(original, expected, true, true);
    }
    
    private void optimize(String original, String expected, boolean hasChildOr, boolean withFlatten) throws ParseException {
        
        ASTJexlScript expectedScript = parseJexlQuery(expected);
        String expectedQuery = JexlStringBuildingVisitor.buildQuery(expectedScript);
        
        ASTJexlScript originalScript = parseJexlQuery(original);
        if (withFlatten) {
            originalScript = TreeFlatteningRebuildingVisitor.flatten(originalScript);
        }
        ASTJexlScript resultScript = BooleanOptimizationRebuildingVisitor.optimize(originalScript);
        String resultQuery = JexlStringBuildingVisitor.buildQuery(resultScript);
        
        assertEquals(expectedQuery, resultQuery);
        assertEquals(hasChildOr, visitor.hasChildOr(resultScript));
    }
}
