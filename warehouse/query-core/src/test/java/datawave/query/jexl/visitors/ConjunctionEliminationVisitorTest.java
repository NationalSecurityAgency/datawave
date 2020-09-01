package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ConjunctionEliminationVisitorTest {
    
    @Test
    public void testSingleTerm() throws ParseException {
        String original = "FOO == 'bar'";
        visitAndValidate(original, original);
    }
    
    @Test
    public void testUniqueConjunctionInTopLevelDisjunction() throws ParseException {
        String original = "(FOO == 'bar' && FOO == 'baz') || FOO == 'zoo'";
        visitAndValidate(original, original);
    }
    
    @Test
    public void testDuplicateInNestedConjunction() throws ParseException {
        String original = "(FOO == 'bar' && FOO == 'baz') || FOO == 'bar'";
        String expected = "FOO == 'bar'";
        visitAndValidate(original, expected);
    }
    
    @Test
    public void testDuplicateConjunctionInWrappedTopLevelDisjunction() throws ParseException {
        String original = "((FOO == 'bar' && FOO == 'baz') || FOO == 'bar')";
        String expected = "(FOO == 'bar')";
        visitAndValidate(original, expected);
    }
    
    @Test
    public void testDuplicateNestedConjunction() throws ParseException {
        String original = "((FOO == 'baz' && FOO == 'zoo') && FOO == 'bar') || (FOO == 'baz' && FOO == 'zoo')";
        String expected = "FOO == 'baz' && FOO == 'zoo'";
        visitAndValidate(original, expected);
    }
    
    @Test
    public void testDuplicateNestedConjunctionInWrappedTopLevelDisjunction() throws ParseException {
        String original = "((FOO == 'baz' && FOO == 'zoo') && FOO == 'bar') || (FOO == 'baz' && FOO == 'zoo')";
        String expected = "FOO == 'baz' && FOO == 'zoo'";
        visitAndValidate(original, expected);
    }
    
    @Test
    public void testMultipleNestedConjunctionsWithDuplicate() throws ParseException {
        String original = "((FOO == 'baz' && FOO == 'zoo') && (FOO == 'bar' && FOO == 'dab')) || (FOO == 'baz' && FOO == 'zoo')";
        String expected = "FOO == 'baz' && FOO == 'zoo'";
        visitAndValidate(original, expected);
    }
    
    @Test
    public void testMultipleNestedConjunctionsWithDifferentlyOrderedDuplicate() throws ParseException {
        String original = "((FOO == 'baz' && FOO == 'zoo') && (FOO == 'bar' && FOO == 'dab')) || (FOO == 'zoo' && FOO == 'baz')";
        String expected = "FOO == 'zoo' && FOO == 'baz'";
        visitAndValidate(original, expected);
    }
    
    @Test
    public void testDuplicateConjunctionWithUniqueNestedDisjunction() throws ParseException {
        String original = "((FOO == 'bar' && FOO == 'baz') || FOO == 'zoo') || FOO == 'bar'";
        visitAndValidate(original, original);
    }
    
    @Test
    public void testDuplicateConjunctionWithWrappedUniqueNestedDisjunction() throws ParseException {
        String original = "(FOO == 'bar' && (FOO == 'baz' || FOO == 'zoo')) || FOO == 'bar'";
        String expected = "FOO == 'bar'";
        visitAndValidate(original, expected);
    }
    
    @Test
    public void testDuplicateDisjunctionInNestedConjunction() throws ParseException {
        String original = "((FOO == 'bar' || FOO == 'baz') && FOO == 'zoo') || (FOO == 'bar' || FOO == 'baz')";
        String expected = "FOO == 'bar' || FOO == 'baz'";
        visitAndValidate(original, expected);
    }
    
    private void visitAndValidate(String original, String expected) throws ParseException {
        ASTJexlScript originalScript = JexlASTHelper.parseJexlQuery(original);
        ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery(expected);
        
        // Remove duplicate terms from within expressions.
        ASTJexlScript visitedScript = ConjunctionEliminationVisitor.optimize(originalScript);
        
        // Visited query should match expected
        assertEquals(expected, JexlStringBuildingVisitor.buildQuery(visitedScript));
        assertTrue(TreeEqualityVisitor.isEqual(expectedScript, visitedScript, new TreeEqualityVisitor.Reason()));
    }
}
