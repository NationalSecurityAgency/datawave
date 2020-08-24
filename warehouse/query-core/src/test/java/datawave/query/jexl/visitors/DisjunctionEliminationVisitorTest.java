package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class DisjunctionEliminationVisitorTest {

    @Test
    public void testSingleTerm() throws ParseException {
        String original = "FOO == 'bar'";
        visitAndValidate(original, original);
    }
    
    @Test
    public void testUniqueDisjunctionInTopLevelConjunction() throws ParseException {
        String original = "(FOO == 'bar' || FOO == 'baz') && FOO == 'zoo'";
        visitAndValidate(original, original);
    }
    
    @Test
    public void testDuplicateInNestedDisjunction() throws ParseException {
        String original = "(FOO == 'bar' || FOO == 'baz') && FOO == 'bar'";
        String expected = "FOO == 'bar'";
        visitAndValidate(original, expected);
    }
    
    @Test
    public void testDuplicateDisjunctionInWrappedTopLevelConjunction() throws ParseException {
        String original = "((FOO == 'bar' || FOO == 'baz') && FOO == 'bar')";
        String expected = "(FOO == 'bar')";
        visitAndValidate(original, expected);
    }
    
    @Test
    public void testDuplicateNestedDisjunction() throws ParseException {
        String original = "((FOO == 'baz' && FOO == 'zoo') || FOO == 'bar') && (FOO == 'baz' && FOO == 'zoo')";
        String expected = "FOO == 'baz' && FOO == 'zoo'";
        visitAndValidate(original, expected);
    }
    
    @Test
    public void testDuplicateNestedDisjunctionInWrappedTopLevelConjunction() throws ParseException {
        String original = "((FOO == 'baz' && FOO == 'zoo') || FOO == 'bar') && (FOO == 'baz' && FOO == 'zoo')";
        String expected = "FOO == 'baz' && FOO == 'zoo'";
        visitAndValidate(original, expected);
    }
    
    @Test
    public void testMultipleNestedDisjunctionsWithDuplicate() throws ParseException {
        String original = "((FOO == 'baz' || FOO == 'zoo') || (FOO == 'bar' || FOO == 'dab')) && (FOO == 'baz' || FOO == 'zoo')";
        String expected = "FOO == 'baz' || FOO == 'zoo'";
        visitAndValidate(original, expected);
    }
    
    @Test
    public void testMultipleNestedDisjunctionsWithDifferentlyOrderedDuplicate() throws ParseException {
        String original = "((FOO == 'baz' || FOO == 'zoo') || (FOO == 'bar' || FOO == 'dab')) && (FOO == 'zoo' || FOO == 'baz')";
        String expected = "FOO == 'zoo' || FOO == 'baz'";
        visitAndValidate(original, expected);
    }
    
    @Test
    public void testDuplicateDisjunctionWithUniqueNestedConjunction() throws ParseException {
        String original = "((FOO == 'bar' && FOO == 'baz') || FOO == 'zoo') || FOO == 'bar'";
        visitAndValidate(original, original);
    }
    
    @Test
    public void testDuplicateDisjunctionWithWrappedUniqueNestedConjunction() throws ParseException {
        String original = "(FOO == 'bar' || (FOO == 'baz' && FOO == 'zoo')) && FOO == 'bar'";
        String expected = "FOO == 'bar'";
        visitAndValidate(original, expected);
    }
    
    @Test
    public void testDuplicateConjunctionWithinNestedDisjunction() throws ParseException {
        String original = "((FOO == 'bar' && FOO == 'baz') || FOO == 'zoo') && (FOO == 'bar' && FOO == 'baz')";
        String expected = "FOO == 'bar' && FOO == 'baz'";
        visitAndValidate(original, expected);
    }
    
    private void visitAndValidate(String original, String expected) throws ParseException {
        ASTJexlScript originalScript = JexlASTHelper.parseJexlQuery(original);
        ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery(expected);
        
        // Remove duplicate terms from within expressions.
        ASTJexlScript visitedScript = DisjunctionEliminationVisitor.optimize(originalScript);
        
        // Visited query should match expected
        assertEquals(expected, JexlStringBuildingVisitor.buildQuery(visitedScript));
        assertTrue(TreeEqualityVisitor.isEqual(expectedScript, visitedScript, new TreeEqualityVisitor.Reason()));
    }
}
