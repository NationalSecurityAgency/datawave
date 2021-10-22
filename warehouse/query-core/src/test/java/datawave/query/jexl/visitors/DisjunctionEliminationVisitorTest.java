package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import datawave.test.JexlNodeAssert;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Test;

public class DisjunctionEliminationVisitorTest {
    
    // A is not reducible.
    @Test
    public void testSingleTerm() throws ParseException {
        String original = "FOO == 'bar'";
        visitAndValidate(original, original);
    }
    
    // (A || B) && C is not reducible.
    @Test
    public void testRelevantDisjunctionInTopLevelConjunction() throws ParseException {
        String original = "(FOO == 'bar' || FOO == 'baz') && FOO == 'zoo'";
        visitAndValidate(original, original);
    }
    
    // ((A && C) || B) && A is not reducible.
    @Test
    public void testRelevantDisjunctionWithUniqueNestedConjunctionTerm() throws ParseException {
        String original = "((FOO == 'bar' && PET == 'fluffy') || VET == 'amy') && FOO == 'bar'";
        visitAndValidate(original, original);
    }
    
    // (A || B) && A reduces to A
    @Test
    public void testRedundantNestedDisjunction() throws ParseException {
        String original = "(FOO == 'bar' || FOO == 'baz') && FOO == 'bar'";
        String expected = "FOO == 'bar'";
        visitAndValidate(original, expected);
    }
    
    // ((A || B) && A) reduces to (A).
    @Test
    public void testRedundantDisjunctionInWrappedTopLevelConjunction() throws ParseException {
        String original = "((FOO == 'bar' || FOO == 'baz') && FOO == 'bar')";
        String expected = "(FOO == 'bar')";
        visitAndValidate(original, expected);
    }
    
    // ((A || C) || B) && A reduces to A.
    @Test
    public void testRedundantDisjunctionWithThreeTerms() throws ParseException {
        String original = "((FOO == 'bar' || PET == 'fluffy') || VET == 'amy') && FOO == 'bar'";
        String expected = "FOO == 'bar'";
        visitAndValidate(original, expected);
    }
    
    // ((A && B) || C) && (A && B) reduces to A && B.
    @Test
    public void testRedundantNestedDisjunctionWithMatchingConjunction() throws ParseException {
        String original = "((FOO == 'baz' && FOO == 'zoo') || FOO == 'bar') && (FOO == 'baz' && FOO == 'zoo')";
        String expected = "FOO == 'baz' && FOO == 'zoo'";
        visitAndValidate(original, expected);
    }
    
    // ((A || B) || (C || D)) && (A || B) reduces to (A || B).
    @Test
    public void testMultipleNestedRedundantDisjunctions() throws ParseException {
        String original = "((FOO == 'baz' || FOO == 'zoo') || (FOO == 'bar' || FOO == 'dab')) && (FOO == 'baz' || FOO == 'zoo')";
        String expected = "(FOO == 'baz' || FOO == 'zoo')";
        visitAndValidate(original, expected);
    }
    
    // ((A || B) || (C || D)) && (B || A) reduces to (B || A).
    @Test
    public void testRedundantDisjunctionsWithDifferentlyOrderedTerms() throws ParseException {
        String original = "((FOO == 'baz' || FOO == 'zoo') || (FOO == 'bar' || FOO == 'dab')) && (FOO == 'zoo' || FOO == 'baz')";
        String expected = "(FOO == 'zoo' || FOO == 'baz')";
        visitAndValidate(original, expected);
    }
    
    // (A || (B && C)) && A reduces to A
    @Test
    public void testRedundantDisjunctionWithWrappedUniqueNestedConjunction() throws ParseException {
        String original = "(FOO == 'bar' || (FOO == 'baz' && FOO == 'zoo')) && FOO == 'bar'";
        String expected = "FOO == 'bar'";
        visitAndValidate(original, expected);
    }
    
    // ((A || B) && A) || (C || D) reduces to (A) || (C || D)
    @Test
    public void testRedundantDisjunctionWithinTopLevelDisjunction() throws ParseException {
        String original = "((FOO == 'bar' || PET == 'fluffy') && FOO == 'bar') || (PET == 'short' || FOO == 'zoo')";
        String expected = "(FOO == 'bar') || PET == 'short' || FOO == 'zoo'";
        visitAndValidate(original, expected);
    }
    
    // (A || B) && A && C reduces to A && C
    @Test
    public void testRedundantDisjunctionWithThreeTopLevelTerms() throws ParseException {
        String original = "(FOO == 'bar' || PET == 'fluffy') && FOO == 'bar' && VET == 'amy'";
        String expected = "FOO == 'bar' && VET == 'amy'";
        visitAndValidate(original, expected);
    }
    
    // ((A && B) || D) && (A && B && C) reduces to (A && B && C).
    @Test
    public void testRedundantDisjunctionWithSubsetConjunctionTerms() throws ParseException {
        String original = "((FOO == 'bar' && PET == 'fluffy') || VET == 'amy') && (FOO == 'bar' && PET == 'fluffy' && FOO == 'zoo')";
        String expected = "FOO == 'bar' && PET == 'fluffy' && FOO == 'zoo'";
        visitAndValidate(original, expected);
    }
    
    private void visitAndValidate(String original, String expected) throws ParseException {
        ASTJexlScript originalScript = JexlASTHelper.parseJexlQuery(original);
        
        // Remove duplicate terms from within expressions.
        ASTJexlScript visitedScript = DisjunctionEliminationVisitor.optimize(originalScript);
        
        // Verify the script is as expected, and has a valid lineage.
        JexlNodeAssert.assertThat(visitedScript).isEqualTo(expected).hasValidLineage();
        
        // Verify the original script was not modified, and still has a valid lineage.
        JexlNodeAssert.assertThat(originalScript).isEqualTo(original).hasValidLineage();
    }
}
