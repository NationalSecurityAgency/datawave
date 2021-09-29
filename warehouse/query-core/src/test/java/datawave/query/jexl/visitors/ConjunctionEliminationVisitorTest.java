package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import datawave.test.JexlNodeAssert;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Test;

public class ConjunctionEliminationVisitorTest {
    
    // A is not reducible.
    @Test
    public void testSingleTerm() throws ParseException {
        String original = "FOO == 'bar'";
        visitAndValidate(original, original);
    }
    
    // (A && B) || C is not reducible.
    @Test
    public void testRelevantConjunctionInTopLevelDisjunction() throws ParseException {
        String original = "(FOO == 'bar' && FOO == 'baz') || FOO == 'zoo'";
        visitAndValidate(original, original);
    }
    
    // ((A || B) && C) || A is not reducible.
    @Test
    public void testRelevantConjunctionWithUniqueNestedDisjunctionTerm() throws ParseException {
        String original = "((FOO == 'bar' || PET == 'white') && FOO == 'baz') || FOO == 'bar'";
        String expected = "((FOO == 'bar' || PET == 'white') && FOO == 'baz') || FOO == 'bar'";
        visitAndValidate(original, expected);
    }
    
    // (A && B) || A reduces to A
    @Test
    public void testRedundantConjunction() throws ParseException {
        String original = "(FOO == 'bar' && FOO == 'baz') || FOO == 'bar'";
        String expected = "FOO == 'bar'";
        visitAndValidate(original, expected);
    }
    
    // ((A && B) || A) reduces to (A)
    @Test
    public void testRedundantConjunctionInWrappedTopLevelDisjunction() throws ParseException {
        String original = "((FOO == 'bar' && FOO == 'baz') || FOO == 'bar')";
        String expected = "(FOO == 'bar')";
        visitAndValidate(original, expected);
    }
    
    // ((A && C) && B) || (A && C) reduces to (A && C)
    @Test
    public void testRedundantNestedConjunction() throws ParseException {
        String original = "((FOO == 'baz' && FOO == 'zoo') && FOO == 'bar') || (FOO == 'baz' && FOO == 'zoo')";
        String expected = "(FOO == 'baz' && FOO == 'zoo')";
        visitAndValidate(original, expected);
    }
    
    // (((A && C) && B) || (A && C)) reduces to (A && C)
    @Test
    public void testRedundantNestedConjunctionInWrappedTopLevelDisjunction() throws ParseException {
        String original = "(((FOO == 'baz' && FOO == 'zoo') && FOO == 'bar') || (FOO == 'baz' && FOO == 'zoo'))";
        String expected = "((FOO == 'baz' && FOO == 'zoo'))";
        visitAndValidate(original, expected);
    }
    
    // ((A && B) && (C && D)) || (A && B) reduces to (A && B)
    @Test
    public void testMultipleRedundantNestedConjunctions() throws ParseException {
        String original = "((FOO == 'baz' && FOO == 'zoo') && (FOO == 'bar' && FOO == 'dab')) || (FOO == 'baz' && FOO == 'zoo')";
        String expected = "(FOO == 'baz' && FOO == 'zoo')";
        visitAndValidate(original, expected);
    }
    
    // ((A && B) && (C && D)) || (B && A) reduces to (B && A)
    @Test
    public void testMultipleRedundantNestedConjunctionsWithDifferentlyOrderedTerms() throws ParseException {
        String original = "((FOO == 'baz' && FOO == 'zoo') && (FOO == 'bar' && FOO == 'dab')) || (FOO == 'zoo' && FOO == 'baz')";
        String expected = "(FOO == 'zoo' && FOO == 'baz')";
        visitAndValidate(original, expected);
    }
    
    // (A && (B || C)) || A reduces to A
    @Test
    public void testRedundantConjunctionWithNestedUniqueDisjunction() throws ParseException {
        String original = "(FOO == 'bar' && (FOO == 'baz' || FOO == 'zoo')) || FOO == 'bar'";
        String expected = "FOO == 'bar'";
        visitAndValidate(original, expected);
    }
    
    // ((A || B) && C) || A || B reduces to A || B
    @Test
    public void testRedundantConjunctionWithNestedDuplicateDisjunction() throws ParseException {
        String original = "((FOO == 'bar' || FOO == 'baz') && FOO == 'zoo') || FOO == 'bar' || FOO == 'baz'";
        String expected = "FOO == 'bar' || FOO == 'baz'";
        visitAndValidate(original, expected);
    }
    
    // (A && (B || C)) || D || A reduces to D || A
    @Test
    public void testRedundantConjunctionWithRedundantNestedDisjunctionTerm() throws ParseException {
        String original = "(FOO == 'bar' && (FOO == 'baz' || FOO == 'zoo')) || PET == 'fluffy' || FOO == 'bar'";
        String expected = "PET == 'fluffy' || FOO == 'bar'";
        visitAndValidate(original, expected);
    }
    
    // ((A && B) || A) && (C || D) reduces to (A) && (C || D)
    @Test
    public void testRedundantConjunctionWithinTopLevelConjunction() throws ParseException {
        String original = "((FOO == 'bar' && PET == 'fluffy') || FOO == 'bar') && (PET == 'short' || FOO == 'zoo')";
        String expected = "(FOO == 'bar') && (PET == 'short' || FOO == 'zoo')";
        visitAndValidate(original, expected);
    }
    
    // (A && B && C) || A reduces to A
    @Test
    public void testRedundantConjunctionWithThreeTerms() throws ParseException {
        String original = "(FOO == 'bar' && PET == 'fluffy' && VET == 'amyr') || FOO == 'bar'";
        String expected = "FOO == 'bar'";
        visitAndValidate(original, expected);
    }
    
    private void visitAndValidate(String original, String expected) throws ParseException {
        ASTJexlScript originalScript = JexlASTHelper.parseJexlQuery(original);
        
        // Remove duplicate terms from within expressions.
        ASTJexlScript visitedScript = ConjunctionEliminationVisitor.optimize(originalScript);
        
        // Verify the script is as expected, and has a valid lineage.
        JexlNodeAssert.assertThat(visitedScript).isEqualTo(expected).hasValidLineage();
        
        // Verify the original script was not modified, and still has a valid lineage.
        JexlNodeAssert.assertThat(originalScript).isEqualTo(original).hasValidLineage();
    }
}
