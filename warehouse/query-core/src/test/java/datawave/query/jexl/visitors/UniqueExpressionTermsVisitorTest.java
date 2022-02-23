package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import datawave.test.JexlNodeAssert;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Test;

public class UniqueExpressionTermsVisitorTest {
    
    @Test
    public void testSingleTerm() throws ParseException {
        String original = "FOO == 'bar'";
        // No change expected
        visitAndValidate(original, original);
    }
    
    @Test
    public void testDuplicateInTopLevelDisjunction() throws ParseException {
        String original = "FOO == 'bar' || FOO == 'bar'";
        String expected = "FOO == 'bar'";
        visitAndValidate(original, expected);
    }
    
    @Test
    public void testDuplicateInWrappedTopLevelDisjunction() throws ParseException {
        String original = "(FOO == 'bar' || FOO == 'bar')";
        String expected = "(FOO == 'bar')";
        visitAndValidate(original, expected);
    }
    
    @Test
    public void testDuplicateInTopLevelConjunction() throws ParseException {
        String original = "FOO == 'bar' && FOO == 'bar'";
        String expected = "FOO == 'bar'";
        visitAndValidate(original, expected);
    }
    
    @Test
    public void testDuplicateInWrappedTopLevelConjunction() throws ParseException {
        String original = "(FOO == 'bar' && FOO == 'bar')";
        String expected = "(FOO == 'bar')";
        visitAndValidate(original, expected);
    }
    
    @Test
    public void testDuplicateInTopLevelConjunctionWithTwoUniqueTerms() throws ParseException {
        String original = "FOO == 'bar' && FOO == 'bar' && FOO == 'baz'";
        String expected = "FOO == 'bar' && FOO == 'baz'";
        visitAndValidate(original, expected);
    }
    
    @Test
    public void testDuplicateInWrappedTopLevelConjunctionWithTwoUniqueTerms() throws ParseException {
        String original = "(FOO == 'bar' && FOO == 'bar' && FOO == 'baz')";
        String expected = "(FOO == 'bar' && FOO == 'baz')";
        visitAndValidate(original, expected);
    }
    
    @Test
    public void testDuplicateInTopLevelDisjunctionWithTwoUniqueTerms() throws ParseException {
        String original = "FOO == 'bar' || FOO == 'baz' || FOO == 'baz'";
        String expected = "FOO == 'bar' || FOO == 'baz'";
        visitAndValidate(original, expected);
    }
    
    @Test
    public void testDuplicateInWrappedTopLevelDisjunctionWithTwoUniqueTerms() throws ParseException {
        String original = "(FOO == 'bar' || FOO == 'baz' || FOO == 'baz')";
        String expected = "(FOO == 'bar' || FOO == 'baz')";
        visitAndValidate(original, expected);
    }
    
    @Test
    public void testDuplicateInNestedConjunction() throws ParseException {
        String original = "FOO == 'bar' || (FOO == 'baz' && FOO == 'baz')";
        String expected = "FOO == 'bar' || (FOO == 'baz')";
        visitAndValidate(original, expected);
    }
    
    @Test
    public void testDuplicateInNestedDisjunction() throws ParseException {
        // All expression terms are unique, no change.
        String original = "FOO == 'bar' && (FOO == 'baz' || FOO == 'baz')";
        String expected = "FOO == 'bar' && (FOO == 'baz')";
        visitAndValidate(original, expected);
    }
    
    @Test
    public void testDuplicateInNestedConjunctionWithThreeUniqueTerms() throws ParseException {
        String original = "FOO == 'bar' || (FOO == 'baz' && FOO == 'baz' && FOO == 'boo')";
        String expected = "FOO == 'bar' || (FOO == 'baz' && FOO == 'boo')";
        visitAndValidate(original, expected);
    }
    
    @Test
    public void testDuplicateInNestedDisjunctionWithThreeUniqueTerms() throws ParseException {
        String original = "FOO == 'bar' && (FOO == 'baz' || FOO == 'baz' || FOO == 'boo')";
        String expected = "FOO == 'bar' && (FOO == 'baz' || FOO == 'boo')";
        visitAndValidate(original, expected);
    }
    
    @Test
    public void testDistributedDuplicateInNestedConjunction() throws ParseException {
        String original = "FOO == 'bar' || (FOO == 'bar' && FOO == 'baz' && FOO == 'baz')";
        String expected = "FOO == 'bar' || (FOO == 'bar' && FOO == 'baz')";
        visitAndValidate(original, expected);
    }
    
    @Test
    public void testDistributedDuplicateInNestedDisjunction() throws ParseException {
        String original = "FOO == 'bar' && (FOO == 'bar' || FOO == 'baz' || FOO == 'baz')";
        String expected = "FOO == 'bar' && (FOO == 'bar' || FOO == 'baz')";
        visitAndValidate(original, expected);
    }
    
    @Test
    public void testDuplicateMirroredDisjunction() throws ParseException {
        String original = "(FOO == 'bar' || FOO == 'bar') && (FOO == 'bar' || FOO == 'bar')";
        String expected = "(FOO == 'bar')";
        visitAndValidate(original, expected);
    }
    
    @Test
    public void testDuplicateWrappedMirroredDisjunction() throws ParseException {
        String original = "((FOO == 'bar' || FOO == 'bar') && (FOO == 'bar' || FOO == 'bar'))";
        String expected = "((FOO == 'bar'))";
        visitAndValidate(original, expected);
    }
    
    @Test
    public void testDuplicateMirroredConjunction() throws ParseException {
        String original = "(FOO == 'bar' && FOO == 'bar') || (FOO == 'bar' && FOO == 'bar')";
        String expected = "(FOO == 'bar')";
        visitAndValidate(original, expected);
    }
    
    @Test
    public void testDuplicateWrappedMirroredConjunction() throws ParseException {
        String original = "((FOO == 'bar' && FOO == 'bar') || (FOO == 'bar' && FOO == 'bar'))";
        String expected = "((FOO == 'bar'))";
        visitAndValidate(original, expected);
    }
    
    @Test
    public void testDuplicateMirroredDisjunctionWithTwoUniqueTerms() throws ParseException {
        String original = "(FOO == 'bar' || FOO == 'bar') && (FOO == 'baz' || FOO == 'baz')";
        String expected = "(FOO == 'bar') && (FOO == 'baz')";
        visitAndValidate(original, expected);
    }
    
    @Test
    public void testDuplicateMirroredConjunctionWithTwoUniqueTerms() throws ParseException {
        String original = "(FOO == 'bar' && FOO == 'bar') || (FOO == 'baz' && FOO == 'baz')";
        String expected = "(FOO == 'bar') || (FOO == 'baz')";
        visitAndValidate(original, expected);
    }
    
    /*
     * Test some non-leaf node conjunctions and disjunctions
     */
    @Test
    public void testDuplicateInNonLeafConjunction() throws ParseException {
        String original = "FOO == 'bar' && FOO == 'bar' && (FOO == 'bar' || FOO == 'baz')";
        String expected = "FOO == 'bar' && (FOO == 'bar' || FOO == 'baz')";
        visitAndValidate(original, expected);
    }
    
    @Test
    public void testDuplicateInWrappedNonLeafConjunction() throws ParseException {
        String original = "(FOO == 'bar' && FOO == 'bar' && (FOO == 'bar' || FOO == 'baz'))";
        String expected = "(FOO == 'bar' && (FOO == 'bar' || FOO == 'baz'))";
        visitAndValidate(original, expected);
    }
    
    @Test
    public void testDuplicateInNonLeafDisjunction() throws ParseException {
        String original = "FOO == 'bar' || FOO == 'bar' || (FOO == 'bar' && FOO == 'baz')";
        String expected = "FOO == 'bar' || (FOO == 'bar' && FOO == 'baz')";
        visitAndValidate(original, expected);
    }
    
    @Test
    public void testDuplicateInWrappedNonLeafDisjunction() throws ParseException {
        String original = "(FOO == 'bar' || FOO == 'bar' || (FOO == 'bar' && FOO == 'baz'))";
        String expected = "(FOO == 'bar' || (FOO == 'bar' && FOO == 'baz'))";
        visitAndValidate(original, expected);
    }
    
    /*
     * Test some distributed duplicates that should not reduce using the UniqueExpressionTermsVisitor
     */
    @Test
    public void testDistributedDuplicateInConjunction() throws ParseException {
        String original = "(FOO == 'bar' && FOO == 'baz') || FOO == 'bar'";
        // No change expected
        visitAndValidate(original, original);
    }
    
    @Test
    public void testDistributedDuplicateInDisjunction() throws ParseException {
        String original = "(FOO == 'bar' || FOO == 'baz') && FOO == 'bar'";
        // No change expected
        visitAndValidate(original, original);
    }
    
    @Test
    public void testDistributedDuplicatesInMirroredConjunctions() throws ParseException {
        String original = "(FOO == 'bar' && FOO == 'baz') || (FOO == 'baz' && FOO == 'bar')";
        String expected = "(FOO == 'bar' && FOO == 'baz')";
        visitAndValidate(original, expected);
    }
    
    @Test
    public void testDistributedDuplicatesInMirroredDisjunctions() throws ParseException {
        String original = "(FOO == 'bar' || FOO == 'baz') && (FOO == 'baz' || FOO == 'bar')";
        String expected = "(FOO == 'bar' || FOO == 'baz')";
        visitAndValidate(original, expected);
    }
    
    @Test
    public void testDuplicateSubTreeConjunction() throws ParseException {
        String original = "(FOO == 'bar' || FOO == 'baz') && (FOO == 'bar' || FOO == 'baz')";
        String expected = "(FOO == 'bar' || FOO == 'baz')";
        visitAndValidate(original, expected);
    }
    
    @Test
    public void testDuplicateSubTreeDisjunction() throws ParseException {
        String original = "(FOO == 'bar' && FOO == 'baz') || (FOO == 'bar' && FOO == 'baz')";
        String expected = "(FOO == 'bar' && FOO == 'baz')";
        visitAndValidate(original, expected);
    }
    
    private void visitAndValidate(String original, String expected) throws ParseException {
        ASTJexlScript originalScript = JexlASTHelper.parseJexlQuery(original);
        
        // Remove duplicate terms from within expressions.
        ASTJexlScript visitedScript = UniqueExpressionTermsVisitor.enforce(originalScript);
        
        // Verify the script is as expected, and has a valid lineage.
        JexlNodeAssert.assertThat(visitedScript).isEqualTo(expected).hasValidLineage();
        
        // Verify the original script was not modified, and still has a valid lineage.
        JexlNodeAssert.assertThat(originalScript).isEqualTo(original).hasValidLineage();
    }
}
