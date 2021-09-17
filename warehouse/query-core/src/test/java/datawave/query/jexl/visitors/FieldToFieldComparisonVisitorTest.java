package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;

import datawave.test.JexlNodeAssert;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Test;

public class FieldToFieldComparisonVisitorTest {
    
    @Test
    public void testEQ() throws ParseException {
        assertResult("FOO == BAR", "((_Eval_ = true) && (FOO == BAR))");
        
        assertResult("FOO == 'bar'", "FOO == 'bar'");
        assertResult("(FOO || BAR).min().hashCode() == 0", "(FOO || BAR).min().hashCode() == 0");
    }
    
    @Test
    public void testNE() throws ParseException {
        assertResult("FOO != BAR", "((_Eval_ = true) && (FOO != BAR))");
        
        assertResult("FOO != 'bar'", "FOO != 'bar'");
        assertResult("(FOO || BAR).min().hashCode() != 0", "(FOO || BAR).min().hashCode() != 0");
    }
    
    @Test
    public void testLT() throws ParseException {
        assertResult("FOO < BAR", "((_Eval_ = true) && (FOO < BAR))");
        
        assertResult("FOO < 1", "FOO < 1");
        assertResult("(FOO || BAR).min().hashCode() < 0", "(FOO || BAR).min().hashCode() < 0");
    }
    
    @Test
    public void testLE() throws ParseException {
        assertResult("FOO <= BAR", "((_Eval_ = true) && (FOO <= BAR))");
        
        assertResult("FOO <= 1", "FOO <= 1");
        assertResult("(FOO || BAR).min().hashCode() <= 0", "(FOO || BAR).min().hashCode() <= 0");
    }
    
    @Test
    public void testGT() throws ParseException {
        assertResult("FOO > BAR", "((_Eval_ = true) && (FOO > BAR))");
        
        assertResult("FOO > 1", "FOO > 1");
        assertResult("(FOO || BAR).min().hashCode() > 0", "(FOO || BAR).min().hashCode() > 0");
    }
    
    @Test
    public void testGE() throws ParseException {
        assertResult("FOO >= BAR", "((_Eval_ = true) && (FOO >= BAR))");
        
        assertResult("FOO >= 1", "FOO >= 1");
        assertResult("(FOO || BAR).min().hashCode() >= 0", "(FOO || BAR).min().hashCode() >= 0");
    }
    
    @Test
    public void testRegexOpsAreNotModified() throws ParseException {
        assertResult("(UUID =~ 'C.*?' || UUID =~ 'S.*?')", "(UUID =~ 'C.*?' || UUID =~ 'S.*?')");
        assertResult("(UUID !~ 'C.*?' || UUID !~ 'S.*?')", "(UUID !~ 'C.*?' || UUID !~ 'S.*?')");
    }
    
    @Test(expected = ParseException.class)
    public void testChainedEQThrowsException() throws ParseException {
        JexlASTHelper.parseJexlQuery("FIELD_A == FIELD_B == FIELD_C");
    }
    
    private void assertResult(String original, String expected) throws ParseException {
        ASTJexlScript originalScript = JexlASTHelper.parseJexlQuery(original);
        
        ASTJexlScript visitedScript = FieldToFieldComparisonVisitor.forceEvaluationOnly(originalScript);
        
        // Verify the script is as expected, and has a valid lineage.
        JexlNodeAssert.assertThat(visitedScript).isEqualTo(expected).hasValidLineage();
        
        // Verify the original script was not modified, and still has a valid lineage.
        JexlNodeAssert.assertThat(originalScript).isEqualTo(original).hasValidLineage();
    }
}
