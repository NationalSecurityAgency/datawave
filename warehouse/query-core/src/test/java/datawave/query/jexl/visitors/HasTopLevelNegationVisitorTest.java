package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;

import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.junit.Assert;
import org.junit.Test;

public class HasTopLevelNegationVisitorTest {
    
    @Test
    public void testNoNegations() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("FOO == 'bar'");
        Assert.assertFalse(HasTopLevelNegationVisitor.hasTopLevelNegation(script));
    }
    
    @Test
    public void testNegation() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("FOO != 'bar'");
        Assert.assertTrue(HasTopLevelNegationVisitor.hasTopLevelNegation(script));
    }
    
    @Test
    public void testWrappedNonNegation() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("(FOO == 'bar')");
        Assert.assertFalse(HasTopLevelNegationVisitor.hasTopLevelNegation(script));
    }
    
    @Test
    public void testWrappedNegation() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("(FOO != 'bar')");
        Assert.assertTrue(HasTopLevelNegationVisitor.hasTopLevelNegation(script));
    }
    
    @Test
    public void testConjunctionWithoutNegations() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("FOO == 'bar' && FOO == 'bar'");
        Assert.assertFalse(HasTopLevelNegationVisitor.hasTopLevelNegation(script));
    }
    
    @Test
    public void testConjunctionWithNegation() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("FOO == 'bar' && FOO != 'bar'");
        Assert.assertTrue(HasTopLevelNegationVisitor.hasTopLevelNegation(script));
    }
    
    @Test
    public void testDisjunctionWithoutNegations() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("FOO == 'bar' || FOO == 'bar'");
        Assert.assertFalse(HasTopLevelNegationVisitor.hasTopLevelNegation(script));
    }
    
    @Test
    public void testDisjunctionWithNegation() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("FOO == 'bar' || FOO != 'bar'");
        Assert.assertTrue(HasTopLevelNegationVisitor.hasTopLevelNegation(script));
    }
    
    @Test
    public void testWrappedConjunctionWithoutNegations() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("(FOO == 'bar' && FOO == 'bar')");
        Assert.assertFalse(HasTopLevelNegationVisitor.hasTopLevelNegation(script));
    }
    
    @Test
    public void testWrappedConjunctionWithNegation() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("(FOO == 'bar' && FOO != 'bar')");
        Assert.assertTrue(HasTopLevelNegationVisitor.hasTopLevelNegation(script));
    }
    
    @Test
    public void testWrappedDisjunctionWithoutNegations() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("(FOO == 'bar' || FOO == 'bar')");
        Assert.assertFalse(HasTopLevelNegationVisitor.hasTopLevelNegation(script));
    }
    
    @Test
    public void testWrappedDisjunctionWithNegation() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("(FOO == 'bar' || FOO != 'bar')");
        Assert.assertTrue(HasTopLevelNegationVisitor.hasTopLevelNegation(script));
    }
    
    @Test
    public void testNegationInNestedDisjunction() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("(FOO == 'bar' && (FOO == 'bar' || FOO != 'bar'))");
        Assert.assertFalse(HasTopLevelNegationVisitor.hasTopLevelNegation(script));
    }
    
    @Test
    public void testNegationInNestedConjunction() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("(FOO == 'bar' && (FOO == 'bar' && FOO != 'bar'))");
        Assert.assertTrue(HasTopLevelNegationVisitor.hasTopLevelNegation(script));
    }
}
