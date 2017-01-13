package nsa.datawave.query.rewrite.jexl.visitors;

import nsa.datawave.query.rewrite.jexl.JexlASTHelper;

import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.junit.Assert;
import org.junit.Test;

public class HasTopLevelNegationVisitorTest {
    
    @Test
    public void test() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("FOO == 'bar'");
        Assert.assertFalse(HasTopLevelNegationVisitor.hasTopLevelNegation(script));
    }
    
    @Test
    public void test1() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("FOO != 'bar'");
        Assert.assertTrue(HasTopLevelNegationVisitor.hasTopLevelNegation(script));
    }
    
    @Test
    public void test2() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("(FOO == 'bar')");
        Assert.assertFalse(HasTopLevelNegationVisitor.hasTopLevelNegation(script));
    }
    
    @Test
    public void test3() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("(FOO != 'bar')");
        Assert.assertTrue(HasTopLevelNegationVisitor.hasTopLevelNegation(script));
    }
    
    @Test
    public void test4() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("FOO == 'bar' && FOO == 'bar'");
        Assert.assertFalse(HasTopLevelNegationVisitor.hasTopLevelNegation(script));
    }
    
    @Test
    public void test5() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("FOO == 'bar' && FOO != 'bar'");
        Assert.assertTrue(HasTopLevelNegationVisitor.hasTopLevelNegation(script));
    }
    
    @Test
    public void test6() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("FOO == 'bar' || FOO == 'bar'");
        Assert.assertFalse(HasTopLevelNegationVisitor.hasTopLevelNegation(script));
    }
    
    @Test
    public void test7() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("(FOO == 'bar' || FOO != 'bar')");
        Assert.assertTrue(HasTopLevelNegationVisitor.hasTopLevelNegation(script));
    }
    
    @Test
    public void test8() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("(FOO == 'bar' && FOO == 'bar')");
        Assert.assertFalse(HasTopLevelNegationVisitor.hasTopLevelNegation(script));
    }
    
    @Test
    public void test9() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("(FOO == 'bar' && FOO != 'bar')");
        Assert.assertTrue(HasTopLevelNegationVisitor.hasTopLevelNegation(script));
    }
    
    @Test
    public void test10() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("(FOO == 'bar' || FOO == 'bar')");
        Assert.assertFalse(HasTopLevelNegationVisitor.hasTopLevelNegation(script));
    }
    
    @Test
    public void test11() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("(FOO == 'bar' || FOO != 'bar')");
        Assert.assertTrue(HasTopLevelNegationVisitor.hasTopLevelNegation(script));
    }
    
    @Test
    public void test12() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("(FOO == 'bar' && (FOO == 'bar' || FOO != 'bar'))");
        Assert.assertFalse(HasTopLevelNegationVisitor.hasTopLevelNegation(script));
    }
    
    @Test
    public void test13() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("(FOO == 'bar' && (FOO == 'bar' && FOO != 'bar'))");
        Assert.assertTrue(HasTopLevelNegationVisitor.hasTopLevelNegation(script));
    }
}
