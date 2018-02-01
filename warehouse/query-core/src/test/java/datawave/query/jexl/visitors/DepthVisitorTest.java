package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;

import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.junit.Assert;
import org.junit.Test;

public class DepthVisitorTest {
    
    @Test
    public void test() throws Exception {
        String originalQuery = "FOO == 'abc'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);
        
        Assert.assertEquals(1, DepthVisitor.getDepth(script, 2));
        Assert.assertEquals(1, DepthVisitor.getDepth(script, 1));
        Assert.assertEquals(1, DepthVisitor.getDepth(script, 0));
    }
    
    @Test
    public void testOr() throws Exception {
        String originalQuery = "FOO == 'abc' or FOO = 'bcd'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);
        
        Assert.assertEquals(2, DepthVisitor.getDepth(script, 3));
        Assert.assertEquals(2, DepthVisitor.getDepth(script, 2));
        Assert.assertEquals(2, DepthVisitor.getDepth(script, 1));
        Assert.assertEquals(1, DepthVisitor.getDepth(script, 0));
    }
    
    @Test
    public void testOr2() throws Exception {
        String originalQuery = "FOO == 'abc' or FOO = 'bcd' or FOO = 'abcdef'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);
        
        Assert.assertEquals(2, DepthVisitor.getDepth(script, 3));
        Assert.assertEquals(2, DepthVisitor.getDepth(script, 2));
        Assert.assertEquals(2, DepthVisitor.getDepth(script, 1));
        Assert.assertEquals(1, DepthVisitor.getDepth(script, 0));
    }
    
    @Test
    public void testOr3() throws Exception {
        String originalQuery = "FOO == 'abc' or FOO = 'bcd' or FOO = 'abcdef' or FOO = 'abcdef' or FOO = 'abcdef' or FOO = 'abcdef' or FOO = 'abcdef' or FOO = 'abcdef' or FOO = 'abcdef' or FOO = 'abcdef' or FOO = 'abcdef'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);
        
        Assert.assertEquals(2, DepthVisitor.getDepth(script, 3));
        Assert.assertEquals(2, DepthVisitor.getDepth(script, 2));
        Assert.assertEquals(2, DepthVisitor.getDepth(script, 1));
        Assert.assertEquals(1, DepthVisitor.getDepth(script, 0));
    }
    
    @Test
    public void testAnd() throws Exception {
        String originalQuery = "FOO == 'abc' and FOO = 'bcd'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);
        
        Assert.assertEquals(2, DepthVisitor.getDepth(script, 3));
        Assert.assertEquals(2, DepthVisitor.getDepth(script, 2));
        Assert.assertEquals(2, DepthVisitor.getDepth(script, 1));
        Assert.assertEquals(1, DepthVisitor.getDepth(script, 0));
    }
    
    @Test
    public void testAnd2() throws Exception {
        String originalQuery = "FOO == 'abc' and FOO = 'bcd' and FOO = 'abcdef'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);
        
        Assert.assertEquals(2, DepthVisitor.getDepth(script, 3));
        Assert.assertEquals(2, DepthVisitor.getDepth(script, 2));
        Assert.assertEquals(2, DepthVisitor.getDepth(script, 1));
        Assert.assertEquals(1, DepthVisitor.getDepth(script, 0));
    }
    
    @Test
    public void testAnd3() throws Exception {
        String originalQuery = "FOO == 'abc' and FOO = 'bcd' and FOO = 'abcdef' and FOO = 'abcdef' and FOO = 'abcdef' and FOO = 'abcdef' and FOO = 'abcdef' and FOO = 'abcdef' and FOO = 'abcdef' and FOO = 'abcdef' and FOO = 'abcdef'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);
        
        Assert.assertEquals(2, DepthVisitor.getDepth(script, 3));
        Assert.assertEquals(2, DepthVisitor.getDepth(script, 2));
        Assert.assertEquals(2, DepthVisitor.getDepth(script, 1));
        Assert.assertEquals(1, DepthVisitor.getDepth(script, 0));
    }
    
    @Test
    public void testParen() throws Exception {
        String originalQuery = "FOO == 'abc' or (FOO = 'bcd' and FOO = 'abcdef')";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);
        
        Assert.assertEquals(4, DepthVisitor.getDepth(script, 5));
        Assert.assertEquals(4, DepthVisitor.getDepth(script, 4));
        Assert.assertEquals(4, DepthVisitor.getDepth(script, 3));
        Assert.assertEquals(3, DepthVisitor.getDepth(script, 2));
        Assert.assertEquals(2, DepthVisitor.getDepth(script, 1));
        Assert.assertEquals(1, DepthVisitor.getDepth(script, 0));
    }
    
    @Test
    public void testParen1() throws Exception {
        String originalQuery = "FOO == 'abc' or (FOO = 'bcd' and (FOO = 'abcdef'))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);
        
        Assert.assertEquals(5, DepthVisitor.getDepth(script, 6));
        Assert.assertEquals(5, DepthVisitor.getDepth(script, 5));
        Assert.assertEquals(5, DepthVisitor.getDepth(script, 4));
        Assert.assertEquals(4, DepthVisitor.getDepth(script, 3));
        Assert.assertEquals(3, DepthVisitor.getDepth(script, 2));
        Assert.assertEquals(2, DepthVisitor.getDepth(script, 1));
        Assert.assertEquals(1, DepthVisitor.getDepth(script, 0));
    }
    
    @Test
    public void testParen2() throws Exception {
        String originalQuery = "(FOO == 'abc' or (FOO = 'bcd' and (FOO = 'abcdef')))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);
        
        Assert.assertEquals(6, DepthVisitor.getDepth(script, 7));
        Assert.assertEquals(6, DepthVisitor.getDepth(script, 6));
        Assert.assertEquals(6, DepthVisitor.getDepth(script, 5));
        Assert.assertEquals(5, DepthVisitor.getDepth(script, 4));
        Assert.assertEquals(4, DepthVisitor.getDepth(script, 3));
        Assert.assertEquals(3, DepthVisitor.getDepth(script, 2));
        Assert.assertEquals(2, DepthVisitor.getDepth(script, 1));
        Assert.assertEquals(1, DepthVisitor.getDepth(script, 0));
    }
    
}
