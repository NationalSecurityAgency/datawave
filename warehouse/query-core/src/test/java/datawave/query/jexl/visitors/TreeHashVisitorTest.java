package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;

import org.apache.commons.jexl2.parser.ASTDelayedPredicate;
import org.apache.commons.jexl2.parser.JexlNode;
import org.junit.Assert;
import org.junit.Test;

public class TreeHashVisitorTest {
    
    @Test
    public void testSimpleEquality() throws Exception {
        String queryA = "FOO == 'abc'";
        String queryB = "FOO == 'abc'";
        Assert.assertEquals(TreeHashVisitor.getNodeHash(JexlASTHelper.parseJexlQuery(queryA)),
                        TreeHashVisitor.getNodeHash(JexlASTHelper.parseJexlQuery(queryB)));
        
    }
    
    @Test
    public void testSubExpression() throws Exception {
        String queryA = "(FOO == 'abc' || CAR == 'carA')";
        String queryB = "(CAR == 'carA' || FOO == 'abc')";
        Assert.assertEquals(TreeHashVisitor.getNodeHash(JexlASTHelper.parseJexlQuery(queryA)),
                        TreeHashVisitor.getNodeHash(JexlASTHelper.parseJexlQuery(queryB)));
        
    }
    
    @Test
    public void testExpressionDelayed() throws Exception {
        JexlNode queryA = JexlASTHelper.parseJexlQuery("FOO == 'abc'");
        JexlNode queryB = ASTDelayedPredicate.create(JexlASTHelper.parseJexlQuery("FOO == 'abc'"));
        Assert.assertNotEquals(TreeHashVisitor.getNodeHash(queryA), TreeHashVisitor.getNodeHash(queryB));
        
    }
    
    @Test
    public void testRange() throws Exception {
        JexlNode queryA = JexlASTHelper.parseJexlQuery("FOO >= 'abc' && FOO >= 'bcd'");
        JexlNode queryB = JexlASTHelper.parseJexlQuery("FOO >= 'bcd' && FOO >= 'abc'");
        Assert.assertEquals(TreeHashVisitor.getNodeHash(queryA), TreeHashVisitor.getNodeHash(queryB));
        
    }
    
    @Test
    public void testRangeFalse() throws Exception {
        JexlNode queryA = JexlASTHelper.parseJexlQuery("(FOO >= 'abc' && FOO >= 'bcd') && CAR=='wagon'");
        JexlNode queryB = JexlASTHelper.parseJexlQuery("FOO >= 'bcd' && FOO >= 'abc'");
        Assert.assertNotEquals(TreeHashVisitor.getNodeHash(queryA), TreeHashVisitor.getNodeHash(queryB));
        
    }
    
    @Test
    public void testFunction() throws Exception {
        JexlNode queryA = JexlASTHelper.parseJexlQuery("(FOO == 'blah1' && FOO == 'blah2' && content:phrase(termOffsetMap, 'twisted', 'pair'))");
        JexlNode queryB = JexlASTHelper.parseJexlQuery("(content:phrase(termOffsetMap, 'twisted', 'pair') && FOO == 'blah1' && FOO == 'blah2')");
        Assert.assertEquals(TreeHashVisitor.getNodeHash(queryA), TreeHashVisitor.getNodeHash(queryB));
        
    }
    
    @Test
    public void testSimpleEqualityNegative() throws Exception {
        String queryA = "FOO == 'ab2c'";
        String queryB = "FOO == 'abc'";
        Assert.assertNotEquals(TreeHashVisitor.getNodeHash(JexlASTHelper.parseJexlQuery(queryA)),
                        TreeHashVisitor.getNodeHash(JexlASTHelper.parseJexlQuery(queryB)));
        
    }
    
}
