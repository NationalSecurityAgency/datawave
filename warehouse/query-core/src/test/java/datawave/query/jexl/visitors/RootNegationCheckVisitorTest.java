package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RootNegationCheckVisitorTest {
    @Before
    public void setup() {
        
    }
    
    @Test
    public void testNEq() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("FOO != 'bar'");
        Assert.assertTrue(RootNegationCheckVisitor.hasTopLevelNegation(script));
    }
    
    @Test
    public void testWrappedNegation() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("!(FOO == 'bar')");
        Assert.assertTrue(RootNegationCheckVisitor.hasTopLevelNegation(script));
    }
    
    @Test
    public void testEq() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("FOO == 'bar'");
        Assert.assertFalse(RootNegationCheckVisitor.hasTopLevelNegation(script));
    }
    
    @Test
    public void testWrappedNEq() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("!(FOO != 'bar')");
        Assert.assertFalse(RootNegationCheckVisitor.hasTopLevelNegation(script));
    }
    
    @Test
    public void testNegatedAnd() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("!(FOO == 'bar' && FOO == 'baz')");
        Assert.assertTrue(RootNegationCheckVisitor.hasTopLevelNegation(script));
    }
    
    @Test
    public void testNegatedOneLeafAnd() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("!(FOO != 'bar' && FOO == 'baz')");
        Assert.assertFalse(RootNegationCheckVisitor.hasTopLevelNegation(script));
    }
    
    @Test
    public void testNegatedMarker() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("((_Delayed_ = true) && !(FOO == 'bar' && FOO == 'baz'))");
        Assert.assertTrue(RootNegationCheckVisitor.hasTopLevelNegation(script));
    }
    
    @Test
    public void testWrappedNegatedMarker() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("!((_Delayed_ = true) && (FOO == 'bar' && FOO == 'baz'))");
        Assert.assertTrue(RootNegationCheckVisitor.hasTopLevelNegation(script));
    }
    
    @Test
    public void testNegatedMarkerOneLeaf() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("((_Delayed_ = true) && !(FOO != 'bar' && FOO == 'baz'))");
        Assert.assertFalse(RootNegationCheckVisitor.hasTopLevelNegation(script));
    }
    
    @Test
    public void testNR() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("FOO !~ 'bar.*'");
        Assert.assertTrue(RootNegationCheckVisitor.hasTopLevelNegation(script));
    }
    
    @Test
    public void testWrappedER() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("!(FOO =~ 'bar.*')");
        Assert.assertTrue(RootNegationCheckVisitor.hasTopLevelNegation(script));
    }
    
    @Test
    public void testDoubleNegatedNR() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("!(FOO !~ 'bar.*')");
        Assert.assertFalse(RootNegationCheckVisitor.hasTopLevelNegation(script));
    }
}
