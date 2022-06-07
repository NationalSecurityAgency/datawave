package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class IvaratorRequiredVisitorTest {
    
    @Test
    public void testExceededOrThresholdTrue() throws ParseException {
        testQuery("((_List_ = true) && (FOO == 1 && FOO == 3))", true);
    }
    
    @Test
    public void testExceededOrThresholdFalse() throws ParseException {
        testQuery("(FOO == 1 && FOO == 3)", false);
    }
    
    @Test
    public void testExceededValueThresholdTrue() throws ParseException {
        testQuery("((_Value_ = true) && (FOO =~ '1*' || FOO =~ '*3'))", true);
    }
    
    @Test
    public void testExceededValueThresholdFalse() throws ParseException {
        testQuery("(FOO =~ '1*' || FOO =~ '*3')", false);
    }
    
    @Test
    public void testExceededValueThresholdIntersection() throws ParseException {
        testQuery("FOO == 'bar' && ((_Value_ = true) && (FOO =~ 'ba.*' ))", true);
    }
    
    @Test
    public void testExceededValueThresholdUnion() throws ParseException {
        testQuery("FOO == 'bar' || ((_Value_ = true) && (FOO =~ 'ba.*' ))", true);
    }
    
    @Test
    public void testExceededOrThresholdIntersection() throws ParseException {
        testQuery("FOO == 'bar' && ((_List_ = true) && (FOO == 'bar' ))", true);
    }
    
    @Test
    public void testExceededOrThresholdUnion() throws ParseException {
        testQuery("FOO == 'bar' || ((_List_ = true) && (FOO == 'bar' ))", true);
    }
    
    private void testQuery(String query, boolean ivaratorExpected) throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        assertEquals(ivaratorExpected, IvaratorRequiredVisitor.isIvaratorRequired(script));
    }
    
}
