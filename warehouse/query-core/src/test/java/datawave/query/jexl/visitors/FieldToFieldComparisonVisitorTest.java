package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.ASTEvaluationOnly;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FieldToFieldComparisonVisitorTest {
    @Test
    public void testEq() throws ParseException {
        ASTJexlScript query = FieldToFieldComparisonVisitor.forceEvaluationOnly(JexlASTHelper.parseJexlQuery("FOO == BAR"));
        assertTrue(ASTEvaluationOnly.instanceOf(query));
    }
    
    @Test
    public void testEqDoNothing() throws ParseException {
        ASTJexlScript query = FieldToFieldComparisonVisitor.forceEvaluationOnly(JexlASTHelper.parseJexlQuery("FOO == 'bar'"));
        assertFalse(ASTEvaluationOnly.instanceOf(query));
    }
    
    @Test
    public void testEqDoNothingFieldsToLiteral() throws ParseException {
        ASTJexlScript query = FieldToFieldComparisonVisitor.forceEvaluationOnly(JexlASTHelper.parseJexlQuery("(FOO || BAR).min().hashCode() == 0"));
        assertFalse(ASTEvaluationOnly.instanceOf(query));
    }
    
    @Test
    public void testEqDoNothing2() throws ParseException {
        ASTJexlScript query = FieldToFieldComparisonVisitor.forceEvaluationOnly(JexlASTHelper.parseJexlQuery("(UUID =~ 'C.*?' || UUID =~ 'S.*?')"));
        assertFalse(ASTEvaluationOnly.instanceOf(query));
    }
    
    @Test(expected = ParseException.class)
    public void testEqDoNotSupport() throws ParseException {
        JexlASTHelper.parseJexlQuery("FIELD_A == FIELD_B == FIELD_C");
    }
}
