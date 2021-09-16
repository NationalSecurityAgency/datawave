package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import datawave.test.JexlNodeAssert;
import org.apache.commons.jexl2.parser.ASTEvaluationOnly;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Test;

import java.util.function.Predicate;

public class FieldToFieldComparisonVisitorTest {
    
    private static final Predicate<JexlNode> notASTEvaluationOnlyInstance = (node) -> !ASTEvaluationOnly.instanceOf(node);
    
    @Test
    public void testEq() throws ParseException {
        String query = "((_Eval_ = true) && (FOO == BAR))";
        ASTJexlScript script = FieldToFieldComparisonVisitor.forceEvaluationOnly(JexlASTHelper.parseJexlQuery("FOO == BAR"));
        JexlNodeAssert.assertThat(script).isEqualTo(query).matches(ASTEvaluationOnly::instanceOf);
    }
    
    @Test
    public void testEqDoNothing() throws ParseException {
        String query = "FOO == 'bar'";
        ASTJexlScript script = FieldToFieldComparisonVisitor.forceEvaluationOnly(JexlASTHelper.parseJexlQuery("FOO == 'bar'"));
        JexlNodeAssert.assertThat(script).isEqualTo(query).matches(notASTEvaluationOnlyInstance);
    }
    
    @Test
    public void testEqDoNothingFieldsToLiteral() throws ParseException {
        String query = "(FOO || BAR).min().hashCode() == 0";
        ASTJexlScript script = FieldToFieldComparisonVisitor.forceEvaluationOnly(JexlASTHelper.parseJexlQuery("(FOO || BAR).min().hashCode() == 0"));
        JexlNodeAssert.assertThat(script).isEqualTo(query).matches(notASTEvaluationOnlyInstance);
    }
    
    @Test
    public void testEqDoNothing2() throws ParseException {
        String query = "(UUID =~ 'C.*?' || UUID =~ 'S.*?')";
        ASTJexlScript script = FieldToFieldComparisonVisitor.forceEvaluationOnly(JexlASTHelper.parseJexlQuery("(UUID =~ 'C.*?' || UUID =~ 'S.*?')"));
        JexlNodeAssert.assertThat(script).isEqualTo(query).matches(notASTEvaluationOnlyInstance);
    }
    
    @Test(expected = ParseException.class)
    public void testEqDoNotSupport() throws ParseException {
        JexlASTHelper.parseJexlQuery("FIELD_A == FIELD_B == FIELD_C");
    }
}
