package datawave.query.jexl.visitors;

import datawave.query.exceptions.InvalidQueryTreeException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.validate.ASTValidator;
import datawave.test.JexlNodeAssert;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Assert;
import org.junit.Test;

public class RewriteNullFunctionsVisitorTest {
    
    @Test
    public void testIsNotNullWithSingleField() throws ParseException {
        String query = "filter:isNotNull(FOO)";
        String expected = "!(FOO == null)";
        test(query, expected);
    }
    
    @Test
    public void testIsNotNullWithMultipleFields() throws ParseException {
        String query = "filter:isNotNull(FOO || FOO2)";
        String expected = "!(FOO == null) || !(FOO2 == null)";
        test(query, expected);
        
        query = "filter:isNotNull(FOO || FOO2 || FOO3)";
        expected = "!(FOO == null) || !(FOO2 == null) || !(FOO3 == null)";
        test(query, expected);
    }
    
    @Test
    public void testNestedIsNotNullWithSingleField() throws ParseException {
        String query = "FOO == 'bar' || filter:isNotNull(FOO)";
        String expected = "FOO == 'bar' || !(FOO == null)";
        test(query, expected);
    }
    
    @Test
    public void testNestedIsNotNullWithMultipleFields() throws ParseException {
        String query = "FOO == 'bar' || filter:isNotNull(FOO || FOO2)";
        String expected = "FOO == 'bar' || !(FOO == null) || !(FOO2 == null)";
        test(query, expected);
    }
    
    private void test(String original, String expected) throws ParseException {
        ASTJexlScript originalScript = JexlASTHelper.parseAndFlattenJexlQuery(original);
        ASTJexlScript actual = RewriteNullFunctionsVisitor.rewriteNullFunctions(originalScript);
        
        JexlNodeAssert.assertThat(actual).isEqualTo(expected).hasValidLineage();
        
        try {
            ASTValidator.isValid(actual);
        } catch (InvalidQueryTreeException e) {
            Assert.fail("IsNotNullIntentVisitor produced an invalid query tree: " + e.getMessage());
        }
    }
}
