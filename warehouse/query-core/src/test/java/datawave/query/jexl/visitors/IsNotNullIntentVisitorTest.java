package datawave.query.jexl.visitors;

import datawave.query.exceptions.InvalidQueryTreeException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.validate.ASTValidator;
import datawave.test.JexlNodeAssert;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Assert;
import org.junit.Test;

public class IsNotNullIntentVisitorTest {
    
    @Test
    public void testMatchAnythingRegex() throws ParseException {
        String query = "FOO =~ '.*?'";
        String expected = "FOO != null";
        
        assertResult(query, expected);
    }
    
    @Test
    public void testMatchSpecificValue() throws ParseException {
        String query = "FOO =~ 'value*'";
        String expected = "FOO =~ 'value*'";
        
        assertResult(query, expected);
    }
    
    @Test
    public void testConjunctionWithMatchAnythingRegex() throws ParseException {
        String query = "FOO =~ '.*?' && BAR =~ 'anything*'";
        String expected = "FOO != null && BAR =~ 'anything*'";
        
        assertResult(query, expected);
    }
    
    @Test
    public void testIsNotNullWithSingleField() throws ParseException {
        String query = "filter:isNotNull(FOO)";
        String expected = "!(FOO == null)";
        assertResult(query, expected);
    }
    
    @Test
    public void testIsNotNullWithMultipleFields() throws ParseException {
        String query = "filter:isNotNull(FOO || FOO2)";
        String expected = "!(FOO == null) || !(FOO2 == null)";
        assertResult(query, expected);
        
        query = "filter:isNotNull(FOO || FOO2 || FOO3)";
        expected = "!(FOO == null) || !(FOO2 == null) || !(FOO3 == null)";
        assertResult(query, expected);
    }
    
    @Test
    public void testNestedIsNotNullWithSingleField() throws ParseException {
        String query = "FOO == 'bar' || filter:isNotNull(FOO)";
        String expected = "FOO == 'bar' || !(FOO == null)";
        assertResult(query, expected);
    }
    
    @Test
    public void testNestedIsNotNullWithMultipleFields() throws ParseException {
        String query = "FOO == 'bar' || filter:isNotNull(FOO || FOO2)";
        String expected = "FOO == 'bar' || !(FOO == null) || !(FOO2 == null)";
        assertResult(query, expected);
    }
    
    private void assertResult(String original, String expected) throws ParseException {
        ASTJexlScript originalScript = JexlASTHelper.parseAndFlattenJexlQuery(original);
        ASTJexlScript actual = IsNotNullIntentVisitor.fixNotNullIntent(originalScript);
        
        JexlNodeAssert.assertThat(actual).isEqualTo(expected).hasValidLineage();
        
        try {
            ASTValidator.isValid(actual);
        } catch (InvalidQueryTreeException e) {
            Assert.fail("IsNotNullIntentVisitor produced an invalid query tree: " + e.getMessage());
        }
    }
}
