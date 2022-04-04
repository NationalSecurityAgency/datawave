package datawave.query.jexl.visitors;

import datawave.query.exceptions.InvalidQueryTreeException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.validate.ASTValidator;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class IsNotNullPruningVisitorTest {
    
    @Test
    public void testSimpleEqCase() {
        String query = "!(FOO == null) && FOO == 'bar'";
        String expected = "FOO == 'bar'";
        test(query, expected);
    }
    
    @Test
    public void testSimpleErCase() {
        String query = "!(FOO == null) && FOO =~ 'ba.*'";
        String expected = "FOO =~ 'ba.*'";
        test(query, expected);
    }
    
    @Test
    public void testLargerCase() {
        String query = "!(FOO == null) && FOO == 'bar' && (FOO2 == 'bar2' || FOO3 == 'bar3')";
        String expected = "FOO == 'bar' && (FOO2 == 'bar2' || FOO3 == 'bar3')";
        test(query, expected);
    }
    
    @Test
    public void testNoOpCase() {
        String query = "!(FOO == null) || FOO == 'bar'";
        test(query, query);
    }
    
    @Test
    public void testNestedCase() {
        String query = "(!(FOO == null) && FOO == 'bar') || (FOO2 == 'bar2' && FOO3 == 'bar3')";
        String expected = "(FOO == 'bar') || (FOO2 == 'bar2' && FOO3 == 'bar3')";
        test(query, expected);
        
        // flip which branch contains the not null
        query = "(FOO2 == 'bar2' && FOO3 == 'bar3') || (!(FOO == null) && FOO == 'bar')";
        expected = "(FOO2 == 'bar2' && FOO3 == 'bar3') || (FOO == 'bar')";
        test(query, expected);
    }
    
    @Test
    public void testOneNotNullToManyEqs() {
        String query = "!(FOO == null) && FOO == 'bar' && FOO == 'baz'";
        String expected = "FOO == 'bar' && FOO == 'baz'";
        test(query, expected);
        
        query = "!(FOO == null) && FOO =~ 'bar.*' && FOO =~ 'baz.*'";
        expected = "FOO =~ 'bar.*' && FOO =~ 'baz.*'";
        test(query, expected);
    }
    
    @Test
    public void testMultipleNotNullsSameField() {
        String query = "!(FOO == null) && !(FOO == null) && FOO == 'bar'";
        String expected = "FOO == 'bar'";
        test(query, expected);
        
        // unordered
        query = "!(FOO == null) && FOO == 'bar' && !(FOO == null)";
        expected = "FOO == 'bar'";
        test(query, expected);
    }
    
    @Test
    public void testMultipleNotNullsDifferentFields() {
        String query = "!(FOO == null) && FOO == 'bar' && !(FOO2 == null) && FOO2 == 'baz'";
        String expected = "FOO == 'bar' && FOO2 == 'baz'";
        test(query, expected);
    }
    
    @Test
    public void testMultipleNotNullsAndEqsUnordered() {
        String query = "FOO2 == 'baz' && FOO == 'bar' && !(FOO == null) && !(FOO2 == null)";
        String expected = "FOO2 == 'baz' && FOO == 'bar'";
        test(query, expected);
    }
    
    @Test
    public void testMultipleNotNullsWithSingleEquality() {
        String query = "!(FOO == null) && FOO == 'bar' && !(FOO2 == null)";
        String expected = "FOO == 'bar' && !(FOO2 == null)";
        test(query, expected);
    }
    
    private void test(String query, String expected) {
        try {
            ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
            ASTJexlScript visited = (ASTJexlScript) IsNotNullPruningVisitor.prune(script);
            ASTJexlScript expectedScript = JexlASTHelper.parseAndFlattenJexlQuery(expected);
            
            assertTrue(JexlStringBuildingVisitor.buildQueryWithoutParse(visited), TreeEqualityVisitor.checkEquality(visited, expectedScript).isEqual());
            assertTrue(ASTValidator.isValid(visited));
        } catch (ParseException | InvalidQueryTreeException e) {
            e.printStackTrace();
            fail("Failed to parse or validate query: " + query);
        }
    }
    
}
