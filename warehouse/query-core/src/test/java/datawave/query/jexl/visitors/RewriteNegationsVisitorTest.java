package datawave.query.jexl.visitors;

import datawave.query.exceptions.InvalidQueryTreeException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.validate.ASTValidator;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Assert that the {@link RewriteNegationsVisitor} functions correctly for queries that exercise all of the basic comparison operators including regex
 * operators.
 */
public class RewriteNegationsVisitorTest {
    
    private final ASTValidator validator = new ASTValidator();
    
    // Test AST such that (A)
    @Test
    public void testSingleEQ() throws ParseException {
        String query = "FOO == 'bar'";
        test(query, query);
    }
    
    // Test AST such that (!A)
    @Test
    public void testSingleNE() throws ParseException {
        String query = "FOO != 'bar'";
        String expected = "!(FOO == 'bar')";
        test(query, expected);
    }
    
    // Test AST such that (!A && !B)
    @Test
    public void testConjunctionTwoNE() throws ParseException {
        String query = "FOO != 'bar' && FOO2 != 'bar2'";
        String expected = "!(FOO == 'bar') && !(FOO2 == 'bar2')";
        test(query, expected);
    }
    
    // Test AST such that ((!A && !B))
    @Test
    public void testConjunctionTwoNestedNE() throws ParseException {
        String query = "(FOO != 'bar' && FOO2 != 'bar2')";
        String expected = "(!(FOO == 'bar') && !(FOO2 == 'bar2'))";
        test(query, expected);
    }
    
    // Test AST such that (!A & B)
    @Test
    public void testConjunctionOfSingleNEAndEQ() throws ParseException {
        String query = "FOO != 'bar' && FOO2 == 'bar2'";
        String expected = "!(FOO == 'bar') && FOO2 == 'bar2'";
        test(query, expected);
    }
    
    // Test AST such that (!A && !B && C)
    @Test
    public void testConjunctionOfTwoNEAndSingleEQ() throws ParseException {
        String query = "FOO != 'bar' && FOO2 != 'bar2' && FOO3 == 'bar3'";
        String expected = "!(FOO == 'bar') && !(FOO2 == 'bar2') && FOO3 == 'bar3'";
        test(query, expected);
    }
    
    // Test AST such that (!A && (!B && C))
    @Test
    public void testConjunctionOfNEAndNestedNeAndEQ() throws ParseException {
        String query = "FOO != 'bar' && (FOO2 != 'bar2' && FOO3 == 'bar3')";
        String expected = "!(FOO == 'bar') && !(FOO2 == 'bar2') && FOO3 == 'bar3'";
        test(query, expected);
    }
    
    // Test AST such that (A && (!B && !C))
    @Test
    public void testSingleEQWithNestedConjunctionOfTwoNE() throws ParseException {
        String query = "FOO == 'bar' && (FOO != 'bar2' && FOO != 'bar3')";
        String expected = "FOO == 'bar' && !(FOO == 'bar2') && !(FOO == 'bar3')";
        test(query, expected);
    }
    
    @Test
    public void testSingleGT() throws ParseException {
        String query = "FOO > 'bar'";
        test(query, query);
    }
    
    @Test
    public void testSingleGE() throws ParseException {
        String query = "FOO >= 'bar'";
        test(query, query);
    }
    
    @Test
    public void testSingleLT() throws ParseException {
        String query = "FOO < BAR";
        test(query, query);
    }
    
    @Test
    public void testSingleLE() throws ParseException {
        String query = "FOO <= 'bar'";
        test(query, query);
    }
    
    // Test a Negated Regex node
    @Test
    public void testSingleNR() throws ParseException {
        String query = "FOO !~ 'bar'";
        String expected = "!(FOO =~ 'bar')";
        test(query, expected);
    }
    
    // Test a conjunction of two Negated Regex nodes
    @Test
    public void testConjunctionOfTwoNR() throws ParseException {
        String query = "FOO !~ 'bar' && FOO2 !~ 'bar2'";
        String expected = "!(FOO =~ 'bar') && !(FOO2 =~ 'bar2')";
        test(query, expected);
    }
    
    private void test(String query, String expected) throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
        ASTJexlScript negatedScript = RewriteNegationsVisitor.rewrite(script);
        
        // assert raw query strings match
        String negatedQuery = JexlStringBuildingVisitor.buildQuery(negatedScript);
        assertEquals(expected, negatedQuery);
        
        // assert script equality
        ASTJexlScript expectedScript = JexlASTHelper.parseAndFlattenJexlQuery(expected);
        assertTrue(TreeEqualityVisitor.checkEquality(expectedScript, negatedScript).isEqual());
        
        try {
            assertTrue(validator.isValid(negatedScript, RewriteNegationsVisitorTest.class.getSimpleName(), false));
        } catch (InvalidQueryTreeException e) {
            fail("Unexpected failure while validating query tree: " + e.getMessage());
        }
    }
}
