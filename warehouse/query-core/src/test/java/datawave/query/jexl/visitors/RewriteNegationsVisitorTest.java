package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Assert that the {@link RewriteNegationsVisitor} functions correctly for queries that exercise all of the basic comparison operators including regex
 * operators.
 */
public class RewriteNegationsVisitorTest {
    
    // Test AST such that (A)
    @Test
    public void testSingleEQ() throws ParseException {
        String queryString = "FOO == BAR";
        String expectedQuery = "FOO == BAR";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        ASTJexlScript negatedScript = RewriteNegationsVisitor.rewrite(script);
        String negatedQuery = JexlStringBuildingVisitor.buildQuery(negatedScript);
        String errMsg = "Failed for query structure like (A)";
        assertEquals(errMsg, expectedQuery, negatedQuery);
    }
    
    // Test AST such that (!A)
    @Test
    public void testSingleNE() throws ParseException {
        String queryString = "FOO != BAR";
        String expectedQuery = "!(FOO == BAR)";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        ASTJexlScript negatedScript = RewriteNegationsVisitor.rewrite(script);
        String negatedQuery = JexlStringBuildingVisitor.buildQuery(negatedScript);
        String errMsg = "Failed for query structure like (!A)";
        assertEquals(errMsg, expectedQuery, negatedQuery);
    }
    
    // Test AST such that (!A && !B)
    @Test
    public void testConjunctionTwoNE() throws ParseException {
        String queryString = "FOO != BAR && BAR != FOO";
        String expectedQuery = "!(FOO == BAR) && !(BAR == FOO)";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        ASTJexlScript negatedScript = RewriteNegationsVisitor.rewrite(script);
        String negatedQuery = JexlStringBuildingVisitor.buildQuery(negatedScript);
        String errMsg = "Failed for query structure like (!A && !B)";
        assertEquals(errMsg, expectedQuery, negatedQuery);
    }
    
    // Test AST such that ((!A && !B))
    @Test
    public void testConjunctionTwoNestedNE() throws ParseException {
        String queryString = "(FOO != BAR && BAR != FOO)";
        String expectedQuery = "(!(FOO == BAR) && !(BAR == FOO))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        ASTJexlScript negatedScript = RewriteNegationsVisitor.rewrite(script);
        String negatedQuery = JexlStringBuildingVisitor.buildQuery(negatedScript);
        String errMsg = "Failed for query structure like ((!A && !B))";
        assertEquals(errMsg, expectedQuery, negatedQuery);
    }
    
    // Test AST such that (!A & B)
    @Test
    public void testConjunctionOfSingleNEAndEQ() throws ParseException {
        String queryString = "FOO != BAR && BAR == FOO";
        String expectedQuery = "!(FOO == BAR) && BAR == FOO";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        ASTJexlScript negatedScript = RewriteNegationsVisitor.rewrite(script);
        String negatedQuery = JexlStringBuildingVisitor.buildQuery(negatedScript);
        assertEquals(expectedQuery, negatedQuery);
    }
    
    // Test AST such that (!A && !B && C)
    @Test
    public void testConjunctionOfTwoNEAndSingleEQ() throws ParseException {
        String queryString = "FOO != BAR && BAR != FOO && BAR == CAT";
        String expectedQuery = "!(FOO == BAR) && !(BAR == FOO) && BAR == CAT";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        ASTJexlScript negatedScript = RewriteNegationsVisitor.rewrite(script);
        String negatedQuery = JexlStringBuildingVisitor.buildQuery(negatedScript);
        String errMsg = "Failed for query structure like (!A && !B && C)";
        assertEquals(errMsg, expectedQuery, negatedQuery);
    }
    
    // Test AST such that (!A && (!B && C))
    @Test
    public void testConjunctionOfNEAndNestedNeAndEQ() throws ParseException {
        String queryString = "FOO != BAR && (BAR != FOO && BAR == CAT)";
        String expectedQuery = "!(FOO == BAR) && (!(BAR == FOO) && BAR == CAT)";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        ASTJexlScript negatedScript = RewriteNegationsVisitor.rewrite(script);
        String negatedQuery = JexlStringBuildingVisitor.buildQuery(negatedScript);
        String errMsg = "Failed for query structure like (!A && !B && !C))";
        assertEquals(errMsg, expectedQuery, negatedQuery);
    }
    
    // Test AST such that (A && (!B && !C))
    @Test
    public void testSingleEQWithNestedConjunctionOfTwoNE() throws ParseException {
        String queryString = "FOO == BAR && (BAR != FOO && BAR != CAT)";
        String expectedQuery = "FOO == BAR && (!(BAR == FOO) && !(BAR == CAT))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        ASTJexlScript negatedScript = RewriteNegationsVisitor.rewrite(script);
        String negatedQuery = JexlStringBuildingVisitor.buildQuery(negatedScript);
        String errMsg = "Failed for query structure like (A && (!B && !C))";
        assertEquals(errMsg, expectedQuery, negatedQuery);
    }
    
    @Test
    public void testSingleGT() throws ParseException {
        String queryString = "FOO > BAR";
        String expectedQuery = "FOO > BAR";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        ASTJexlScript negatedScript = RewriteNegationsVisitor.rewrite(script);
        String negatedQuery = JexlStringBuildingVisitor.buildQuery(negatedScript);
        assertEquals(expectedQuery, negatedQuery);
    }
    
    @Test
    public void testSingleGE() throws ParseException {
        String queryString = "FOO >= BAR";
        String expectedQuery = "FOO >= BAR";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        ASTJexlScript negatedScript = RewriteNegationsVisitor.rewrite(script);
        String negatedQuery = JexlStringBuildingVisitor.buildQuery(negatedScript);
        assertEquals(expectedQuery, negatedQuery);
    }
    
    @Test
    public void testSingleLT() throws ParseException {
        String queryString = "FOO < BAR";
        String expectedQuery = "FOO < BAR";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        ASTJexlScript negatedScript = RewriteNegationsVisitor.rewrite(script);
        String negatedQuery = JexlStringBuildingVisitor.buildQuery(negatedScript);
        assertEquals(expectedQuery, negatedQuery);
    }
    
    @Test
    public void testSingleLE() throws ParseException {
        String queryString = "FOO <= BAR";
        String expectedQuery = "FOO <= BAR";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        ASTJexlScript negatedScript = RewriteNegationsVisitor.rewrite(script);
        String negatedQuery = JexlStringBuildingVisitor.buildQuery(negatedScript);
        assertEquals(expectedQuery, negatedQuery);
    }
    
    // Test a Negated Regex node
    @Test
    public void testSingleNR() throws ParseException {
        String queryString = "FOO !~ BAR";
        String expectedQuery = "!(FOO =~ BAR)";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        ASTJexlScript negatedScript = RewriteNegationsVisitor.rewrite(script);
        String negatedQuery = JexlStringBuildingVisitor.buildQuery(negatedScript);
        assertEquals(expectedQuery, negatedQuery);
    }
    
    // Test a conjunction of two Negated Regex nodes
    @Test
    public void testConjunctionOfTwoNR() throws ParseException {
        String queryString = "FOO !~ BAR && BAR !~ FOO";
        String expectedQuery = "!(FOO =~ BAR) && !(BAR =~ FOO)";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        ASTJexlScript negatedScript = RewriteNegationsVisitor.rewrite(script);
        String negatedQuery = JexlStringBuildingVisitor.buildQuery(negatedScript);
        assertEquals(expectedQuery, negatedQuery);
    }
}
