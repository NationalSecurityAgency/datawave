package datawave.query.jexl.visitors;

import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Test;

public class ValidComparisonVisitorTest {
    
    @Test
    public void testHappyPath() throws ParseException {
        String queryString = "BAR == 1 && FOO =~ '1234.*' && BAR != 'x' && FOO !~ 'why' && BAR < 'x'  && FOO > 'y' && BAR <= 'c' && FOO >= 'd'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        ValidComparisonVisitor.check(script);
    }
    
    @Test
    public void testFieldOpField() throws ParseException {
        String queryString = "BAR == FOO && FOO =~ BAR && BAR != FOO && FOO !~ BAR && BAR < FOO  && FOO > BAR && BAR <= FOO && FOO >= BAR";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        ValidComparisonVisitor.check(script);
    }
    
    @Test(expected = DatawaveFatalQueryException.class)
    public void testInvalidEQ() throws ParseException {
        String queryString = "'BAR' == 1";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        ValidComparisonVisitor.check(script);
    }
    
    @Test(expected = DatawaveFatalQueryException.class)
    public void testInvalidNE() throws ParseException {
        String queryString = "'BAR' != 1";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        ValidComparisonVisitor.check(script);
    }
    
    @Test(expected = DatawaveFatalQueryException.class)
    public void testInvalidRE() throws ParseException {
        String queryString = "'BAR' =~ 'foo'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        ValidComparisonVisitor.check(script);
    }
    
    @Test(expected = DatawaveFatalQueryException.class)
    public void testInvalidNR() throws ParseException {
        String queryString = "'BAR' !~ 'foo'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        ValidComparisonVisitor.check(script);
    }
    
    @Test(expected = DatawaveFatalQueryException.class)
    public void testInvalidLT() throws ParseException {
        String queryString = "'BAR' < 1";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        ValidComparisonVisitor.check(script);
    }
    
    @Test(expected = DatawaveFatalQueryException.class)
    public void testInvalidLE() throws ParseException {
        String queryString = "'BAR' <= 1";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        ValidComparisonVisitor.check(script);
    }
    
    @Test(expected = DatawaveFatalQueryException.class)
    public void testInvalidGT() throws ParseException {
        String queryString = "'BAR' > 1";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        ValidComparisonVisitor.check(script);
    }
    
    @Test(expected = DatawaveFatalQueryException.class)
    public void testInvalidGE() throws ParseException {
        String queryString = "'BAR' >= 1";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        ValidComparisonVisitor.check(script);
    }
    
    @Test(expected = DatawaveFatalQueryException.class)
    public void testInvalidAssignment() throws ParseException {
        String queryString = "'_Eval_' = true";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        ValidComparisonVisitor.check(script);
    }
    
    @Test
    public void testInvalidFunction() throws ParseException {
        String queryString = "geowave:intersects('FIELD1' || 'FIELD2', 'POINT(0 0)')";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        ValidComparisonVisitor.check(script);
    }
    
    @Test
    public void testMethodNode() throws ParseException {
        String queryString = "AG.greaterThan(39).size() >= 1";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        ValidComparisonVisitor.check(script);
    }
    
    @Test
    public void testAdditiveNode() throws ParseException {
        String queryString = "1+1+1 == 3";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        ValidComparisonVisitor.check(script);
    }
    
    @Test
    public void testSubNode() throws ParseException {
        String queryString = "5-1-1 == 3";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        ValidComparisonVisitor.check(script);
    }
    
    @Test
    public void testMulNode() throws ParseException {
        String queryString = "1*3*1 == 3";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        ValidComparisonVisitor.check(script);
    }
    
    @Test
    public void testDivNode() throws ParseException {
        String queryString = "6/2 == 3";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        ValidComparisonVisitor.check(script);
    }
    
    @Test
    public void testModNode() throws ParseException {
        String queryString = "3%10 == 3";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        ValidComparisonVisitor.check(script);
    }
}
