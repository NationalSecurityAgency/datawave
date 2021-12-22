package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TermCountingVisitorTest {
    
    @Test
    public void testRange() throws ParseException {
        String query = "((_Bounded_ = true) && (NUM >= '+aE1' && NUM <= '+aE4'))";
        testCounts(query, 1);
    }
    
    @Test
    public void testNegatedRange() throws ParseException {
        String query = "!((_Bounded_ = true) && (NUM >= '+aE1' && NUM <= '+aE4'))";
        testCounts(query, 1);
    }
    
    @Test
    public void testRangePlusTerm() throws ParseException {
        String query = "((_Bounded_ = true) && (NUM >= '+aE1' && NUM <= '+aE4')) && FOO == 'bar'";
        testCounts(query, 2);
    }
    
    @Test
    public void testUnBRAndTerm() throws ParseException {
        String query = "NUM >= '+aE1' && FOO == 'bar' && NUM <= '+aE4'";
        testCounts(query, 3);
    }
    
    @Test
    public void testUnboundedUnorderedRangeAndTerm() throws ParseException {
        String query = "(NUM >= '+aE1' && FOO == 'bar') && NUM <= '+aE4'";
        testCounts(query, 3);
    }
    
    @Test
    public void testTwoRangesForTheSameTerm() throws ParseException {
        String query = "((_Bounded_ = true) && (NUM >= '+aE1' && NUM <= '+aE4')) || ((_Bounded_ = true) && (NUM >= '+aE7' && NUM <= '+aE9'))";
        // When the same term is present in two distinct ranges, it should be counted as two separate terms.
        testCounts(query, 2);
    }
    
    @Test
    public void testTwoRangesPlusTerm() throws ParseException {
        String query = "((_Bounded_ = true) && (NUM >= '+aE1' && NUM <= '+aE4')) && FOO == 'bar' && ((_Bounded_ = true) && (NUM2 >= '+aE7' && NUM2 <= '+aE9'))";
        testCounts(query, 3);
    }
    
    @Test
    public void testUnboundedTwoRangesPlusTerm_UnorderedWrapping() throws ParseException {
        String query = "NUM >= '+aE1' && (NUM <= '+aE4' && FOO == 'bar') && (NUM2 >= '+aE7' && NUM2 <= '+aE9')";
        testCounts(query, 5);
    }
    
    @Test
    public void testNestedRangeWithinConjunction() throws ParseException {
        String query = "((_Bounded_ = true) && (NUM >= '+aE1' && NUM <= '+aE4')) && (FOO == 'bar' || ((_Bounded_ = true) && (NUM2 >= '+aE7' && NUM2 <= '+aE9')))";
        testCounts(query, 3);
    }
    
    @Test
    public void testNestedRangeWithinDisjunction() throws ParseException {
        String query = "((_Bounded_ = true) && (NUM >= '+aE1' && NUM <= '+aE4')) || (FOO == 'bar' && ((_Bounded_ = true) && (NUM2 >= '+aE7' && NUM2 <= '+aE9')))";
        testCounts(query, 3);
    }
    
    @Test
    public void testQueryWithFunction() throws ParseException {
        String query = "((_Bounded_ = true) && (NUM >= '+aE1' && NUM <= '+aE4')) || filter:includeRegex(FOO, '.*bar.*')";
        testCounts(query, 2);
    }
    
    @Test
    public void testExceededOrThreshold() throws ParseException {
        String query = "((_List_ = true) && ((id = '123') && (field = 'TEST') && (params = '{\"values\":[\"a\",\"b\",\"c\"]}')))";
        testCounts(query, 1);
    }
    
    @Test
    public void testExceededTermThreshold() throws ParseException {
        String query = "((_Term_ = true) && (FOO == 'bar'))";
        testCounts(query, 1);
    }
    
    @Test
    public void testExceededValueThreshold() throws ParseException {
        String query = "((_Value_ = true) && (INDEX_ONLY_FIELD =~ 'a*'))";
        testCounts(query, 1);
    }
    
    @Test
    public void testRandomAssignmentNodeThreshold() throws ParseException {
        String query = "((OtherProperty = true) && ((id = '123') && (field = 'test') && (params = '{\"values\":[\"a\",\"b\",\"c\"]}')))";
        testCounts(query, 0);
    }
    
    @Test
    public void testFailure() throws ParseException {
        String query = "(((_Bounded_ = true) && (GEO >= '0202' && GEO <= '020d')) || ((_Bounded_ = true) && (GEO >= '030a' && GEO <= '0335')) || ((_Bounded_ = true) && (GEO >= '0428' && GEO <= '0483')) || (((_Bounded_ = true) && GEO >= '0500aa' && GEO <= '050355')) || ((_Bounded_ = true) && (GEO >= '1f0aaaaaaaaaaaaaaa' && GEO <= '1f36c71c71c71c71c7'))) && ((_Bounded_ = true) && (WKT_BYTE_LENGTH >= 0 && WKT_BYTE_LENGTH < 80))";
        testCounts(query, 6);
    }
    
    private void testCounts(String query, int expectedCount) throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        int count = TermCountingVisitor.countTerms(script);
        assertEquals(expectedCount, count);
        assertTrue(JexlASTHelper.validateLineage(script, false));
    }
}
