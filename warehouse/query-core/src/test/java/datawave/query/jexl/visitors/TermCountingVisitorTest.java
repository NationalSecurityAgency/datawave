package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TermCountingVisitorTest {
    
    @Test
    public void testRange() throws ParseException {
        String query = "((BoundedRange = true) && (NUM >= '+aE1' && NUM <= '+aE4'))";
        testCounts(query, 1);
    }
    
    @Test
    public void testNegatedRange() throws ParseException {
        String query = "!((BoundedRange = true) && (NUM >= '+aE1' && NUM <= '+aE4'))";
        testCounts(query, 1);
    }
    
    @Test
    public void testRangePlusTerm() throws ParseException {
        String query = "((BoundedRange = true) && (NUM >= '+aE1' && NUM <= '+aE4')) && FOO == 'bar'";
        testCounts(query, 2);
    }
    
    @Test
    public void testUnboundedRangeAndTerm() throws ParseException {
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
        String query = "((BoundedRange = true) && (NUM >= '+aE1' && NUM <= '+aE4')) || ((BoundedRange = true) && (NUM >= '+aE7' && NUM <= '+aE9'))";
        // When the same term is present in two distinct ranges, it should be counted as two separate terms.
        testCounts(query, 2);
    }
    
    @Test
    public void testTwoRangesPlusTerm() throws ParseException {
        String query = "((BoundedRange = true) && (NUM >= '+aE1' && NUM <= '+aE4')) && FOO == 'bar' && ((BoundedRange = true) && (NUM2 >= '+aE7' && NUM2 <= '+aE9'))";
        testCounts(query, 3);
    }
    
    @Test
    public void testUnboundedTwoRangesPlusTerm_UnorderedWrapping() throws ParseException {
        String query = "NUM >= '+aE1' && (NUM <= '+aE4' && FOO == 'bar') && (NUM2 >= '+aE7' && NUM2 <= '+aE9')";
        testCounts(query, 5);
    }
    
    @Test
    public void testNestedRangeWithinConjunction() throws ParseException {
        String query = "((BoundedRange = true) && (NUM >= '+aE1' && NUM <= '+aE4')) && (FOO == 'bar' || ((BoundedRange = true) && (NUM2 >= '+aE7' && NUM2 <= '+aE9')))";
        testCounts(query, 3);
    }
    
    @Test
    public void testNestedRangeWithinDisjunction() throws ParseException {
        String query = "((BoundedRange = true) && (NUM >= '+aE1' && NUM <= '+aE4')) || (FOO == 'bar' && ((BoundedRange = true) && (NUM2 >= '+aE7' && NUM2 <= '+aE9')))";
        testCounts(query, 3);
    }
    
    @Test
    public void testQueryWithFunction() throws ParseException {
        String query = "((BoundedRange = true) && (NUM >= '+aE1' && NUM <= '+aE4')) || filter:includeRegex(FOO, '.*bar.*')";
        testCounts(query, 2);
    }
    
    private void testCounts(String query, int expectedCount) throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        int count = TermCountingVisitor.countTerms(script);
        assertEquals(expectedCount, count);
    }
}
