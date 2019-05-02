package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * For a basic set of jexl queries assert that ranges are correctly coalesced
 */
public class RangeCoalescingVisitorTest {
    
    @Test
    public void testCoalesceTwoRanges() throws ParseException {
        String originalQuery = "NUM >= '+aE2' && NUM <= '+aE5'";
        String expectedQuery = "(NUM >= '+aE2' && NUM <= '+aE5')";
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);
        ASTJexlScript newScript = RangeCoalescingVisitor.coalesceRanges(script);
        
        String newQuery = JexlStringBuildingVisitor.buildQuery(newScript);
        assertEquals("Expected " + expectedQuery + " but was " + newQuery, expectedQuery, newQuery);
    }
    
    @Test
    public void testCoalesceTwoRangesWithAnchor() throws ParseException {
        String originalQuery = "FOO == 'ba' && NUM >= '+aE2' && NUM <= '+aE5'";
        String expectedQuery = "FOO == 'ba' && (NUM >= '+aE2' && NUM <= '+aE5')";
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);
        ASTJexlScript newScript = RangeCoalescingVisitor.coalesceRanges(script);
        
        String newQuery = JexlStringBuildingVisitor.buildQuery(newScript);
        assertEquals("Expected " + expectedQuery + " but was " + newQuery, expectedQuery, newQuery);
    }
    
    @Test
    public void testCoalesceTwoRangesWithOneRangeInNestedAnd() throws ParseException {
        String originalQuery = "(NUM > '+aE1' && FOO == 'ba') && NUM < '+aE5'";
        String expectedQuery = "FOO == 'ba' && (NUM > '+aE1' && NUM < '+aE5')";
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);
        ASTJexlScript newScript = RangeCoalescingVisitor.coalesceRanges(script);
        
        String newQuery = JexlStringBuildingVisitor.buildQuery(newScript);
        assertEquals("Expected " + expectedQuery + " but was " + newQuery, expectedQuery, newQuery);
    }
    
    @Test
    public void testCoalesceRangeWithAnchorTerm() throws ParseException {
        String originalQuery = "(NUM >= '+aE1' && NUM <= '+aE4') && TACO == 'tacocat'";
        // Ordering of query terms is not deterministic
        String expectedQuery = "TACO == 'tacocat' && (NUM >= '+aE1' && NUM <= '+aE4')";
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);
        ASTJexlScript newScript = RangeCoalescingVisitor.coalesceRanges(script);
        
        String newQuery = JexlStringBuildingVisitor.buildQuery(newScript);
        assertEquals("Expected " + expectedQuery + " but was " + newQuery, expectedQuery, newQuery);
    }
    
    @Test
    public void testCoalesceRangeWithAnchorTerm2() throws ParseException {
        String originalQuery = "(FOO >= 'bar' && FOO <= 'bas') && TACO == 'tacocat'";
        // Ordering of query terms is not deterministic
        String expectedQuery = "TACO == 'tacocat' && (FOO >= 'bar' && FOO <= 'bas')";
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);
        ASTJexlScript newScript = RangeCoalescingVisitor.coalesceRanges(script);
        
        String newQuery = JexlStringBuildingVisitor.buildQuery(newScript);
        assertEquals("Expected " + expectedQuery + " but was " + newQuery, expectedQuery, newQuery);
    }
    
    @Test
    public void testCoalesceTwoRangesWithAnchorTerm() throws ParseException {
        String originalQuery = "(NUM >= '+aE1' && NUM <= '+aE4') && TACO == 'tacocat' && (FOO >= 'bar' && FOO <= 'bas')";
        // Ordering of query terms is not deterministic, assert both possibilities
        String expectedQuery1 = "TACO == 'tacocat' && (FOO >= 'bar' && FOO <= 'bas') && (NUM >= '+aE1' && NUM <= '+aE4')";
        String expectedQuery2 = "TACO == 'tacocat' && (NUM >= '+aE1' && NUM <= '+aE4') && (FOO >= 'bar' && FOO <= 'bas')";
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);
        ASTJexlScript newScript = RangeCoalescingVisitor.coalesceRanges(script);
        
        String newQuery = JexlStringBuildingVisitor.buildQuery(newScript);
        assertTrue("Expected " + expectedQuery1 + " or " + expectedQuery2 + " but was " + newQuery,
                        (expectedQuery1.equals(newQuery) || expectedQuery2.equalsIgnoreCase(newQuery)));
    }
    
    @Test
    public void testCoalesceTwoRangesWithAnchorTerm2() throws ParseException {
        String originalQuery = "NUM >= '+aE1' && (NUM <= '+aE4' && TACO == 'tacocat') && (FOO >= 'bar' && FOO <= 'bas')";
        // Ordering of query terms is not deterministic, assert both possibilities
        String expectedQuery1 = "TACO == 'tacocat' && (FOO >= 'bar' && FOO <= 'bas') && (NUM >= '+aE1' && NUM <= '+aE4')";
        String expectedQuery2 = "TACO == 'tacocat' && (NUM >= '+aE1' && NUM <= '+aE4') && (FOO >= 'bar' && FOO <= 'bas')";
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);
        ASTJexlScript newScript = RangeCoalescingVisitor.coalesceRanges(script);
        
        String newQuery = JexlStringBuildingVisitor.buildQuery(newScript);
        assertTrue("Expected " + expectedQuery1 + " or " + expectedQuery2 + " but was " + newQuery,
                        (expectedQuery1.equals(newQuery) || expectedQuery2.equalsIgnoreCase(newQuery)));
    }
    
    @Test
    public void testCoalesceManyNestedAndUnorderedTermsWithAnchorTerm() throws ParseException {
        // Should extract single NUM3 term, coalesce NUM and NUM2 terms into
        String originalQuery = "TACO == 'tacocat' && (NUM >= '+aE1' && NUM2 >= '+aE2' && NUM3 == '+aE3' && NUM2 <= '+aE4' && NUM <= '+aE5')";
        // Ordering of query terms is not deterministic, assert both possibilities
        String expectedQuery1 = "TACO == 'tacocat' && NUM3 == '+aE3' && (NUM2 >= '+aE2' && NUM2 <= '+aE4') && (NUM >= '+aE1' && NUM <= '+aE5')";
        String expectedQuery2 = "TACO == 'tacocat' && NUM3 == '+aE3' && (NUM >= '+aE1' && NUM <= '+aE5') && (NUM2 >= '+aE2' && NUM2 <= '+aE4')";
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);
        ASTJexlScript newScript = RangeCoalescingVisitor.coalesceRanges(script);
        
        String newQuery = JexlStringBuildingVisitor.buildQuery(newScript);
        assertTrue("Expected [" + expectedQuery1 + "] or [" + expectedQuery2 + "] but was " + newQuery,
                        (expectedQuery1.equals(newQuery) || expectedQuery2.equalsIgnoreCase(newQuery)));
    }
}
