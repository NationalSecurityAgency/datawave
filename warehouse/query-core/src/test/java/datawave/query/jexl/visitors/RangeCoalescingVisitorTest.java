package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.log4j.Logger;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * For a basic set of jexl queries assert that ranges are correctly coalesced
 */
public class RangeCoalescingVisitorTest {
    
    private static final Logger log = Logger.getLogger(RangeCoalescingVisitor.class);
    
    @Test
    public void testCoalesceTwoRanges() throws ParseException {
        String originalQuery = "NUM >= '+aE2' && NUM <= '+aE5'";
        String expectedQuery = "(NUM >= '+aE2' && NUM <= '+aE5')";
        
        assertResult(originalQuery, expectedQuery);
    }
    
    @Test
    public void testCoalesceTwoRangesWithAnchor() throws ParseException {
        String originalQuery = "FOO == 'ba' && NUM >= '+aE2' && NUM <= '+aE5'";
        String expectedQuery = "FOO == 'ba' && (NUM >= '+aE2' && NUM <= '+aE5')";
        
        assertResult(originalQuery, expectedQuery);
    }
    
    @Test
    public void testCoalesceTwoRangesWithOneRangeInNestedAnd() throws ParseException {
        String originalQuery = "(NUM > '+aE1' && FOO == 'ba') && NUM < '+aE5'";
        String expectedQuery = "FOO == 'ba' && (NUM > '+aE1' && NUM < '+aE5')";
        
        assertResult(originalQuery, expectedQuery);
    }
    
    @Test
    public void testCoalesceRangeWithAnchorTerm() throws ParseException {
        String originalQuery = "(NUM >= '+aE1' && NUM <= '+aE4') && TACO == 'tacocat'";
        // Ordering of query terms is not deterministic
        String expectedQuery = "TACO == 'tacocat' && (NUM >= '+aE1' && NUM <= '+aE4')";
        
        assertResult(originalQuery, expectedQuery);
    }
    
    @Test
    public void testCoalesceRangeWithAnchorTerm2() throws ParseException {
        String originalQuery = "(FOO >= 'bar' && FOO <= 'bas') && TACO == 'tacocat'";
        // Ordering of query terms is not deterministic
        String expectedQuery = "TACO == 'tacocat' && (FOO >= 'bar' && FOO <= 'bas')";
        
        assertResult(originalQuery, expectedQuery);
    }
    
    @Test
    public void testCoalesceTwoRangesWithAnchorTerm() throws ParseException {
        String originalQuery = "(NUM >= '+aE1' && NUM <= '+aE4') && TACO == 'tacocat' && (FOO >= 'bar' && FOO <= 'bas')";
        // Ordering of query terms is not deterministic, assert both possibilities
        String expectedQuery1 = "TACO == 'tacocat' && (FOO >= 'bar' && FOO <= 'bas') && (NUM >= '+aE1' && NUM <= '+aE4')";
        String expectedQuery2 = "TACO == 'tacocat' && (NUM >= '+aE1' && NUM <= '+aE4') && (FOO >= 'bar' && FOO <= 'bas')";
        
        assertResult(originalQuery, expectedQuery1, expectedQuery2);
    }
    
    @Test
    public void testCoalesceTwoRangesWithAnchorTerm2() throws ParseException {
        String originalQuery = "NUM >= '+aE1' && (NUM <= '+aE4' && TACO == 'tacocat') && (FOO >= 'bar' && FOO <= 'bas')";
        // Ordering of query terms is not deterministic, assert both possibilities
        String expectedQuery1 = "TACO == 'tacocat' && (FOO >= 'bar' && FOO <= 'bas') && (NUM >= '+aE1' && NUM <= '+aE4')";
        String expectedQuery2 = "TACO == 'tacocat' && (NUM >= '+aE1' && NUM <= '+aE4') && (FOO >= 'bar' && FOO <= 'bas')";
        
        assertResult(originalQuery, expectedQuery1, expectedQuery2);
    }
    
    @Test
    public void testCoalesceManyNestedAndUnorderedTermsWithAnchorTerm() throws ParseException {
        // Should extract single NUM3 term, coalesce NUM and NUM2 terms into
        String originalQuery = "TACO == 'tacocat' && (NUM >= '+aE1' && NUM2 >= '+aE2' && NUM3 == '+aE3' && NUM2 <= '+aE4' && NUM <= '+aE5')";
        // Ordering of query terms is not deterministic, assert both possibilities
        String expectedQuery1 = "TACO == 'tacocat' && NUM3 == '+aE3' && (NUM2 >= '+aE2' && NUM2 <= '+aE4') && (NUM >= '+aE1' && NUM <= '+aE5')";
        String expectedQuery2 = "TACO == 'tacocat' && NUM3 == '+aE3' && (NUM >= '+aE1' && NUM <= '+aE5') && (NUM2 >= '+aE2' && NUM2 <= '+aE4')";
        
        assertResult(originalQuery, expectedQuery1, expectedQuery2);
    }
    
    private void assertResult(String original, String expected) throws ParseException {
        ASTJexlScript originalScript = JexlASTHelper.parseJexlQuery(original);
        
        ASTJexlScript actualScript = RangeCoalescingVisitor.coalesceRanges(originalScript);
        
        // Verify that the resulting script is as expected with a valid lineage.
        assertScriptEquality(actualScript, expected);
        assertLineage(actualScript);
    
        // Verify the original script was not modified and has a valid lineage.
        assertScriptEquality(originalScript, original);
        assertLineage(originalScript);
    }
    
    private void assertResult(String original, String expected, String altExpected) throws ParseException {
        ASTJexlScript originalScript = JexlASTHelper.parseJexlQuery(original);
        
        ASTJexlScript actualScript = RangeCoalescingVisitor.coalesceRanges(originalScript);
    
        // Verify that the resulting script is as expected with a valid lineage.
        assertScriptEquality(actualScript, expected, altExpected);
        assertLineage(actualScript);
    
        // Verify the original script was not modified and has a valid lineage.
        assertScriptEquality(originalScript, original);
        assertLineage(originalScript);
    }
    
    private void assertScriptEquality(ASTJexlScript actualScript, String expected) throws ParseException {
        ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery(expected);
        
        TreeEqualityVisitor.Reason reason = new TreeEqualityVisitor.Reason();
        boolean equal = TreeEqualityVisitor.isEqual(expectedScript, actualScript, reason);
        if (!equal) {
            log.error("Expected " + PrintingVisitor.formattedQueryString(expectedScript));
            log.error("Actual " + PrintingVisitor.formattedQueryString(actualScript));
        }
        assertTrue(reason.reason, equal);
    }
    
    private void assertScriptEquality(ASTJexlScript actualScript, String expected, String altExpected) throws ParseException {
        ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery(expected);
        ASTJexlScript altExpectedScript = JexlASTHelper.parseJexlQuery(altExpected);
        
        TreeEqualityVisitor.Reason reason = new TreeEqualityVisitor.Reason();
        TreeEqualityVisitor.Reason altReason = new TreeEqualityVisitor.Reason();
        boolean equal = TreeEqualityVisitor.isEqual(expectedScript, actualScript, reason);
        boolean altEqual = TreeEqualityVisitor.isEqual(altExpectedScript, actualScript, altReason);
        if (!equal && !altEqual) {
            log.error("Expected " + PrintingVisitor.formattedQueryString(expectedScript));
            log.error("Alt Expected: " + PrintingVisitor.formattedQueryString(altExpectedScript));
            log.error("Actual " + PrintingVisitor.formattedQueryString(actualScript));
        }
        
        assertTrue(equal || altEqual);
    }
    
    private void assertLineage(JexlNode node) {
        assertTrue(JexlASTHelper.validateLineage(node, true));
    }
}
