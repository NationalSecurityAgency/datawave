package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.log4j.Logger;
import org.junit.Test;

import static datawave.query.Constants.SHARD_DAY_HINT;
import static org.junit.Assert.assertTrue;

public class DateIndexCleanupVisitorTest {
    
    private static final Logger log = Logger.getLogger(DateIndexCleanupVisitorTest.class);
    
    @Test
    public void testConjunction() throws ParseException {
        String original = "FOO == 'bar' && (" + SHARD_DAY_HINT + " = 'hello,world')";
        String expected = "FOO == 'bar'";
        testCleanup(original, expected);
    }
    
    @Test
    public void testDisjunction() throws ParseException {
        String original = "FOO == 'bar' || (" + SHARD_DAY_HINT + " = 'hello,world')";
        String expected = "FOO == 'bar'";
        testCleanup(original, expected);
    }
    
    @Test
    public void testDuplicateHints() throws ParseException {
        String original = "FOO == 'bar' || (" + SHARD_DAY_HINT + " = 'hello,world' && " + SHARD_DAY_HINT + " = 'hello,world')";
        String expected = "FOO == 'bar'";
        testCleanup(original, expected);
    }
    
    @Test
    public void testConjunctionHint() throws ParseException {
        String original = "((FOO == 'bar' || (" + SHARD_DAY_HINT + " = 'hello,world')) && (" + SHARD_DAY_HINT + " = 'hello,world'))";
        String expected = "(FOO == 'bar')";
        testCleanup(original, expected);
    }
    
    @Test
    public void testOnlyHint() throws ParseException {
        String original = "(" + SHARD_DAY_HINT + " = 'hello,world')";
        String expected = "";
        testCleanup(original, expected);
    }
    
    private void testCleanup(String original, String expected) throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(original);
        
        ASTJexlScript cleaned = DateIndexCleanupVisitor.cleanup(script);
        
        // Verify the result script is as expected, with a valid lineage.
        assertScriptEquality(cleaned, expected);
        assertLineage(cleaned);
        
        // Verify the original script was not modified, and has a valid lineage.
        assertScriptEquality(script, original);
        assertLineage(script);
    }
    
    private void assertScriptEquality(JexlNode actual, String expected) throws ParseException {
        ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery(expected);
        TreeEqualityVisitor.Comparison comparison = TreeEqualityVisitor.checkEquality(expectedScript, actual);
        if (!comparison.isEqual()) {
            log.error("Expected " + PrintingVisitor.formattedQueryString(expectedScript));
            log.error("Actual " + PrintingVisitor.formattedQueryString(actual));
        }
        assertTrue(comparison.getReason(), comparison.isEqual());
    }
    
    private void assertLineage(JexlNode node) {
        assertTrue(JexlASTHelper.validateLineage(node, true));
    }
}
