package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.log4j.Logger;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class IsNotNullIntentVisitorTest {
    
    private static final Logger log = Logger.getLogger(IsNotNullIntentVisitorTest.class);
    
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
    
    private void assertResult(String original, String expected) throws ParseException {
        ASTJexlScript originalScript = JexlASTHelper.parseJexlQuery(original);
        ASTJexlScript actual = IsNotNullIntentVisitor.fixNotNullIntent(originalScript);
     
        // Verify the resulting script is as expected and has a valid lineage.
        assertScriptEquality(actual, expected);
        assertLineage(actual);
        
        // Verify the original script was not modified and has a valid lineage.
        assertScriptEquality(originalScript, original);
        assertLineage(originalScript);
    }
    
    private void assertScriptEquality(JexlNode actual, String expected) throws ParseException {
        ASTJexlScript actualScript = JexlASTHelper.parseJexlQuery(JexlStringBuildingVisitor.buildQuery(actual));
        ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery(expected);
        TreeEqualityVisitor.Reason reason = new TreeEqualityVisitor.Reason();
        boolean equal = TreeEqualityVisitor.isEqual(expectedScript, actualScript, reason);
        if (!equal) {
            log.error("Expected " + PrintingVisitor.formattedQueryString(expectedScript));
            log.error("Actual " + PrintingVisitor.formattedQueryString(actualScript));
        }
        assertTrue(reason.reason, equal);
    }
    
    private void assertLineage(JexlNode node) {
        assertTrue(JexlASTHelper.validateLineage(node, true));
    }
}
