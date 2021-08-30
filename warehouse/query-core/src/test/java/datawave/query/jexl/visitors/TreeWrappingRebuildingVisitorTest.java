package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.log4j.Logger;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TreeWrappingRebuildingVisitorTest {
    
    private static final Logger log = Logger.getLogger(TreeWrappingRebuildingVisitorTest.class);
    
    /**
     * Test that a new, top-level, unwrapped OR node is wrapped.
     */
    @Test
    public void testTopLevelUnwrappedORNode() throws ParseException {
        assertResult("FOO == 'bar' OR FOO == 'bat'", "(FOO == 'bar' OR FOO == 'bat')");
    }
    
    /**
     * Test that a new, top-level, unwrapped OR node is wrapped even with a wrapped child OR node.
     */
    @Test
    public void testTopLevelUnwrappedNodeWithChildOr() throws ParseException {
        assertResult("NAME = 'batman' OR (FOO == 'bar' OR FOO == 'bat')", "(NAME = 'batman' OR (FOO == 'bar' OR FOO == 'bat'))");
    }
    
    /**
     * Test that a wrapped OR node is not re-wrapped.
     */
    @Test
    public void testWrappedORNode() throws ParseException {
        assertResult("(FOO == 'bar' OR FOO == 'bat')", "(FOO == 'bar' OR FOO == 'bat')");
    }
    
    private void assertResult(String expected, String original) throws ParseException {
        ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery(expected);
        ASTJexlScript originalScript = JexlASTHelper.parseJexlQuery(original);
        
        ASTJexlScript flattened = TreeWrappingRebuildingVisitor.wrap(originalScript);
        
        assertScriptEquality(flattened, expected);
        assertLineage(flattened);
        
        assertScriptEquality(originalScript, original);
        assertLineage(originalScript);
        
        assertTrue(TreeEqualityVisitor.isEqual(expectedScript, flattened));
    }
    
    private void assertScriptEquality(ASTJexlScript actual, String expected) throws ParseException {
        ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery(expected);
        TreeEqualityVisitor.Comparison comparison = TreeEqualityVisitor.checkEquality(expectedScript, actual);
        if (!comparison.isEqual()) {
            log.error("Expected " + JexlStringBuildingVisitor.buildQuery(expectedScript) + "\n" + PrintingVisitor.formattedQueryString(expectedScript));
            log.error("Actual " + JexlStringBuildingVisitor.buildQuery(actual) + "\n" + PrintingVisitor.formattedQueryString(actual));
        }
        assertTrue(comparison.getReason(), comparison.isEqual());
    }
    
    private void assertLineage(JexlNode node) {
        assertTrue(JexlASTHelper.validateLineage(node, true));
    }
}
