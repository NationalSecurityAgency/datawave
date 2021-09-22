package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import datawave.test.JexlNodeAssert;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Test;

public class TreeWrappingRebuildingVisitorTest {
    
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
        
        JexlNodeAssert.assertThat(flattened).isEqualTo(expectedScript).hasValidLineage();
        
        JexlNodeAssert.assertThat(originalScript).isEqualTo(original).hasValidLineage();
    }
}
