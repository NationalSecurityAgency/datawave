package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.JexlNodes;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class EnsureReferenceNodesVisitorTest {
    
    @Test
    public void testInjectReferenceNode() {
        // Build a simple AST that does not have a reference node
        JexlNode eqNode = JexlNodeFactory.buildEQNode("F", "v");
        JexlNode expr = new ASTReferenceExpression(ParserTreeConstants.JJTREFERENCEEXPRESSION);
        ASTJexlScript script = new ASTJexlScript(ParserTreeConstants.JJTJEXLSCRIPT);
        JexlNodes.children(expr, eqNode);
        JexlNodes.children(script, expr);
        
        // assert initial tree state
        assertEquals(1, script.jjtGetNumChildren());
        JexlNode child = script.jjtGetChild(0);
        assertEquals(ParserTreeConstants.JJTREFERENCEEXPRESSION, JexlNodes.id(child));
        assertEquals(1, child.jjtGetNumChildren());
        JexlNode grandChild = child.jjtGetChild(0);
        assertEquals(ParserTreeConstants.JJTEQNODE, JexlNodes.id(grandChild));
        
        // assert visible query
        String expectedQuery = "(F == 'v')";
        String builtQuery = JexlStringBuildingVisitor.buildQueryWithoutParse(script);
        assertEquals(expectedQuery, builtQuery);
        
        // inject reference nodes
        script = (ASTJexlScript) EnsureReferenceNodesVisitor.ensureReferences(script);
        
        // assert lineage
        assertTrue(JexlASTHelper.validateLineage(script, true));
        
        // visible query should not have changed
        builtQuery = JexlStringBuildingVisitor.buildQueryWithoutParse(script);
        assertEquals(expectedQuery, builtQuery);
        
        // assert that the backing AST now has a reference expression
        assertEquals(1, script.jjtGetNumChildren());
        child = script.jjtGetChild(0);
        assertEquals(ParserTreeConstants.JJTREFERENCE, JexlNodes.id(child));
        assertEquals(1, child.jjtGetNumChildren());
        grandChild = child.jjtGetChild(0);
        assertEquals(ParserTreeConstants.JJTREFERENCEEXPRESSION, JexlNodes.id(grandChild));
        assertEquals(1, grandChild.jjtGetNumChildren());
        JexlNode greatGrandChild = grandChild.jjtGetChild(0);
        assertEquals(ParserTreeConstants.JJTEQNODE, JexlNodes.id(greatGrandChild));
    }
}
