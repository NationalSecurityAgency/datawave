package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTNullLiteral;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.JexlNodes;
import org.apache.commons.jexl2.parser.ParserTreeConstants;

import java.util.ArrayList;
import java.util.List;

/**
 * This visitor replaces any occurrences of <code>FIELD =~'.*?'</code> with the more efficient equivalent <code>FIELD != null</code>.
 */
public class IsNotNullIntentVisitor extends BaseVisitor {
    
    /**
     * Apply this visitor to the provided node and return the result.
     * 
     * @param node
     *            a JexlNode
     * @return the same node
     */
    public static <T extends JexlNode> T fixNotNullIntent(T node) {
        node.jjtAccept(new IsNotNullIntentVisitor(), null);
        return node;
    }
    
    @Override
    public Object visit(ASTERNode node, Object data) {
        // If the ER node is meant to match any string, it can be replaced with FIELD != null.
        Object value = JexlASTHelper.getLiteralValue(node);
        if (".*?".equals(value)) {
            List<JexlNode> children = new ArrayList<>();
            children.add(node.jjtGetChild(0));
            children.add(new ASTNullLiteral(ParserTreeConstants.JJTNULLLITERAL));
            
            JexlNode neNode = new ASTNENode(ParserTreeConstants.JJTNENODE);
            JexlNodes.children(neNode, children.toArray(new JexlNode[0]));
            
            JexlNodes.replaceChild(node.jjtGetParent(), node, neNode);
        }
        return data;
    }
}
