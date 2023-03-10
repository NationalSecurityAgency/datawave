package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTNullLiteral;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParserTreeConstants;

/**
 * This visitor examines a query tree and replaces any occurrences of FIELD =~'.*?' with the more efficient equivalent FIELD != null.
 */
public class IsNotNullIntentVisitor extends RebuildingVisitor {
    
    /**
     * Apply this visitor to the provided script and return the result.
     * 
     * @param script
     *            the script to apply
     * @return the result
     */
    public static <T extends JexlNode> T fixNotNullIntent(T script) {
        IsNotNullIntentVisitor visitor = new IsNotNullIntentVisitor();
        
        // noinspection unchecked
        return (T) script.jjtAccept(visitor, null);
    }
    
    @Override
    public Object visit(ASTERNode node, Object data) {
        // If the ER node is meant to match any string, it can be replaced with FIELD != null.
        Object value = JexlASTHelper.getLiteralValue(node);
        if (".*?".equals(value)) {
            ASTNENode neNode = new ASTNENode(ParserTreeConstants.JJTNENODE);
            neNode.jjtAddChild(node.jjtGetChild(0), 0);
            neNode.jjtAddChild(new ASTNullLiteral(ParserTreeConstants.JJTNULLLITERAL), 1);
            return super.visit(neNode, data);
        }
        return super.visit(node, data);
    }
}
