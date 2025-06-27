package datawave.query.jexl.visitors;

import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTNotNode;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;

import datawave.query.jexl.nodes.QueryPropertyMarker;

/**
 * Visitors that modify the query tree will frequently leave behind extra reference expressions.
 * <p>
 * This visitor detects and eliminates adjacent reference expressions (double parens) in addition to eliminating references with a single child except marker
 * nodes and negations.
 */
public class RemoveExtraReferenceExpressionsVisitor extends BaseVisitor {

    public static JexlNode remove(JexlNode node) {
        RemoveExtraReferenceExpressionsVisitor visitor = new RemoveExtraReferenceExpressionsVisitor();
        node.jjtAccept(visitor, null);
        return node;
    }

    @Override
    public Object visit(ASTAndNode node, Object data) {
        if (QueryPropertyMarker.findInstance(node).isAnyType()) {
            return data;
        }
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        node.childrenAccept(this, data);

        // need a parent and a child to operate
        if (node.jjtGetParent() != null && node.jjtGetNumChildren() == 1) {
            JexlNode parent = node.jjtGetParent();
            JexlNode child = node.jjtGetChild(0);

            if (parent instanceof ASTReferenceExpression) {
                // eliminate double parens
                JexlNodes.swap(parent, node, child);
                //  @formatter:off
            } else if (
                    !(parent instanceof ASTNotNode) &&
                    !(child instanceof ASTOrNode || child instanceof ASTAndNode) &&
                    !QueryPropertyMarker.findInstance(child).isAnyType()
            ) {
                //  @formatter:on
                // eliminate parens with a single child
                JexlNodes.swap(node.jjtGetParent(), node, node.jjtGetChild(0));
            }
        }
        return data;
    }
}
