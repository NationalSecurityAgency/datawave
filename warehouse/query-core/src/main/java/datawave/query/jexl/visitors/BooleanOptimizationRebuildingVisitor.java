package datawave.query.jexl.visitors;

import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.apache.log4j.Logger;

import datawave.query.util.Tuple2;

/**
 * Visit a JexlNode tree, optimizing the boolean logic.
 *
 */
public class BooleanOptimizationRebuildingVisitor extends RebuildingVisitor {
    private static final Logger log = Logger.getLogger(BooleanOptimizationRebuildingVisitor.class);

    public static ASTJexlScript optimize(JexlNode node) {
        if (node == null) {
            return null;
        }

        BooleanOptimizationRebuildingVisitor visitor = new BooleanOptimizationRebuildingVisitor();

        return (ASTJexlScript) node.jjtAccept(visitor, null);
    }

    @Override
    public Object visit(ASTAndNode node, Object data) {
        node = (ASTAndNode) copy(node);
        if (hasChildOr(node)) {
            ASTOrNode orNode = new ASTOrNode(ParserTreeConstants.JJTORNODE);
            orNode.image = node.image;

            return optimizeTree(node, orNode, data);
        } else {
            log.trace("nothing to optimize");
            return super.visit(node, data);
        }
    }

    private JexlNode optimizeTree(JexlNode currentNode, JexlNode newNode, Object data) {
        if ((currentNode instanceof ASTAndNode) && hasChildOr(currentNode)) {
            ASTAndNode andNode = new ASTAndNode(ParserTreeConstants.JJTANDNODE);
            andNode.image = currentNode.image;
            andNode.jjtSetParent(currentNode.jjtGetParent());

            ASTOrNode orNode = new ASTOrNode(ParserTreeConstants.JJTORNODE);
            orNode.image = currentNode.image;
            orNode.jjtSetParent(currentNode.jjtGetParent());

            Tuple2<JexlNode,JexlNode> nodes = prune(currentNode, andNode, orNode);

            JexlNode prunedNode = nodes.first();

            JexlNode toAttach = nodes.second();
            toAttach = TreeFlatteningRebuildingVisitor.flatten(toAttach);

            for (int i = 0; i < toAttach.jjtGetNumChildren(); i++) {
                JexlNode node = copy(prunedNode);
                JexlNode attach = (JexlNode) toAttach.jjtGetChild(i).jjtAccept(this, data);
                attach.jjtSetParent(node);
                node.jjtAddChild(attach, node.jjtGetNumChildren());
                newNode.jjtAddChild(node, newNode.jjtGetNumChildren());
                node.jjtSetParent(newNode);
            }
        } else {
            return currentNode;
        }

        if (newNode.jjtGetNumChildren() > 0) {
            return newNode;
        } else {
            log.trace("Optimization failed somewhere ... returning currentNode!");
            return currentNode;
        }
    }

    /**
     * Returns a tuple where the first element is the new node and the second element is the node that was pruned.
     *
     * @param currentNode
     *            the current node
     * @param newNode
     *            the new node
     * @param prunedNode
     *            the pruned node
     * @return a tuple
     */
    private Tuple2<JexlNode,JexlNode> prune(JexlNode currentNode, JexlNode newNode, JexlNode prunedNode) {
        for (int i = 0; i < currentNode.jjtGetNumChildren(); i++) {
            JexlNode child = currentNode.jjtGetChild(i);

            if (child instanceof ASTOrNode && child.jjtGetNumChildren() > prunedNode.jjtGetNumChildren()) {
                if (prunedNode.jjtGetNumChildren() > 0) {
                    newNode.jjtAddChild(prunedNode, newNode.jjtGetNumChildren());
                    prunedNode.jjtSetParent(newNode);
                }
                prunedNode = child;
            } else if (((child instanceof ASTReference) || (child instanceof ASTReferenceExpression) || child.getClass().equals(currentNode.getClass()))
                            && hasChildOr(child)) {
                Tuple2<JexlNode,JexlNode> nodes = prune(child, newNode, prunedNode);
                newNode = nodes.first();
                prunedNode = nodes.second();
            } else {
                newNode.jjtAddChild(child, newNode.jjtGetNumChildren());
                child.jjtSetParent(newNode);
            }
        }

        return new Tuple2<>(newNode, prunedNode);
    }

    /**
     * Returns true if there is a child OR (or OR directly inside ASTReference or ASTReferenceException).
     *
     * @param currentNode
     *            the current node
     * @return boolean
     */
    private boolean hasChildOr(JexlNode currentNode) {
        boolean foundChildOr = false;
        for (int i = 0; i < currentNode.jjtGetNumChildren(); i++) {
            JexlNode child = currentNode.jjtGetChild(i);

            if (child instanceof ASTOrNode) {
                return true;
            } else if ((child instanceof ASTReference) || (child instanceof ASTReferenceExpression)) {
                foundChildOr = foundChildOr || hasChildOr(child);
            } else if (child.getClass().equals(currentNode.getClass())) {
                foundChildOr = foundChildOr || hasChildOr(child);
            }
        }

        return foundChildOr;
    }
}
