package datawave.query.jexl.visitors;

import datawave.query.util.Tuple2;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.apache.log4j.Logger;

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
        if (hasChildOr(node)) {
            return optimizeTree(node, data);
        } else {
            log.trace("nothing to optimize");
            return super.visit(node, data);
        }
    }
    
    protected JexlNode optimizeTree(JexlNode currentNode, Object data) {
        if (currentNode instanceof ASTAndNode) {
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
            ASTOrNode newNode = new ASTOrNode(ParserTreeConstants.JJTORNODE);
            newNode.image = currentNode.image;
            
            for (int i = 0; i < toAttach.jjtGetNumChildren(); i++) {
                JexlNode node = copy(prunedNode);
                JexlNode attach = (JexlNode) toAttach.jjtGetChild(i).jjtAccept(this, data);
                attach.jjtSetParent(node);
                node.jjtAddChild(attach, node.jjtGetNumChildren());
                newNode.jjtAddChild(node, newNode.jjtGetNumChildren());
                node.jjtSetParent(newNode);
            }
            return newNode;
        }
        return currentNode;
    }
    
    /**
     * Returns a tuple where the first element is the new node and the second element is the node that was pruned.
     * 
     * @param currentNode
     * @param newNode
     * @param prunedNode
     */
    protected Tuple2<JexlNode,JexlNode> prune(JexlNode currentNode, JexlNode newNode, JexlNode prunedNode) {
        for (int i = 0; i < currentNode.jjtGetNumChildren(); i++) {
            JexlNode child = currentNode.jjtGetChild(i);
            
            if (child instanceof ASTOrNode && child.jjtGetNumChildren() > prunedNode.jjtGetNumChildren()) {
                if (prunedNode.jjtGetNumChildren() > 0) {
                    newNode.jjtAddChild(prunedNode, newNode.jjtGetNumChildren());
                    prunedNode.jjtSetParent(newNode);
                }
                prunedNode = child;
            } else if (isWrapperNodeOrSameClass(child, currentNode) && hasChildOr(child)) {
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
    
    // Return whether or not if the provided node has a child OR, wrapped or otherwise.
    private boolean hasChildOr(JexlNode currentNode) {
        for (int i = 0; i < currentNode.jjtGetNumChildren(); i++) {
            JexlNode child = currentNode.jjtGetChild(i);
            if (child instanceof ASTOrNode) {
                return true;
            } else if (isWrapperNodeOrSameClass(child, currentNode) && hasChildOr(child)) {
                return true;
            }
        }
        return false;
    }
    
    // Return true if the node is a reference, reference expression, or the same class as other.
    private boolean isWrapperNodeOrSameClass(JexlNode node, JexlNode other) {
        return node instanceof ASTReference || node instanceof ASTReferenceExpression || node.getClass().equals(other.getClass());
    }
}
