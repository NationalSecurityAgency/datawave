package datawave.query.jexl.visitors;

import org.apache.commons.jexl2.parser.ASTAdditiveNode;
import org.apache.commons.jexl2.parser.ASTAdditiveOperator;
import org.apache.commons.jexl2.parser.ASTAmbiguous;
import org.apache.commons.jexl2.parser.ASTArrayAccess;
import org.apache.commons.jexl2.parser.ASTArrayLiteral;
import org.apache.commons.jexl2.parser.ASTAssignment;
import org.apache.commons.jexl2.parser.ASTBitwiseAndNode;
import org.apache.commons.jexl2.parser.ASTBitwiseComplNode;
import org.apache.commons.jexl2.parser.ASTBitwiseOrNode;
import org.apache.commons.jexl2.parser.ASTBitwiseXorNode;
import org.apache.commons.jexl2.parser.ASTBlock;
import org.apache.commons.jexl2.parser.ASTConstructorNode;
import org.apache.commons.jexl2.parser.ASTDivNode;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTEmptyFunction;
import org.apache.commons.jexl2.parser.ASTFalseNode;
import org.apache.commons.jexl2.parser.ASTFloatLiteral;
import org.apache.commons.jexl2.parser.ASTForeachStatement;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTIfStatement;
import org.apache.commons.jexl2.parser.ASTIntegerLiteral;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTMapEntry;
import org.apache.commons.jexl2.parser.ASTMapLiteral;
import org.apache.commons.jexl2.parser.ASTMethodNode;
import org.apache.commons.jexl2.parser.ASTModNode;
import org.apache.commons.jexl2.parser.ASTMulNode;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.ASTNullLiteral;
import org.apache.commons.jexl2.parser.ASTNumberLiteral;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.ASTReturnStatement;
import org.apache.commons.jexl2.parser.ASTSizeFunction;
import org.apache.commons.jexl2.parser.ASTSizeMethod;
import org.apache.commons.jexl2.parser.ASTStringLiteral;
import org.apache.commons.jexl2.parser.ASTTernaryNode;
import org.apache.commons.jexl2.parser.ASTTrueNode;
import org.apache.commons.jexl2.parser.ASTUnaryMinusNode;
import org.apache.commons.jexl2.parser.ASTVar;
import org.apache.commons.jexl2.parser.ASTWhileStatement;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.JexlNodes;
import org.apache.commons.jexl2.parser.ParserTreeConstants;

/**
 * Input: ((FOO == 'bar')) Output: (FOO == 'bar')
 *
 * Preferable to {@link TreeFlatteningRebuildingVisitor#flattenAll(JexlNode)} to preserve reference expressions within marker nodes, for example bounded ranges.
 *
 * Note: does not operate on a copy, operates directly on the tree.
 */
public class RemoveExtraParensVisitor extends BaseVisitor {
    
    public static JexlNode remove(JexlNode node) {
        RemoveExtraParensVisitor visitor = new RemoveExtraParensVisitor();
        visitor.visit(node, null);
        return node;
    }
    
    /**
     * Can remove a paren if a hierarchy exists like
     *
     * RefExpr -{@literal >} Ref -{@literal >} RefExpr
     *
     * @param node
     *            an ASTReferenceExpression node
     * @param data
     *            don't need it
     * @return
     */
    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        // Operate on a plain JexlNode to avoid class cast exception
        JexlNode target = node;
        boolean changed;
        do {
            changed = false;
            
            if (isDoubleWrapped(target)) {
                // Replace this reference expression with the child reference node
                target = removeDoubleWrap(target);
                changed = true;
            }
            
            if (isDoubleReferenceExpression(target)) {
                target = removeMiddleNode(target);
                changed = true;
            }
        } while (changed);
        
        return super.visit(target, data);
    }
    
    @Override
    public Object visit(ASTReference node, Object data) {
        
        while (isDoubleReference(node)) {
            node = (ASTReference) removeMiddleNode(node);
        }
        
        return super.visit(node, data);
    }
    
    /**
     * Returns true if two reference expression nodes exist adjacent to each other in the tree hierarchy
     * 
     * @param node
     *            a Jexl node
     * @return true if this node is double wrapped
     */
    private boolean isDoubleWrapped(JexlNode node) {
        if (node.jjtGetParent() != null && isReferenceExpression(node) && hasSingleChild(node)) {
            
            JexlNode child = node.jjtGetChild(0);
            if (isReference(child) && hasSingleChild(child)) {
                
                JexlNode gc = child.jjtGetChild(0);
                if (isReferenceExpression(gc) && hasSingleChild(gc)) {
                    
                    // Confirm something exists within the parens
                    return gc.jjtGetNumChildren() > 0;
                }
            }
        }
        return false;
    }
    
    /**
     * Handles the case when two reference nodes exist in the tree hiearchy
     * 
     * @param node
     * @return
     */
    private boolean isDoubleReference(JexlNode node) {
        if (node.jjtGetParent() != null && isReference(node) && hasSingleChild(node)) {
            
            JexlNode child = node.jjtGetChild(0);
            return isReference(child) && hasSingleChild(child);
        }
        return false;
    }
    
    private boolean isDoubleReferenceExpression(JexlNode node) {
        if (node.jjtGetParent() != null && isReferenceExpression(node) && hasSingleChild(node)) {
            
            JexlNode child = node.jjtGetChild(0);
            return isReferenceExpression(child) && hasSingleChild(child);
        }
        return false;
    }
    
    /**
     * Assumes {@link #isDoubleWrapped(JexlNode)} has validated that the incoming node is the start of a double paren.
     *
     * Given a tree like A - B - C - D, remove nodes B and C to leave A - D
     * 
     * @param node
     *            a double wrapped Jexl node.
     * @return the unwrapped node, in this case a reference node
     */
    private JexlNode removeDoubleWrap(JexlNode node) {
        
        JexlNode parent = node.jjtGetParent();
        JexlNode child = node.jjtGetChild(0);
        
        // Execute two swaps to clear references to stale nodes
        JexlNodes.swap(parent, node, child);
        
        return child;
    }
    
    /**
     * Given a tree like A - B - C, remove node B to leave A - C
     *
     * @return
     */
    private JexlNode removeMiddleNode(JexlNode node) {
        JexlNode parent = node.jjtGetParent();
        JexlNode child = node.jjtGetChild(0);
        
        JexlNodes.swap(parent, node, child);
        node = null;
        
        return child;
    }
    
    private boolean hasSingleChild(JexlNode node) {
        return node.jjtGetNumChildren() == 1;
    }
    
    private boolean isReference(JexlNode node) {
        return JexlNodes.id(node) == ParserTreeConstants.JJTREFERENCE;
    }
    
    private boolean isReferenceExpression(JexlNode node) {
        return JexlNodes.id(node) == ParserTreeConstants.JJTREFERENCEEXPRESSION;
    }
    
    // Short circuit when visiting specific nodes
    
    @Override
    public Object visit(ASTBlock node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTAmbiguous node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTIfStatement node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTWhileStatement node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTForeachStatement node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTAssignment node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTTernaryNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTBitwiseOrNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTBitwiseXorNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTBitwiseAndNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTEQNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTNENode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTLTNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTGTNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTLENode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTGENode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTERNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTNRNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTAdditiveNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTAdditiveOperator node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTMulNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTDivNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTModNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTUnaryMinusNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTBitwiseComplNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTIdentifier node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTNullLiteral node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTTrueNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTFalseNode node, Object data) {
        return data;
    }
    
    public Object visit(ASTIntegerLiteral node, Object data) {
        return data;
    }
    
    public Object visit(ASTFloatLiteral node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTStringLiteral node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTArrayLiteral node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTMapLiteral node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTMapEntry node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTEmptyFunction node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTSizeFunction node, Object data) {
        return data;
    }
    
    // TODO -- ?
    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTMethodNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTSizeMethod node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTConstructorNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTArrayAccess node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTReturnStatement node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTVar node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTNumberLiteral node, Object data) {
        return data;
    }
    
}
