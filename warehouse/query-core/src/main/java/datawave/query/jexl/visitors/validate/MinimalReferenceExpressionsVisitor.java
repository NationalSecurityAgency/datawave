package datawave.query.jexl.visitors.validate;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.jexl.visitors.BaseVisitor;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.jexl.visitors.QueryPropertyMarkerVisitor;
import org.apache.commons.jexl2.parser.ASTAdditiveNode;
import org.apache.commons.jexl2.parser.ASTAdditiveOperator;
import org.apache.commons.jexl2.parser.ASTAmbiguous;
import org.apache.commons.jexl2.parser.ASTAndNode;
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
import org.apache.commons.jexl2.parser.ASTNotNode;
import org.apache.commons.jexl2.parser.ASTNullLiteral;
import org.apache.commons.jexl2.parser.ASTNumberLiteral;
import org.apache.commons.jexl2.parser.ASTOrNode;
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
import org.apache.commons.jexl2.parser.SimpleNode;
import org.apache.log4j.Logger;

/**
 * Detect if a query tree contains nodes surrounded by multiple reference expressions, or a child node with an extra wrap.
 *
 * Assumes that each ASTReferenceExpression has a parent ASTReference node
 */
public class MinimalReferenceExpressionsVisitor extends BaseVisitor {
    
    private static final Logger log = Logger.getLogger(MinimalReferenceExpressionsVisitor.class);
    
    private boolean isValid = true;
    
    private enum reason {
        // the reason a query tree failed validation
        DOUBLE_PAREN,
        WRAPPED_SINGLE_CHILD
    }
    
    public static boolean validate(JexlNode node) {
        MinimalReferenceExpressionsVisitor visitor = new MinimalReferenceExpressionsVisitor();
        node.jjtAccept(visitor, null);
        return visitor.isValid();
    }
    
    private MinimalReferenceExpressionsVisitor() {}
    
    public boolean isValid() {
        return this.isValid;
    }
    
    /*
     * Operations
     */
    
    @Override
    public Object visit(ASTReference node, Object data) {
        if (!isValid) {
            // short circuit if the tree is already marked invalid
            return data;
        } else if (isParen(node) && isParen(getLeftMostGrandchild(node))) {
            // found a double paren
            invalidate(node, reason.DOUBLE_PAREN);
            return data;
        } else {
            node.childrenAccept(this, data);
            return data;
        }
    }
    
    @Override
    public Object visit(ASTOrNode node, Object data) {
        if (!isValid) {
            // short circuit if the tree is already marked invalid
            return data;
        } else if (isAnyChildAWrappedSingleTerm(node)) {
            // found a single term child that is wrapped
            invalidate(node, reason.WRAPPED_SINGLE_CHILD);
            return data;
        } else {
            node.childrenAccept(this, data);
            return data;
        }
    }
    
    @Override
    public Object visit(ASTAndNode node, Object data) {
        if (!isValid)
            return data;
        
        QueryPropertyMarker.Instance instance = QueryPropertyMarkerVisitor.getInstance(node);
        
        if (instance.isAnyType()) {
            // descend into the marker's source node
            JexlNode source = instance.getSource();
            source.childrenAccept(this, data);
            return data;
        } else if (isAnyChildAWrappedSingleTerm(node)) {
            // invalidate
            invalidate(node, reason.WRAPPED_SINGLE_CHILD);
            return data;
        } else {
            // otherwise continue descent
            node.childrenAccept(this, data);
            return data;
        }
    }
    
    /*
     * Helper methods
     */
    
    // is this node a parentheses
    private boolean isParen(JexlNode node) {
        return node instanceof ASTReference && node.jjtGetNumChildren() == 1 && node.jjtGetChild(0) instanceof ASTReferenceExpression;
    }
    
    // fetch the grandchild
    private JexlNode getLeftMostGrandchild(JexlNode node) {
        return node.jjtGetChild(0).jjtGetChild(0);
    }
    
    private boolean isAnyChildAWrappedSingleTerm(JexlNode node) {
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            JexlNode child = node.jjtGetChild(i);
            if (isWrappedSingleTerm(child)) {
                return true;
            }
        }
        return false;
    }
    
    private boolean isWrappedSingleTerm(JexlNode node) {
        if (isParen(node)) {
            boolean isMarkerNode = QueryPropertyMarkerVisitor.getInstance(node).isAnyType();
            if (!isMarkerNode) {
                JexlNode unwrapped = JexlASTHelper.dereference(node);
                
                return !(unwrapped instanceof ASTAndNode || unwrapped instanceof ASTOrNode || unwrapped instanceof ASTAssignment);
            }
        }
        return false;
    }
    
    /**
     * Mark the tree as invalid, print the cause and the node that caused this
     *
     * @param node
     *            the node that caused this tree to be marked invalid
     * @param cause
     *            the reason why
     */
    private void invalidate(JexlNode node, reason cause) {
        isValid = false;
        log.info("Invalid node (" + cause + "): " + JexlStringBuildingVisitor.buildQueryWithoutParse(node));
    }
    
    /*
     * Pass through ASTJexlScript.
     * 
     * Potential to short circuit on SimpleNode, ASTReferenceExpression and ASTNotNode methods
     */
    
    @Override
    public Object visit(SimpleNode node, Object data) {
        if (isValid) {
            node.childrenAccept(this, data);
        }
        return data;
    }
    
    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        if (isValid) {
            node.childrenAccept(this, data);
        }
        return data;
    }
    
    // descend into negated branches of the query
    @Override
    public Object visit(ASTNotNode node, Object data) {
        if (isValid) {
            node.childrenAccept(this, data);
        }
        return data;
    }
    
    /*
     * Short circuits, do not descend further into leaf nodes
     */
    
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
