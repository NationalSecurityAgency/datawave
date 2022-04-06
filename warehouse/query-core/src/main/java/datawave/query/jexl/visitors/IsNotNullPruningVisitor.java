package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
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
import org.apache.commons.jexl2.parser.ASTJexlScript;
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
import org.apache.commons.jexl2.parser.JexlNodes;
import org.apache.commons.jexl2.parser.SimpleNode;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Prune #IS_NOT_NULL functions that share a field with a positive inclusive node
 * <p>
 * Assumes the tree is flattened
 */
public class IsNotNullPruningVisitor extends BaseVisitor {
    
    private IsNotNullPruningVisitor() {}
    
    /**
     * Generic entrypoint, applies pruning logic to subtree
     *
     * @param node
     *            an arbitrary Jexl node
     * @return the node
     */
    public static JexlNode prune(JexlNode node) {
        IsNotNullPruningVisitor visitor = new IsNotNullPruningVisitor();
        node.jjtAccept(visitor, null);
        return node;
    }
    
    /**
     * Prune 'is not null' terms that share a common field with other terms in this intersection
     *
     * @param node
     *            an intersection
     * @param data
     *            null object
     * @return the same ASTAnd node
     */
    @Override
    public Object visit(ASTAndNode node, Object data) {
        
        // make a single pass over the children
        List<JexlNode> isNotNulls = new LinkedList<>();
        Set<String> equalityFields = new HashSet<>();
        JexlNode deref;
        for (JexlNode child : JexlNodes.children(node)) {
            deref = JexlASTHelper.dereference(child);
            if (isChildNotNullFunction(deref)) {
                isNotNulls.add(child);
            } else if (deref instanceof ASTEQNode || deref instanceof ASTERNode) {
                String field = fieldForChild(deref);
                if (field != null)
                    equalityFields.add(field);
            }
        }
        
        // only rebuild if it's possible
        if (!isNotNulls.isEmpty() && !equalityFields.isEmpty()) {
            List<JexlNode> next = new ArrayList<>();
            for (JexlNode child : JexlNodes.children(node)) {
                if (isNotNulls.contains(child)) {
                    String field = fieldForChild(child);
                    if (field != null && equalityFields.contains(field)) {
                        continue; // skip the is not null term if it shares a common field
                    }
                }
                next.add(child);
            }
            
            // rebuild
            if (next.size() == 1) {
                JexlNodes.replaceChild(node.jjtGetParent(), node, next.iterator().next());
                return data; // no sense visiting a single node we just built, so return here
            } else {
                JexlNodes.children(node, next.toArray(new JexlNode[0]));
            }
        }
        
        node.childrenAccept(this, data);
        return data;
    }
    
    /**
     * Determines if a node is <code>!(FOO == null)</code>
     * 
     * @param node
     * @return
     */
    protected boolean isChildNotNullFunction(JexlNode node) {
        if (node instanceof ASTNotNode) {
            JexlNode child = node.jjtGetChild(0);
            child = JexlASTHelper.dereference(child);
            if (child instanceof ASTEQNode && child.jjtGetNumChildren() == 2) {
                child = JexlASTHelper.dereference(child.jjtGetChild(1));
                return child instanceof ASTNullLiteral;
            }
        }
        return false;
    }
    
    /**
     * Extract the field for the provided Jexl node
     *
     * @param node
     *            an arbitrary Jexl node
     * @return the field, or null if no such field exists
     */
    protected String fieldForChild(JexlNode node) {
        if (!(node instanceof ASTAndNode || node instanceof ASTOrNode)) {
            Set<String> names = JexlASTHelper.getIdentifierNames(node);
            if (names.size() == 1) {
                return names.iterator().next();
            }
        }
        return null;
    }
    
    // +-----------------------------+
    // | Descend through these nodes |
    // +-----------------------------+
    
    @Override
    public Object visit(ASTJexlScript node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTOrNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTReference node, Object data) {
        // do not descend into marker nodes
        if (!QueryPropertyMarkerVisitor.getInstance(node).isAnyType()) {
            node.childrenAccept(this, data);
        }
        return data;
    }
    
    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        node.childrenAccept(this, data);
        //  @formatter:off
        if (node.jjtGetNumChildren() == 1 &&
                !JexlNodes.isNodeNegated(node) &&
                (node.jjtGetChild(0) instanceof ASTEQNode || node.jjtGetChild(0) instanceof ASTERNode)) {
            //  @formatter:on
            JexlNodes.replaceChild(node.jjtGetParent(), node, node.jjtGetChild(0));
        }
        return data;
    }
    
    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTNotNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    // +-----------------------------------+
    // | Do not descend through leaf nodes |
    // +-----------------------------------+
    
    @Override
    public Object visit(SimpleNode node, Object data) {
        return data;
    }
    
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
        node.childrenAccept(this, data);
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
    
    @Override
    public Object visit(ASTIntegerLiteral node, Object data) {
        return data;
    }
    
    @Override
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
