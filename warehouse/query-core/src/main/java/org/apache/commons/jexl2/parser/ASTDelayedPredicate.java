package org.apache.commons.jexl2.parser;

import datawave.query.jexl.nodes.QueryPropertyMarker;

/**
 * Represents a delayed predicate. If this reference expression exists, we should not perform any processing that may affect the indexed query.
 */
public class ASTDelayedPredicate extends QueryPropertyMarker {
    
    private static final String LABEL = "_Delayed_";
    
    private static final String CLASS_NAME = ASTDelayedPredicate.class.getSimpleName();
    
    public static String label() {
        return LABEL;
    }
    
    public ASTDelayedPredicate(int id) {
        super(id);
    }
    
    public ASTDelayedPredicate(Parser p, int id) {
        super(p, id);
    }
    
    public ASTDelayedPredicate(JexlNode source) {
        super(source);
    }
    
    @Override
    public String getLabel() {
        return LABEL;
    }
    
    /** Accept the visitor. **/
    @Override
    public Object jjtAccept(ParserVisitor visitor, Object data) {
        return visitor.visit(this, data);
    }
    
    @Override
    public String toString() {
        return CLASS_NAME;
    }
    
    /**
     * Wrap a node in an ASTDelayedPredicate, if not already delayed
     *
     * @param node
     *            a JexlNode
     * @return an ASTDelayedPredicate, or the original node if this node is already delayed
     */
    public static JexlNode create(JexlNode node) {
        
        // Do not delay if this subtree is already delayed
        if (instanceOf(node, ASTDelayedPredicate.class) || ASTDelayedPredicate.isSubTreeAlreadyDelayed(node)) {
            return node;
        }
        
        JexlNode parent = node.jjtGetParent();
        
        ASTDelayedPredicate expr = new ASTDelayedPredicate(node);
        
        if (parent != null) {
            JexlNodes.replaceChild(parent, node, expr);
        }
        
        return expr;
    }
    
    /**
     * Recursively ascend the tree looking for an instance of an ASTDelayedPredicate intersected with a parent
     * 
     * @param node
     *            the node to check
     * @return true if this node is delayed, or if a parent node is delayed
     */
    public static boolean isSubTreeAlreadyDelayed(JexlNode node) {
        if (node instanceof ASTAndNode && node.jjtGetNumChildren() == 2) {
            if (instanceOf(node, ASTDelayedPredicate.class)) {
                return true;
            }
        }
        
        if (node.jjtGetParent() == null) {
            return false;
        } else {
            return isSubTreeAlreadyDelayed(node.jjtGetParent());
        }
    }
    
    /**
     * A routine to determine whether an and node is actually a delayed predicate marker. The reason for this routine is that if the query is serialized and
     * deserialized, then only the underlying assignment will persist.
     * 
     * @param node
     * @return true if this and node is a delayed predicate marker
     */
    public static boolean instanceOf(JexlNode node) {
        return QueryPropertyMarker.instanceOf(node, ASTDelayedPredicate.class);
    }
    
    /**
     * A routine to determine get the node which is the source of the delayed predicate
     * 
     * @param node
     * @return the source node or null if not an a delayed predicate marker
     */
    public static JexlNode getDelayedPredicateSource(JexlNode node) {
        return QueryPropertyMarker.getQueryPropertySource(node, ASTDelayedPredicate.class);
    }
    
}
