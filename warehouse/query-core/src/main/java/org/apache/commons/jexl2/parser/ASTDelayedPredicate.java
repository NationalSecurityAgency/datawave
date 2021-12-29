package org.apache.commons.jexl2.parser;

import datawave.query.jexl.nodes.QueryPropertyMarker;

import java.util.function.Function;

/**
 * Represents a delayed predicate. If this reference expression exists, we should not perform any processing that may affect the indexed query.
 */
public class ASTDelayedPredicate extends QueryPropertyMarker {
    
    private static final String LABEL = "_Delayed_";
    
    private static final String CLASS_NAME = ASTDelayedPredicate.class.getSimpleName();
    
    /**
     * Return the label this marker type: {@value #LABEL}. Overrides {@link QueryPropertyMarker#label()}.
     * 
     * @return the label
     */
    public static String label() {
        return LABEL;
    }
    
    /**
     * Create and return a new {@link ASTDelayedPredicate} with the given source. If the source is already a delayed predicate, or is part of an already-delayed
     * subtree, the original node is returned unchanged.
     * 
     * @param node
     *            the source node
     * @return the new marker node
     * @see QueryPropertyMarker#create(JexlNode, Function)
     */
    public static JexlNode create(JexlNode node) {
        // Do not delay if this subtree is already delayed
        if (QueryPropertyMarker.findInstance(node).isType(ASTDelayedPredicate.class) || ASTDelayedPredicate.isSubTreeAlreadyDelayed(node)) {
            return node;
        }
        
        return create(node, ASTDelayedPredicate::new);
    }
    
    public ASTDelayedPredicate() {
        super();
    }
    
    public ASTDelayedPredicate(int id) {
        super(id);
    }
    
    /**
     * Returns a new query property marker with the expression <code>(({@value #LABEL} = true) &amp;&amp; ({source}))</code>.
     * 
     * @param source
     *            the source node
     * @see QueryPropertyMarker#QueryPropertyMarker(JexlNode) the super constructor for additional information on the tree structure
     */
    public ASTDelayedPredicate(JexlNode source) {
        super(source);
    }
    
    /**
     * Returns {@value #LABEL}.
     * 
     * @return the label
     */
    @Override
    public String getLabel() {
        return LABEL;
    }
    
    @Override
    public String toString() {
        return CLASS_NAME;
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
            if (QueryPropertyMarker.findInstance(node).isType(ASTDelayedPredicate.class)) {
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
     * Unwrap a delayed predicate, fully. Intended to handle the odd edge case when multiple delayed predicate markers are applied to the same node
     *
     * @param node
     *            an arbitrary jexl node
     * @return the source node, or the original node if this node is not delayed
     */
    public static JexlNode unwrapFully(JexlNode node) {
        QueryPropertyMarker.Instance instance = QueryPropertyMarker.findInstance(node);
        if (instance.isType(ASTDelayedPredicate.class)) {
            
            JexlNode source = instance.getSource();
            instance = QueryPropertyMarker.findInstance(source);
            
            while (instance.isType(ASTDelayedPredicate.class)) {
                source = instance.getSource();
                instance = QueryPropertyMarker.findInstance(source);
            }
            
            return source;
        }
        return node;
    }
}
