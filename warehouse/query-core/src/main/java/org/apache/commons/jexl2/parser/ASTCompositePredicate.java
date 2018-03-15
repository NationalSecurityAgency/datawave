package org.apache.commons.jexl2.parser;

import datawave.query.jexl.nodes.QueryPropertyMarker;

/**
 * Represents a composite predicate. If this reference expression exists, we should not perform any processing that may affect the indexed query.
 */
public class ASTCompositePredicate extends QueryPropertyMarker {
    
    private static final String CLASS_NAME = ASTCompositePredicate.class.getSimpleName();
    
    public ASTCompositePredicate(int id) {
        super(id);
    }
    
    public ASTCompositePredicate(Parser p, int id) {
        super(p, id);
    }
    
    public ASTCompositePredicate(JexlNode source) {
        super(source);
    }
    
    @Override
    public String toString() {
        return CLASS_NAME;
    }
    
    /**
     * @param node
     * @return
     */
    public static ASTCompositePredicate create(JexlNode node) {
        
        JexlNode parent = node.jjtGetParent();
        
        ASTCompositePredicate expr = new ASTCompositePredicate(node);
        
        if (parent != null) {
            JexlNodes.replaceChild(parent, node, expr);
        }
        
        return expr;
    }
    
    /**
     * A routine to determine whether an and node is actually a composite predicate marker. The reason for this routine is that if the query is serialized and
     * deserialized, then only the underlying assignment will persist.
     *
     * @param node
     * @return true if this and node is a composite predicate marker
     */
    public static boolean instanceOf(JexlNode node) {
        return QueryPropertyMarker.instanceOf(node, ASTCompositePredicate.class);
    }
    
    /**
     * A routine to determine get the node which is the source of the composite predicate
     *
     * @param node
     * @return the source node or null if not an a composite predicate marker
     */
    public static JexlNode getCompositePredicateSource(JexlNode node) {
        return QueryPropertyMarker.getQueryPropertySource(node, ASTCompositePredicate.class);
    }
}
