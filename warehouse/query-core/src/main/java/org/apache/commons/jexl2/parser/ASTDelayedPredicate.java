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
     * @param node
     * @return
     */
    public static ASTDelayedPredicate create(JexlNode node) {
        
        JexlNode parent = node.jjtGetParent();
        
        ASTDelayedPredicate expr = new ASTDelayedPredicate(node);
        
        if (parent != null) {
            JexlNodes.replaceChild(parent, node, expr);
        }
        
        return expr;
    }
}
