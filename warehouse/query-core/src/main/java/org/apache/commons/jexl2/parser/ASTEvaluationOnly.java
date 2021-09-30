package org.apache.commons.jexl2.parser;

import datawave.query.jexl.nodes.QueryPropertyMarker;

/**
 * Represents a node which should only be used for jexl evaluation. If this reference expression exists, we should not perform any processing that may affect
 * the indexed query.
 */
public class ASTEvaluationOnly extends QueryPropertyMarker {
    
    private static final String CLASS_NAME = ASTEvaluationOnly.class.getSimpleName();
    
    private static final String LABEL = "_Eval_";
    
    public static String label() {
        return LABEL;
    }
    
    public ASTEvaluationOnly(int id) {
        super(id);
    }
    
    public ASTEvaluationOnly(Parser p, int id) {
        super(p, id);
    }
    
    public ASTEvaluationOnly(JexlNode source) {
        super(source);
    }
    
    @Override
    public String getLabel() {
        return LABEL;
    }
    
    @Override
    public String toString() {
        return CLASS_NAME;
    }
    
    /**
     * @param node
     * @return
     */
    public static ASTEvaluationOnly create(JexlNode node) {
        
        JexlNode parent = node.jjtGetParent();
        
        ASTEvaluationOnly expr = new ASTEvaluationOnly(node);
        
        if (parent != null) {
            JexlNodes.replaceChild(parent, node, expr);
        }
        
        return expr;
    }
}
