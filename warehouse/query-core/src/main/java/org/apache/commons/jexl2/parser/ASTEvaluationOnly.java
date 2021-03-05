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
    
    /**
     * A routine to determine whether an and node is actually an evaluation only marker. The reason for this routine is that if the query is serialized and
     * deserialized, then only the underlying assignment will persist.
     *
     * @param node
     * @return true if this and node is a composite predicate marker
     */
    public static boolean instanceOf(JexlNode node) {
        return QueryPropertyMarker.instanceOf(node, ASTEvaluationOnly.class);
    }
    
    /**
     * A routine to get the node which is the source of the evaluation only marker
     *
     * @param node
     * @return the source node or null if not an a composite predicate marker
     */
    public static JexlNode getEvaluationOnlySource(JexlNode node) {
        return QueryPropertyMarker.getQueryPropertySource(node, ASTEvaluationOnly.class);
    }
}
