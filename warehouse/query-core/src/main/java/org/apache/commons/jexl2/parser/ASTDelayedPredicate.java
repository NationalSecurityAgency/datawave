package org.apache.commons.jexl2.parser;

import datawave.query.jexl.nodes.QueryPropertyMarker;

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
     * Create and return a new {@link ASTDelayedPredicate} with the given source. This method will insert the marked node into the node tree in place of the
     * provided node
     * 
     * @param node
     *            the source node
     * @return the new marker node
     * @see QueryPropertyMarker#create(JexlNode, Class)
     */
    public static JexlNode create(JexlNode node) {
        return create(node, ASTDelayedPredicate.class);
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
}
