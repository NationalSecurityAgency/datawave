package org.apache.commons.jexl2.parser;

import datawave.query.jexl.nodes.QueryPropertyMarker;

import java.util.function.Function;

/**
 * Represents a node which should only be used for jexl evaluation. If this reference expression exists, we should not perform any processing that may affect
 * the indexed query.
 */
public class ASTEvaluationOnly extends QueryPropertyMarker {

    private static final String CLASS_NAME = ASTEvaluationOnly.class.getSimpleName();

    private static final String LABEL = "_Eval_";

    /**
     * Return the label this marker type: {@value #LABEL}. Overrides {@link QueryPropertyMarker#label()}.
     *
     * @return the label
     */
    public static String label() {
        return LABEL;
    }

    /**
     * Create and return a new {@link ASTEvaluationOnly} with the given source.
     *
     * @param node
     *            the source node
     * @return the new marker node
     * @see QueryPropertyMarker#create(JexlNode, Function)
     */
    public static ASTEvaluationOnly create(JexlNode node) {
        return create(node, ASTEvaluationOnly::new);
    }

    public ASTEvaluationOnly() {
        super();
    }

    public ASTEvaluationOnly(int id) {
        super(id);
    }

    /**
     * Returns a new query property marker with the expression <code>(({@value #LABEL} = true) &amp;&amp; ({source}))</code>.
     *
     * @param source
     *            the source node
     * @see QueryPropertyMarker#QueryPropertyMarker(JexlNode) the super constructor for additional information on the tree structure
     */
    public ASTEvaluationOnly(JexlNode source) {
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
