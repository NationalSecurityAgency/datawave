package org.apache.commons.jexl2.parser;

import java.util.function.Function;

import datawave.query.jexl.nodes.QueryPropertyMarker;

/**
 * Represents a node which can be treated as lenient. Lenient means for example that the expression will be tolerant of normalizations that cannot be used
 * relative to the field types.
 */
public class LenientExpression extends QueryPropertyMarker {

    private static final String CLASS_NAME = LenientExpression.class.getSimpleName();

    private static final String LABEL = "_Lenient_";

    /**
     * Return the label this marker type: {@value #LABEL}. Overrides {@link QueryPropertyMarker#label()}.
     *
     * @return the label
     */
    public static String label() {
        return LABEL;
    }

    /**
     * Create and return a new {@link LenientExpression}. In this case the source node is turned into a string which is assigned to a variable such that this
     * expression avoids its evaluation.
     *
     * @param node
     *            the source node
     * @return the new marker node
     * @see QueryPropertyMarker#create(JexlNode, Function)
     */
    public static LenientExpression create(JexlNode node) {
        return create(node, LenientExpression::new);
    }

    public LenientExpression() {
        super();
    }

    public LenientExpression(int id) {
        super(id);
    }

    /**
     * Returns a new query property marker with the expression <code>(({@value #LABEL} = true) &amp;&amp; ({source}))</code>.
     *
     * @param source
     *            the source node
     * @see QueryPropertyMarker#QueryPropertyMarker(JexlNode) the super constructor for additional information on the tree structure
     */
    public LenientExpression(JexlNode source) {
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
