package org.apache.commons.jexl2.parser;

import java.util.function.Function;

import com.google.common.collect.Lists;

import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;

/**
 * Represents a node which captures an expression which will be dropped/ignored. The ignored expression will be assigned to the _Query_ variable.
 */
public class DroppedExpression extends QueryPropertyMarker {

    private static final String CLASS_NAME = DroppedExpression.class.getSimpleName();

    private static final String LABEL = "_Drop_";

    private static final String REASON_LABEL = "_Reason_";

    private static final String QUERY_LABEL = "_Query_";

    /**
     * Return the label for this marker type: {@value #LABEL}. Overrides {@link QueryPropertyMarker#label()}.
     *
     * @return the label
     */
    public static String label() {
        return LABEL;
    }

    /**
     * Create and return a new {@link DroppedExpression}. In this case the source node is turned into a string which is assigned to a variable such that this
     * expression avoids its evaluation.
     *
     * @param node
     *            the source node
     * @return the new marker node
     * @see QueryPropertyMarker#create(JexlNode, Function)
     */
    public static DroppedExpression create(JexlNode node, final String reason) {
        return create(node, s -> new DroppedExpression(s, reason));
    }

    public DroppedExpression() {
        super();
    }

    public DroppedExpression(int id) {
        super(id);
    }

    /**
     * Create and return a new {@link DroppedExpression}. In this case the source node is turned into a string which is assigned to a variable such that this
     * expression avoids its evaluation.
     *
     * @param node
     *            the source node
     * @return the new marker node
     * @see QueryPropertyMarker#create(JexlNode, Function)
     */
    public static DroppedExpression create(JexlNode node) {
        return create(node, DroppedExpression::new);
    }

    /**
     * Returns a new query property marker with the expression <code>(({@value #LABEL} = true) &amp;&amp; ( {@value #QUERY_LABEL} = '{source}' ))</code>. In
     * this case the source node is turned into a string which is assigned to a variable such that this expression avoids its evaluation.
     *
     * @param source
     *            the source node
     * @param reason
     *            The reason for the dropped expression
     * @see QueryPropertyMarker#QueryPropertyMarker(JexlNode) the super constructor for additional information on the tree structure
     */
    public DroppedExpression(JexlNode source, String reason) {
        super(source);
        addReason(reason);
    }

    /**
     * Returns a new query property marker with the expression <code>(({@value #LABEL} = true) &amp;&amp; ( {@value #QUERY_LABEL} = '{source}' ))</code>. In
     * this case the source node is turned into a string which is assigned to a variable such that this expression avoids its evaluation.
     *
     * @param source
     *            the source node
     * @see QueryPropertyMarker#QueryPropertyMarker(JexlNode) the super constructor for additional information on the tree structure
     */
    public DroppedExpression(JexlNode source) {
        super(source);
    }

    @Override
    protected void setupSource(JexlNode source) {
        // create the assignment using the query label wrapped in an expression
        String expression = JexlStringBuildingVisitor.buildQueryWithoutParse(source);
        JexlNode newSource = JexlNodeFactory.createExpression(JexlNodeFactory.createAssignment(QUERY_LABEL, expression));

        // inserts new assignment node in place of the original source
        JexlNode parent = source.jjtGetParent();
        if (parent != null) {
            newSource.jjtSetParent(parent);
            JexlNodes.replaceChild(parent, source, newSource);
        }

        // now wrap this new source with the marker
        super.setupSource(newSource);
    }

    protected void addReason(String reason) {
        Instance instance = findInstance(this);
        JexlNode reasonAssignment = JexlNodeFactory.createExpression(JexlNodeFactory.createAssignment(REASON_LABEL, reason));
        JexlNode newSource = JexlNodeFactory.createAndNode(Lists.newArrayList(reasonAssignment, instance.getSource()));
        super.setupSource(newSource);
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
