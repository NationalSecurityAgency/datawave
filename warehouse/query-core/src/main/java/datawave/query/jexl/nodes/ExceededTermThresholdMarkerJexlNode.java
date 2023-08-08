package datawave.query.jexl.nodes;

import org.apache.commons.jexl2.parser.JexlNode;

/**
 * This is a node that can be put in place of an underlying regex or range to denote that the term threshold was exceeded preventing expansion of the underlying
 * range into a conjunction of terms.
 */
public class ExceededTermThresholdMarkerJexlNode extends QueryPropertyMarker {

    private static final String LABEL = "_Term_";

    /**
     * Return the label this marker type: {@value #LABEL}. Overrides {@link QueryPropertyMarker#label()}.
     *
     * @return the label
     */
    public static String label() {
        return LABEL;
    }

    public ExceededTermThresholdMarkerJexlNode() {
        super();
    }

    public ExceededTermThresholdMarkerJexlNode(int id) {
        super(id);
    }

    /**
     * Returns a new query property marker with the expression <code>(({@value #LABEL} = true) &amp;&amp; ({source}))</code>.
     *
     * @param node
     *            the source node
     * @see QueryPropertyMarker#QueryPropertyMarker(JexlNode) the super constructor for additional information on the tree structure
     */
    public ExceededTermThresholdMarkerJexlNode(JexlNode node) {
        super(node);
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
}
