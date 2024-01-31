package datawave.query.jexl.nodes;

import java.util.function.Function;

import org.apache.commons.jexl2.parser.JexlNode;

/**
 * This is a node that can wrap an expression (normally an EQ or RE node) to denote that it references a known hole in the global index.
 */
public class IndexHoleMarkerJexlNode extends QueryPropertyMarker {

    private static final String LABEL = "_Hole_";

    /**
     * Return the label this marker type: {@value #LABEL}. Overrides {@link QueryPropertyMarker#label()}.
     *
     * @return the label
     */
    public static String label() {
        return LABEL;
    }

    /**
     * Create and return a new {@link IndexHoleMarkerJexlNode} with the given source.
     *
     * @param node
     *            the source node
     * @return the new marker node
     * @see QueryPropertyMarker#create(JexlNode, Function)
     */
    public static IndexHoleMarkerJexlNode create(JexlNode node) {
        return create(node, IndexHoleMarkerJexlNode::new);
    }

    public IndexHoleMarkerJexlNode() {
        super();
    }

    public IndexHoleMarkerJexlNode(int id) {
        super(id);
    }

    /**
     * Returns a new query property marker with the expression <code>(({@value #LABEL} = true) &amp;&amp; ({source}))</code>.
     *
     * @param node
     *            the source node
     * @see QueryPropertyMarker#QueryPropertyMarker(JexlNode) the super constructor for additional information on the tree structure
     */
    public IndexHoleMarkerJexlNode(JexlNode node) {
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
