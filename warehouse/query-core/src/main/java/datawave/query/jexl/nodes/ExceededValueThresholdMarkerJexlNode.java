package datawave.query.jexl.nodes;

import org.apache.commons.jexl2.parser.JexlNode;

/**
 * This is a node that can be put in place of an ASTERNode to denote that the value threshold was exceeded preventing expansion into a conjunction of terms
 */
public class ExceededValueThresholdMarkerJexlNode extends QueryPropertyMarker {
    
    private static final String LABEL = "_Value_";
    
    /**
     * Return the label this marker type: {@value #LABEL}. Overrides {@link QueryPropertyMarker#label()}.
     * 
     * @return the label
     */
    public static String label() {
        return LABEL;
    }
    
    /**
     * Create and return a new {@link ExceededValueThresholdMarkerJexlNode} with the given source.
     * 
     * @param node
     *            the source node
     * @return the new marker node
     * @see QueryPropertyMarker#create(JexlNode, Class)
     */
    public static JexlNode create(JexlNode node) {
        return create(node, ExceededValueThresholdMarkerJexlNode.class);
    }
    
    public ExceededValueThresholdMarkerJexlNode() {
        super();
    }
    
    public ExceededValueThresholdMarkerJexlNode(int id) {
        super(id);
    }
    
    /**
     * Returns a new query property marker with the expression <code>(({@value #LABEL} = true) &amp;&amp; ({source}))</code>.
     * 
     * @param node
     *            the source node
     * @see QueryPropertyMarker#QueryPropertyMarker(JexlNode) the super constructor for additional information on the tree structure
     */
    public ExceededValueThresholdMarkerJexlNode(JexlNode node) {
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
