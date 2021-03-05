package datawave.query.jexl.nodes;

import org.apache.commons.jexl2.parser.JexlNode;

/**
 * This is a node that can be put in place of an underlying regex or range to denote that the term threshold was exceeded preventing expansion of the underlying
 * range into a conjunction of terms
 */
public class ExceededTermThresholdMarkerJexlNode extends QueryPropertyMarker {
    
    private static final String LABEL = "_Term_";
    
    public static String label() {
        return LABEL;
    }
    
    public ExceededTermThresholdMarkerJexlNode(int id) {
        super(id);
    }
    
    public ExceededTermThresholdMarkerJexlNode() {
        super();
    }
    
    /**
     * This will create a structure as follows around the specified node: Reference (this node) Reference Expression AND Reference Reference Expression
     * Assignment Reference Identifier:_Term_ True node (the one specified
     * 
     * Hence the resulting expression will be ((_Term_ = True) AND {specified node})
     * 
     * @param node
     */
    public ExceededTermThresholdMarkerJexlNode(JexlNode node) {
        super(node);
    }
    
    @Override
    public String getLabel() {
        return LABEL;
    }
    
    /**
     * A routine to determine whether an and node is actually an exceeded term threshold marker. The reason for this routine is that if the query is serialized
     * and deserialized, then only the underlying assignment will persist.
     * 
     * @param node
     * @return true if this and node is an exceeded term marker
     */
    public static boolean instanceOf(JexlNode node) {
        return QueryPropertyMarker.instanceOf(node, ExceededTermThresholdMarkerJexlNode.class);
    }
    
    /**
     * A routine to determine get the node which is the source of the exceeded value threshold (i.e. the underlying anyfield expression)
     * 
     * @param node
     * @return the source node or null if not an an exceededValueThreshold Marker
     */
    public static JexlNode getExceededTermThresholdSource(JexlNode node) {
        return QueryPropertyMarker.getQueryPropertySource(node, ExceededTermThresholdMarkerJexlNode.class);
    }
    
}
