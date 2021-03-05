package datawave.query.jexl.nodes;

import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.JexlNodes;

/**
 * This is a node that can be put in place of an ASTERNode to denote that the value threshold was exceeded preventing expansion into a conjunction of terms
 */
public class ExceededValueThresholdMarkerJexlNode extends QueryPropertyMarker {
    
    private static final String LABEL = "_Value_";
    
    public static String label() {
        return LABEL;
    }
    
    public ExceededValueThresholdMarkerJexlNode(int id) {
        super(id);
    }
    
    public ExceededValueThresholdMarkerJexlNode() {
        super();
    }
    
    /**
     * This will create a structure as follows around the specified node: Reference (this node) Reference Expression AND Reference Reference Expression
     * Assignment Reference Identifier:ExceededValueThresholdMarkerJexlNode True node (the one specified
     * 
     * Hence the resulting expression will be ((ExceededValueThresholdMarkerJexlNode = True) AND {specified node})
     * 
     * @param node
     */
    public ExceededValueThresholdMarkerJexlNode(JexlNode node) {
        super(node);
    }
    
    @Override
    public String getLabel() {
        return LABEL;
    }
    
    /**
     * A routine to determine whether an and node is actually an exceeded value threshold marker. The reason for this routine is that if the query is serialized
     * and deserialized, then only the underlying assignment will persist.
     * 
     * @param node
     * @return true if this and node is an exceeded value marker
     */
    public static boolean instanceOf(JexlNode node) {
        return QueryPropertyMarker.instanceOf(node, ExceededValueThresholdMarkerJexlNode.class);
    }
    
    /**
     * A routine to determine get the node which is the source of the exceeded value threshold (i.e. the underlying regex or range)
     * 
     * @param node
     * @return the source node or null if not an an exceededValueThreshold Marker
     */
    public static JexlNode getExceededValueThresholdSource(JexlNode node) {
        return QueryPropertyMarker.getQueryPropertySource(node, ExceededValueThresholdMarkerJexlNode.class);
    }
    
    public static ExceededValueThresholdMarkerJexlNode create(JexlNode node) {
        
        JexlNode parent = node.jjtGetParent();
        
        ExceededValueThresholdMarkerJexlNode expr = new ExceededValueThresholdMarkerJexlNode(node);
        
        if (parent != null) {
            JexlNodes.replaceChild(parent, node, expr);
        }
        
        return expr;
    }
}
