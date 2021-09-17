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
}
