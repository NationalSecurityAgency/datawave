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
     * Assignment Reference Identifier:_Value_ True node (the one specified
     * 
     * Hence the resulting expression will be ((_Value_ = True) AND {specified node})
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
    
    public static ExceededValueThresholdMarkerJexlNode create(JexlNode node) {
        
        JexlNode parent = node.jjtGetParent();
        
        ExceededValueThresholdMarkerJexlNode expr = new ExceededValueThresholdMarkerJexlNode(node);
        
        if (parent != null) {
            JexlNodes.replaceChild(parent, node, expr);
        }
        
        return expr;
    }
}
