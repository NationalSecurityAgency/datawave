package datawave.query.jexl.nodes;

import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.JexlNodes;

/**
 * This is a node that can wrap an expression (normally an EQ or RE node) to denote that it references a known hole in the global index.
 */
public class IndexHoleMarkerJexlNode extends QueryPropertyMarker {
    
    private static final String LABEL = "_Hole_";
    
    public static String label() {
        return LABEL;
    }
    
    public IndexHoleMarkerJexlNode(int id) {
        super(id);
    }
    
    public IndexHoleMarkerJexlNode() {
        super();
    }
    
    /**
     * This will create a structure as follows around the specified node: Reference (this node) Reference Expression AND Reference Reference Expression
     * Assignment Reference Identifier:_Hole_ True node (the one specified
     *
     * Hence the resulting expression will be ((_Hole_ = True) AND {specified node})
     *
     * @param node
     */
    public IndexHoleMarkerJexlNode(JexlNode node) {
        super(node);
    }
    
    @Override
    public String getLabel() {
        return LABEL;
    }
    
    /**
     * @param node
     * @return
     */
    public static IndexHoleMarkerJexlNode create(JexlNode node) {
        
        JexlNode parent = node.jjtGetParent();
        
        IndexHoleMarkerJexlNode expr = new IndexHoleMarkerJexlNode(node);
        
        if (parent != null) {
            JexlNodes.replaceChild(parent, node, expr);
        }
        
        return expr;
    }
    
    /**
     * A routine to determine whether an and node is actually an index hold marker. The reason for this routine is that if the query is serialized and
     * deserialized, then only the underlying assignment will persist.
     * 
     * @param node
     * @return true if this and node is an index hold marker
     */
    public static boolean instanceOf(JexlNode node) {
        return instanceOf(node, IndexHoleMarkerJexlNode.class);
    }
    
    /**
     * A routine to determine get the node which is the source of the index hold (i.e. the underlying eq, regex or range)
     * 
     * @param node
     * @return the source node or null if not an an index hole Marker
     */
    public static JexlNode getIndexHoleSource(JexlNode node) {
        return getQueryPropertySource(node, IndexHoleMarkerJexlNode.class);
    }
    
}
