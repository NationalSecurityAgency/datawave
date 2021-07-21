package datawave.query.jexl.nodes;

import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.visitors.QueryPropertyMarkerVisitor;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.Parser;
import org.apache.commons.jexl2.parser.ParserTreeConstants;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This is a node that can be put in place of an underlying reference node to place a property on an underlying query sub-tree (e.g. ExceededValueThreshold)
 */
public abstract class QueryPropertyMarker extends ASTReference {
    
    public static String label() {
        throw new IllegalStateException("Label hasn't been configured in subclass.");
    }
    
    public QueryPropertyMarker() {
        this(ParserTreeConstants.JJTREFERENCE);
    }
    
    public QueryPropertyMarker(int id) {
        super(id);
    }
    
    public QueryPropertyMarker(Parser p, int id) {
        super(p, id);
    }
    
    /**
     * This will create a structure as follows around the specified node: Reference (this node) Reference Expression AND Reference Reference Expression
     * Assignment Reference Identifier:(the class' label) True Reference Reference Expression source (the one specified)
     * 
     * Hence the resulting expression will be (({label} = True) AND ({specified node}))
     * 
     * @param source
     */
    public QueryPropertyMarker(JexlNode source) {
        this();
        
        setupSource(source);
    }
    
    /**
     * Return the identifier to use when marking a node as a specific {@link QueryPropertyMarker} type. This method must be overridden by all sub-types.
     * 
     * @return the short label
     */
    public abstract String getLabel();
    
    protected void setupSource(JexlNode source) {
        this.jjtSetParent(source.jjtGetParent());
        
        // create the assignment using the label wrapped in an expression
        JexlNode refNode1 = JexlNodeFactory.createExpression(JexlNodeFactory.createAssignment(getLabel(), true));
        
        // wrap the source in an expression, but only if needed
        JexlNode refNode2 = JexlNodeFactory.createExpression(source);
        
        // wrap the assignment and source in an AND node
        JexlNode andNode = JexlNodeFactory.createUnwrappedAndNode(Arrays.asList(refNode1, refNode2));
        
        // wrap the and node with an expression (see JexlNodeFactory.createAndNode)
        ASTReferenceExpression refExpNode1 = new ASTReferenceExpression(ParserTreeConstants.JJTREFERENCEEXPRESSION);
        andNode.jjtSetParent(refExpNode1);
        refExpNode1.jjtAddChild(andNode, 0);
        
        // and make a child of this
        refExpNode1.jjtSetParent(this);
        this.jjtAddChild(refExpNode1, 0);
    }
    
    /**
     * A routine to determine whether an and node is actually a specific instance of a query marker. The reason for this routine is that if the query is
     * serialized and deserialized, then only the underlying assignment will persist. Any node within the tree originally created except for a sibling of the
     * source can be used here.
     *
     * @param node
     * @param type
     *            The type to look for
     * @return true if this and node is a query marker
     */
    public static boolean instanceOf(JexlNode node, Class<? extends QueryPropertyMarker> type) {
        return QueryPropertyMarkerVisitor.instanceOf(node, type);
    }
    
    /**
     * A routine to determine the node which is the source of the query property (i.e. the one passed into the constructor of this class)
     *
     * @param node
     * @param type
     *            The type to look for
     * @return the source node or null if not an a query property marker
     */
    public static JexlNode getQueryPropertySource(JexlNode node, Class<? extends QueryPropertyMarker> type) {
        List<JexlNode> sourceNodes = new ArrayList<>();
        if (QueryPropertyMarkerVisitor.instanceOf(node, type, sourceNodes) && !sourceNodes.isEmpty()) {
            if (sourceNodes.size() == 1)
                return sourceNodes.get(0);
            else
                return JexlNodeFactory.createUnwrappedAndNode(sourceNodes);
        }
        
        return null;
    }
}
