package datawave.query.jexl.nodes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import datawave.query.jexl.JexlNodeFactory;

import datawave.query.jexl.visitors.QueryPropertyMarkerVisitor;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.Parser;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.apache.log4j.Logger;

/**
 * This is a node that can be put in place of an underlying reference node to place a property on an underlying query sub-tree (e.g. ExceededValueThreshold)
 */
public class QueryPropertyMarker extends ASTReference {
    
    private static final Logger log = Logger.getLogger(QueryPropertyMarker.class);
    
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
     * Assignment Reference Identifier:(this class' simple name) True Reference Reference Expression source (the one specified)
     * 
     * Hence the resulting expression will be (({class name} = True) AND ({specified node}))
     * 
     * @param source
     */
    public QueryPropertyMarker(JexlNode source) {
        this();
        
        setupSource(source);
    }
    
    protected void setupSource(JexlNode source) {
        this.jjtSetParent(source.jjtGetParent());
        
        // create the assignment using the class name wrapped in an expression
        JexlNode refNode1 = JexlNodeFactory.createExpression(JexlNodeFactory.createAssignment(getClass().getSimpleName(), true));
        
        // wrap the source in an expression
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
        return QueryPropertyMarkerVisitor.instanceOf(node, type, null);
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
    
    /**
     * Find the QueryPropertyMarker node that would have been constructed from the source node. This reverses the setupSource() above
     * 
     * @param source
     *            the source node that would have been used to construct this QueryPropertyMarker
     * @param type
     *            a class extending QueryPropertyMarker to look for, or null to look for any QueryPropertyMarker. If null is specified, the first (closest to
     *            the source node) marker will be returned
     * @return the QueryPropertyMarker node associated with source, or null if one doesn't exist
     */
    public static JexlNode getQueryPropertyMarker(JexlNode source, Class<? extends QueryPropertyMarker> type) {
        JexlNode queryPropertyMarker = null;
        
        if (source != null) {
            JexlNode current = source;
            while (queryPropertyMarker == null && current != null) {
                // get the next parent reference
                current = unwrapParent(current);
                
                if (current != null) {
                    if (current instanceof ASTAndNode && QueryPropertyMarker.instanceOf(current, type)) {
                        // should be wrapped in a ASTReferenceExpression
                        JexlNode referenceAnd = current.jjtGetParent();
                        if (referenceAnd instanceof ASTReferenceExpression) {
                            JexlNode marker = referenceAnd.jjtGetParent();
                            if (marker instanceof ASTReference) {
                                queryPropertyMarker = marker;
                            }
                        }
                    } else if (!(current instanceof ASTAndNode) || !QueryPropertyMarker.instanceOf(current, null)) {
                        // if it's not an ASTAndNode or isn't an ASTAndNode that corresponds with a marker bail out
                        current = null;
                    }
                }
            }
        }
        
        return queryPropertyMarker;
    }
    
    /**
     * Unwrap ReferenceExpression/Reference nodes from node.jjtGetParent() until there are no more pairs of ASTReferenceExpression/ASTReference nodes, return
     * the last thing that wasn't
     * 
     * @param node
     *            to unwrap
     * @return the next parent that isn't a pair of ASTReferenceExpression/ASTReference nodes or null if there isn't one
     */
    private static JexlNode unwrapParent(JexlNode node) {
        JexlNode unwrapped = null;
        JexlNode current = node.jjtGetParent();
        while (current instanceof ASTReferenceExpression) {
            // now check for reference wrapper
            JexlNode reference = current.jjtGetParent();
            if (reference instanceof ASTReference) {
                current = reference.jjtGetParent();
                unwrapped = current;
            } else {
                // not valid structure, stop unwrapping
                current = null;
            }
        }
        
        return unwrapped;
    }
}
