package nsa.datawave.query.rewrite.jexl.nodes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import nsa.datawave.query.rewrite.jexl.JexlASTHelper;
import nsa.datawave.query.rewrite.jexl.JexlNodeFactory;

import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTAssignment;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.Parser;
import org.apache.commons.jexl2.parser.ParserTreeConstants;

/**
 * This is a node that can be put in place of an underlying reference node to place a property on an underlying query sub-tree (e.g. ExceededValueThreshold)
 */
public class QueryPropertyMarker extends ASTReference {
    
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
        JexlNode andNode = JexlNodeFactory.createUnwrappedAndNode(Arrays.asList(new JexlNode[] {refNode1, refNode2}));
        
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
        if (!_instanceOf(node, type, 0, null)) {
            // this could be the source node..so lets unwrap up to the parent AND
            int count = 0;
            // unwrap a max of two reference/reference expression parents
            while ((count < 2) && (node.jjtGetParent() != null)
                            && ((node.jjtGetParent() instanceof ASTReferenceExpression) || (node.jjtGetParent() instanceof ASTReference))
                            && (node.jjtGetParent().jjtGetNumChildren() == 1)) {
                node = node.jjtGetParent();
                count++;
            }
            
            // and now look for the and node
            if ((node.jjtGetParent() instanceof ASTAndNode) && (node.jjtGetParent().jjtGetNumChildren() == 2)) {
                node = node.jjtGetParent();
                return _instanceOf(node.jjtGetChild(0), type, 3, null);
            } else {
                return false;
            }
        } else {
            return true;
        }
    }
    
    /**
     * A routine to determine whether an and node is actually a specific instance of a query marker. The reason for this routine is that if the query is
     * serialized and deserialized, then only the underlying assignment will persist. This routine only searches downward for the marker. Hence any node within
     * the tree originally created except for anything under and including the reference node two notches above the source.
     * 
     * @param node
     * @param type
     *            The type to look for
     * @param depth
     *            The depth searched thus far
     * @return true if this and node is a query marker
     */
    private static boolean _instanceOf(JexlNode node, Class<? extends QueryPropertyMarker> type, int depth, List<JexlNode> sourceReturn) {
        if (node == null) {
            return false;
        }
        
        // if we are too deep and we have not found the assignment node yet, then false
        if (depth > 5) {
            return false;
        }
        // first check the simple case
        if (depth == 0) {
            if (type == null) {
                if (node instanceof QueryPropertyMarker) {
                    if (sourceReturn != null) {
                        sourceReturn.add(node.jjtGetChild(0).jjtGetChild(0).jjtGetChild(1).jjtGetChild(0).jjtGetChild(0));
                    }
                    return true;
                }
            } else {
                if (type.isInstance(node)) {
                    if (sourceReturn != null) {
                        sourceReturn.add(node.jjtGetChild(0).jjtGetChild(0).jjtGetChild(1).jjtGetChild(0).jjtGetChild(0));
                    }
                    return true;
                }
            }
        }
        
        if (node instanceof ASTReference && node.jjtGetNumChildren() == 1 && node.jjtGetChild(0) instanceof ASTReferenceExpression) {
            return _instanceOf(node.jjtGetChild(0), type, depth + 1, sourceReturn);
        }
        if (node instanceof ASTReferenceExpression && node.jjtGetNumChildren() == 1
                        && (node.jjtGetChild(0) instanceof ASTAndNode || node.jjtGetChild(0) instanceof ASTAssignment)) {
            return _instanceOf(node.jjtGetChild(0), type, depth + 1, sourceReturn);
        }
        if (node instanceof ASTAndNode && node.jjtGetNumChildren() == 2) {
            return _instanceOf(node.jjtGetChild(0), type, depth + 1, sourceReturn);
        }
        if (node instanceof ASTAssignment) {
            List<ASTIdentifier> ids = JexlASTHelper.getIdentifiers(node.jjtGetChild(0));
            if (ids.size() == 1) {
                if ((node.jjtGetParent() instanceof ASTReferenceExpression) && (node.jjtGetParent().jjtGetParent() instanceof ASTReference)
                                && (node.jjtGetParent().jjtGetParent().jjtGetParent() instanceof ASTAndNode)
                                && (node.jjtGetParent().jjtGetParent().jjtGetParent().jjtGetNumChildren() == 2)
                                && (node.jjtGetParent().jjtGetParent().jjtGetParent().jjtGetChild(1) instanceof ASTReference)
                                && (node.jjtGetParent().jjtGetParent().jjtGetParent().jjtGetChild(1).jjtGetChild(0) instanceof ASTReferenceExpression)) {
                    if (type == null || type.getSimpleName().equalsIgnoreCase(JexlASTHelper.deconstructIdentifier(ids.get(0)))) {
                        if (sourceReturn != null) {
                            // go up 3 to the AndNode, and then go down 3 to the source node
                            sourceReturn.add(node.jjtGetParent().jjtGetParent().jjtGetParent().jjtGetChild(1).jjtGetChild(0).jjtGetChild(0));
                        }
                        return true;
                    }
                }
            }
        }
        return false;
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
        List<JexlNode> sourceReturn = new ArrayList<>();
        if (!_instanceOf(node, type, 0, sourceReturn) || sourceReturn.isEmpty()) {
            // this could be the source node..so lets unwrap up to the parent AND
            int count = 0;
            // unwrap a max of two reference/reference expression parents
            while ((count < 2) && (node.jjtGetParent() != null)
                            && ((node.jjtGetParent() instanceof ASTReferenceExpression) || (node.jjtGetParent() instanceof ASTReference))
                            && (node.jjtGetParent().jjtGetNumChildren() == 1)) {
                node = node.jjtGetParent();
                count++;
            }
            
            // and now look for the and node
            if ((node.jjtGetParent() instanceof ASTAndNode) && (node.jjtGetParent().jjtGetNumChildren() == 2)) {
                node = node.jjtGetParent();
                if (_instanceOf(node.jjtGetChild(0), type, 2, sourceReturn)) {
                    // now we should definitely have a source node
                    return sourceReturn.get(0);
                } else {
                    return null;
                }
            } else {
                return null;
            }
        } else {
            return sourceReturn.get(0);
        }
    }
    
}
