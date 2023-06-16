package datawave.query.jexl.visitors;

import com.google.common.collect.Lists;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType;
import datawave.query.jexl.nodes.QueryPropertyMarker.Instance;
import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTAssignment;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.JexlNode;

import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

/**
 * This class is used to determine whether the specified node is an instance of a query marker. The reason for this functionality is that if the query is
 * serialized and deserialized, then only the underlying assignment will persist. This class will identify the ReferenceExpression, or And nodes, created by the
 * original QueryPropertyMarker instance, as the marked node. Children of the marker node will not be identified as marked.
 */
public class QueryPropertyMarkerVisitor extends BaseVisitor {
    
    /**
     * Examine the specified node to see if it represents a query property marker, and return an {@link Instance} with the marker's type and source node. If the
     * specified node is not a marker type, an empty {@link Instance} will be returned.
     *
     * @param node
     *            the node
     * @return an {@link Instance}
     */
    public static Instance getInstance(JexlNode node) {
        if (node != null) {
            QueryPropertyMarkerVisitor visitor = new QueryPropertyMarkerVisitor();
            node.jjtAccept(visitor, null);
            if (visitor.markerFound()) {
                return Instance.of(visitor.marker, visitor.sourceNodes);
            }
        }
        return Instance.of();
    }
    
    private MarkerType marker;
    private List<JexlNode> sourceNodes;
    private boolean visitedFirstAndNode;
    
    private QueryPropertyMarkerVisitor() {}
    
    @Override
    public Object visit(ASTAssignment node, Object data) {
        // Do not search for a marker in any assignment nodes that are not within the first AND node.
        if (visitedFirstAndNode) {
            String identifier = JexlASTHelper.getIdentifier(node);
            if (identifier != null) {
                marker = MarkerType.forLabel(identifier);
            }
        }
        return null;
    }
    
    @Override
    public Object visit(ASTOrNode node, Object data) {
        // Do not descend into children of OR nodes.
        return null;
    }
    
    @Override
    public Object visit(ASTAndNode node, Object data) {
        // Only the first AND node is a potential marker candidate.
        if (!visitedFirstAndNode) {
            visitedFirstAndNode = true;
            
            // Get the flattened children.
            List<JexlNode> children = getFlattenedChildren(node);
            
            // Examine each child for a marker identifier.
            List<JexlNode> siblings = new ArrayList<>();
            for (JexlNode child : children) {
                // Look for a marker only if one hasn't been found yet.
                if (!markerFound()) {
                    child.jjtAccept(this, null);
                    // / If a marker was found, continue and do not add this node as a sibling.
                    if (markerFound()) {
                        continue;
                    }
                }
                
                siblings.add(JexlASTHelper.dereference(child));
            }
            
            // If a marker was found, assign the source nodes.
            if (markerFound())
                sourceNodes = siblings;
        }
        return null;
    }
    
    /**
     * Return the flattened children of the specified node. Nested {@link ASTAndNode} nodes are ignored, and their children are considered direct children for
     * the parent {@link ASTAndNode} node.
     *
     * @param node
     *            the node to retrieve the flattened children of
     * @return the flattened children
     */
    private List<JexlNode> getFlattenedChildren(JexlNode node) {
        List<JexlNode> children = new ArrayList<>();
        Deque<JexlNode> stack = new LinkedList<>();
        stack.push(node);
        while (!stack.isEmpty()) {
            JexlNode descendent = stack.pop();
            if (descendent instanceof ASTAndNode) {
                for (int i = 0; i < descendent.jjtGetNumChildren(); i++) {
                    JexlNode sibling = descendent.jjtGetChild(i);
                    stack.push(sibling);
                }
            } else {
                children.add(descendent);
            }
        }
        // Ensure we return the children in their original order.
        return children.size() == 1 ? children : Lists.reverse(children);
    }
    
    /**
     * Return whether a {@link QueryPropertyMarker} has been found yet.
     *
     * @return true if a marker has been found, or false otherwise
     */
    private boolean markerFound() {
        return marker != null;
    }
}
