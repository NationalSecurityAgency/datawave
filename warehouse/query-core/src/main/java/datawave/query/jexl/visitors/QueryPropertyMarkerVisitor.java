package datawave.query.jexl.visitors;

import static org.apache.commons.jexl2.parser.JexlNodes.children;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTAssignment;
import org.apache.commons.jexl2.parser.ASTDelayedPredicate;
import org.apache.commons.jexl2.parser.ASTEvaluationOnly;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.JexlNode;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.nodes.BoundedRange;
import datawave.query.jexl.nodes.ExceededOrThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.ExceededTermThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.ExceededValueThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.IndexHoleMarkerJexlNode;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.jexl.nodes.QueryPropertyMarker.Instance;

/**
 * This class is used to determine whether the specified node is an instance of a query marker. The reason for this functionality is that if the query is
 * serialized and deserialized, then only the underlying assignment will persist. This class will identify the Reference, ReferenceExpression, or And nodes,
 * created by the original QueryPropertyMarker instance, as the marked node. Children of the marker node will not be identified as marked.
 */
public class QueryPropertyMarkerVisitor extends BaseVisitor {

    private static final Map<String,Class<? extends QueryPropertyMarker>> markers = new HashMap<>();
    private static final Set<String> registeredMarkers = new HashSet<>();

    /**
     * Register a query property marker type so that it may be identified by {@link QueryPropertyMarkerVisitor}.
     *
     * @param marker
     *            the marker type
     * @return true if the marker was not already registered, or false otherwise
     * @throws NoSuchMethodException
     *             if the marker type does not override {@link QueryPropertyMarker#label()}
     * @throws InvocationTargetException
     *             if the marker's label() method cannot be invoked
     * @throws IllegalAccessException
     *             if the marker's label() method cannot be accessed
     */
    public static boolean registerMarker(Class<? extends QueryPropertyMarker> marker)
                    throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Preconditions.checkNotNull(marker, "Marker class must not be null");

        // Check if this marker type has already been registered. This is a safeguard to avoid the reflection steps below if possible.
        if (registeredMarkers.contains(marker.getName())) {
            return false;
        } else {
            // Get the label via reflection by calling the static label() method.
            Method method = marker.getDeclaredMethod("label");
            String label = (String) method.invoke(null);

            // Verify the label returned is not null or empty. This will ensure a query marker is not registered without properly overriding the base
            // QueryPropertyMarker.label() method.
            if (label == null || label.isEmpty()) {
                throw new IllegalArgumentException("label() method must return a unique, non-empty label for type " + marker.getName());
            }

            // Verify the label is unique. This will ensure a query marker is not registered with a conflicting label.
            if (markers.containsKey(label)) {
                Class<? extends QueryPropertyMarker> existingMarker = markers.get(label);
                throw new IllegalArgumentException(marker.getName() + " has the same label as " + existingMarker.getName() + ", labels must be unique");
            }

            // Register the marker.
            markers.put(label, marker);
            registeredMarkers.add(marker.getName());
            return true;
        }
    }

    // Register known marker types.
    static {
        try {
            registerMarker(IndexHoleMarkerJexlNode.class);
            registerMarker(ASTDelayedPredicate.class);
            registerMarker(ASTEvaluationOnly.class);
            registerMarker(ExceededOrThresholdMarkerJexlNode.class);
            registerMarker(ExceededTermThresholdMarkerJexlNode.class);
            registerMarker(ExceededValueThresholdMarkerJexlNode.class);
            registerMarker(BoundedRange.class);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException("Failed to register default marker types for " + QueryPropertyMarkerVisitor.class.getName(), e);
        }
    }

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

    private Class<? extends QueryPropertyMarker> marker;
    private List<JexlNode> sourceNodes;
    private boolean visitedFirstAndNode;

    private QueryPropertyMarkerVisitor() {}

    @Override
    public Object visit(ASTAssignment node, Object data) {
        // Do not search for a marker in any assignment nodes that are not within the first AND node.
        if (visitedFirstAndNode) {
            String identifier = JexlASTHelper.getIdentifier(node);
            if (identifier != null) {
                marker = markers.get(identifier);
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
            JexlNode descendant = stack.pop();
            if (descendant instanceof ASTAndNode) {
                for (JexlNode sibling : children(descendant)) {
                    stack.push(sibling);
                }
            } else {
                children.add(descendant);
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
