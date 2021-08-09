package datawave.query.jexl.visitors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.nodes.BoundedRange;
import datawave.query.jexl.nodes.ExceededOrThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.ExceededTermThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.ExceededValueThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.IndexHoleMarkerJexlNode;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTAssignment;
import org.apache.commons.jexl2.parser.ASTDelayedPredicate;
import org.apache.commons.jexl2.parser.ASTEvaluationOnly;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.JexlNode;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.commons.jexl2.parser.JexlNodes.children;

/**
 * This class is used to determine whether the specified node is an instance of a query marker. The reason for this functionality is that if the query is
 * serialized and deserialized, then only the underlying assignment will persist. This class will identify the Reference, ReferenceExpression, or And nodes,
 * created by the original QueryPropertyMarker instance, as the marked node. Children of the marker node will not be identified as marked.
 */
public class QueryPropertyMarkerVisitor extends BaseVisitor {
    
    private static final Map<String,String> LABEL_MAP = new HashMap<>();
    
    // @formatter:off
    private static final Set<String> DEFAULT_TYPES = ImmutableSet.of(
                    IndexHoleMarkerJexlNode.label(),
                    ASTDelayedPredicate.label(),
                    ASTEvaluationOnly.label(),
                    ExceededOrThresholdMarkerJexlNode.label(),
                    ExceededTermThresholdMarkerJexlNode.label(),
                    ExceededValueThresholdMarkerJexlNode.label(),
                    BoundedRange.label());
    // @formatter:on
    
    // @formatter:off
    private static final List<Class<? extends QueryPropertyMarker>> DELAYED_PREDICATE_TYPES = ImmutableList.of(
                    IndexHoleMarkerJexlNode.class,
                    ASTDelayedPredicate.class,
                    ASTEvaluationOnly.class,
                    ExceededOrThresholdMarkerJexlNode.class,
                    ExceededTermThresholdMarkerJexlNode.class,
                    ExceededValueThresholdMarkerJexlNode.class);
    // @formatter:on
    
    // @formatter:off
    private static final List<Class<? extends QueryPropertyMarker>> IVARATOR_TYPES = ImmutableList.of(
            ExceededOrThresholdMarkerJexlNode.class,
            ExceededTermThresholdMarkerJexlNode.class,
            ExceededValueThresholdMarkerJexlNode.class);
    // @formatter:on
    
    private final Set<String> allowedTypes;
    private final Set<String> deniedTypes;
    private List<JexlNode> sourceNodes;
    
    private boolean allowedTypeFound = false;
    private boolean deniedTypeFound = false;
    
    public static boolean instanceOfAny(JexlNode node) {
        return instanceOfAny(node, null);
    }
    
    public static boolean instanceOfAny(JexlNode node, List<JexlNode> sourceNodes) {
        return instanceOf(node, null, null, sourceNodes);
    }
    
    public static boolean instanceOf(JexlNode node, Class<? extends QueryPropertyMarker> type) {
        return instanceOf(node, type, null);
    }
    
    public static boolean instanceOf(JexlNode node, Class<? extends QueryPropertyMarker> type, List<JexlNode> sourceNodes) {
        return instanceOf(node, type == null ? null : Collections.singleton(type), null, sourceNodes);
    }
    
    public static boolean instanceOf(JexlNode node, Collection<Class<? extends QueryPropertyMarker>> types) {
        return instanceOf(node, types, null);
    }
    
    public static boolean instanceOf(JexlNode node, Collection<Class<? extends QueryPropertyMarker>> types, List<JexlNode> sourceNodes) {
        return instanceOf(node, types, null, sourceNodes);
    }
    
    public static boolean instanceOfAnyExcept(JexlNode node, Class<? extends QueryPropertyMarker> deniedType) {
        return instanceOfAnyExcept(node, deniedType == null ? null : Collections.singleton(deniedType));
    }
    
    public static boolean instanceOfAnyExcept(JexlNode node, Collection<Class<? extends QueryPropertyMarker>> deniedTypes) {
        return instanceOfAnyExcept(node, deniedTypes, null);
    }
    
    public static boolean instanceOfAnyExcept(JexlNode node, Collection<Class<? extends QueryPropertyMarker>> deniedTypes, List<JexlNode> sourceNodes) {
        return instanceOf(node, null, deniedTypes, sourceNodes);
    }
    
    /**
     * Return whether or not the provided node is a delayed predicate type.
     *
     * @param node
     *            the node
     * @return true if the node is a delayed predicate, or false otherwise
     */
    public static boolean isDelayedPredicate(JexlNode node) {
        return instanceOf(node, DELAYED_PREDICATE_TYPES);
    }
    
    /**
     * Test a node to determine if it is an Ivarator node or not
     * 
     * @param node
     *            the node to test
     * @return true if the node is an instance of an IVARATOR_TYPE, false otherwise
     */
    public static boolean isIvarator(JexlNode node) {
        return instanceOf(node, IVARATOR_TYPES);
    }
    
    /**
     * Check a node for any QueryPropertyMarker in types as long as it doesn't have any QueryPropertyMarkers in except
     *
     * @param node
     * @param types
     * @param deniedTypes
     * @param sourceNodes
     * @return true if at least one of the types QueryPropertyMarkers exists and there are no QueryPropertyMarkers from except, false otherwise
     */
    public static boolean instanceOf(JexlNode node, Collection<Class<? extends QueryPropertyMarker>> types,
                    Collection<Class<? extends QueryPropertyMarker>> deniedTypes, List<JexlNode> sourceNodes) {
        if (node != null) {
            Set<String> allowed = types == null ? DEFAULT_TYPES : types.stream().map(QueryPropertyMarkerVisitor::getLabel).collect(Collectors.toSet());
            Set<String> denied = deniedTypes == null ? Collections.emptySet() : deniedTypes.stream().map(QueryPropertyMarkerVisitor::getLabel)
                            .collect(Collectors.toSet());
            
            QueryPropertyMarkerVisitor visitor = new QueryPropertyMarkerVisitor(allowed, denied);
            
            node.jjtAccept(visitor, null);
            
            if (visitor.allowedTypeFound) {
                if (sourceNodes != null)
                    for (JexlNode sourceNode : visitor.sourceNodes)
                        sourceNodes.add(trimReferenceNodes(sourceNode));
                return !visitor.deniedTypeFound;
            }
        }
        return false;
    }
    
    /**
     * Return the label associated with the particular {@link QueryPropertyMarker} type.
     * 
     * @param type
     *            the type to return the label for
     * @return the label to return
     * @throws java.lang.IllegalStateException
     *             if the provided type does not override {@link QueryPropertyMarker#getLabel()}, or if the overridden method returns a null or empty label.
     */
    private static String getLabel(Class<? extends QueryPropertyMarker> type) {
        String label = LABEL_MAP.get(type.getName());
        // If the label is null, this is the first time this type has been encountered in the visitor. Add the type's label.
        if (label == null) {
            try {
                // Get the label via reflection by calling the static label() method.
                Method method = type.getDeclaredMethod("label");
                label = (String) method.invoke(null);
                // Verify the label returned is not null or empty. This will ensure no new query marker types are created without properly overriding the base
                // QueryPropertyMarker.label() method.
                if (label == null || label.isEmpty()) {
                    throw new IllegalStateException("label() method returns null/empty label for type " + type.getName());
                }
                LABEL_MAP.put(type.getName(), label);
            } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                throw new IllegalStateException("Unable to invoke label() method for " + type.getName(), e);
            }
        }
        return label;
    }
    
    private static JexlNode trimReferenceNodes(JexlNode node) {
        if ((node instanceof ASTReference || node instanceof ASTReferenceExpression) && node.jjtGetNumChildren() == 1)
            return trimReferenceNodes(node.jjtGetChild(0));
        return node;
    }
    
    private QueryPropertyMarkerVisitor(Set<String> allowedTypes, Set<String> deniedTypes) {
        this.allowedTypes = allowedTypes;
        this.deniedTypes = deniedTypes;
    }
    
    @Override
    public Object visit(ASTAssignment node, Object data) {
        if (data != null) {
            @SuppressWarnings("unchecked")
            Set<String> foundIdentifiers = (Set<String>) data;
            
            String identifier = JexlASTHelper.getIdentifier(node);
            if (identifier != null) {
                foundIdentifiers.add(identifier);
            }
            
            if (deniedTypes.contains(identifier)) {
                deniedTypeFound = true;
            }
        }
        return null;
    }
    
    @Override
    public Object visit(ASTOrNode node, Object data) {
        return null;
    }
    
    @Override
    public Object visit(ASTAndNode node, Object data) {
        // if this is an and node, and it is the first one we've
        // found, it is our potential candidate
        if (data == null) {
            List<JexlNode> siblingNodes = new ArrayList<>();
            
            Deque<JexlNode> siblings = new LinkedList<>();
            Deque<JexlNode> stack = new LinkedList<>();
            stack.push(node);
            
            // for the purposes of this method, nested and nodes are
            // ignored, and their children are handled as direct children
            // of the parent and node.
            while (!stack.isEmpty()) {
                JexlNode descendant = stack.pop();
                
                if (descendant instanceof ASTAndNode) {
                    for (JexlNode sibling : children(descendant))
                        stack.push(sibling);
                } else {
                    siblings.push(descendant);
                }
            }
            
            // check each child to see if we found our identifier, and
            // save off the siblings as potential source nodes
            for (JexlNode child : siblings) {
                
                // don't look for identifiers if we already found what we were looking for
                if (!allowedTypeFound) {
                    Set<String> foundIdentifiers = new HashSet<>();
                    child.jjtAccept(this, foundIdentifiers);
                    
                    foundIdentifiers.retainAll(allowedTypes);
                    
                    // if we found our identifier, proceed to the next child node
                    if (!foundIdentifiers.isEmpty()) {
                        allowedTypeFound = true;
                        continue;
                    }
                }
                
                siblingNodes.add(child);
            }
            
            if (allowedTypeFound)
                sourceNodes = siblingNodes;
        }
        return null;
    }
}
