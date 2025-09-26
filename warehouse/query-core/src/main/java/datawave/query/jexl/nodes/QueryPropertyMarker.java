package datawave.query.jexl.nodes;

import static datawave.query.jexl.visitors.RebuildingVisitor.copy;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;
import org.apache.commons.jexl3.parser.ParserTreeConstants;

import com.google.common.collect.Lists;

import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.visitors.QueryPropertyMarkerVisitor;

/**
 * This is a node that can be put in place of a given node to place a property on an underlying query sub-tree (e.g. ExceededValueThreshold)
 */
public class QueryPropertyMarker {

    public enum MarkerType {
        INDEX_HOLE("_Hole_", false, true),
        DELAYED("_Delayed_", false, true),
        EVALUATION_ONLY("_Eval_", false, true),
        DROPPED("_Drop_", false, true),
        EXCEEDED_OR("_List_", true, true),
        EXCEEDED_TERM("_Term_", true, true),
        EXCEEDED_VALUE("_Value_", true, true),
        BOUNDED_RANGE("_Bounded_", false, false),
        LENIENT("_Lenient_", false, false),
        STRICT("_Strict_", false, false),
        PLAN("plan", false, false);

        private final String label;
        private final boolean ivarator;
        private final boolean delayed;

        MarkerType(String label, boolean ivarator, boolean delayed) {
            this.label = label;
            this.ivarator = ivarator;
            this.delayed = delayed;
        }

        public String getLabel() {
            return label;
        }

        public boolean isIvarator() {
            return ivarator;
        }

        public boolean isDelayed() {
            return delayed;
        }

        @Override
        public String toString() {
            return label;
        }

        public static MarkerType forLabel(String label) {
            MarkerType result = null;
            for (MarkerType type : MarkerType.values()) {
                if (type.getLabel().equals(label)) {
                    result = type;
                    break;
                }
            }
            return result;
        }
    }

    private static final Set<MarkerType> DELAYED_TYPES;
    private static final Set<MarkerType> IVARATOR_TYPES;

    static {
        HashSet<MarkerType> delayedTypes = new HashSet<>();
        HashSet<MarkerType> ivaratorTypes = new HashSet<>();

        for (MarkerType markerType : MarkerType.values()) {
            if (markerType.isDelayed()) {
                delayedTypes.add(markerType);
            }

            if (markerType.isIvarator()) {
                ivaratorTypes.add(markerType);
            }
        }

        DELAYED_TYPES = Collections.unmodifiableSet(delayedTypes);
        IVARATOR_TYPES = Collections.unmodifiableSet(ivaratorTypes);
    }

    public static Set<MarkerType> getIvaratorTypes() {
        return Collections.unmodifiableSet(IVARATOR_TYPES);
    }

    public static JexlNode create(JexlNode source, MarkerType type) {
        if (isSourceMarked(source, type) || isAncestorMarked(source, type)) {
            return source;
        }

        JexlNode parent = source.jjtGetParent();

        // create the assignment using the label wrapped in an expression
        JexlNode refExpNode1 = JexlNodeFactory.createExpression(JexlNodeFactory.createAssignment(type.getLabel(), true));

        // copy the source, and wrap in an expression, but only if needed
        JexlNode refExpNode2 = JexlNodeFactory.createExpression(copy(source));

        // wrap the assignment and source in an AND node
        JexlNode andNode = JexlNodeFactory.createAndNode(Arrays.asList(refExpNode1, refExpNode2));

        JexlNode finalNode = andNode;
        if (!(parent instanceof ASTReferenceExpression)) {
            // wrap the and node with an expression (see JexlNodeFactory.createAndNode)
            finalNode = JexlNodes.makeRefExp();

            andNode.jjtSetParent(finalNode);
            finalNode.jjtAddChild(andNode, 0);
        }

        return finalNode;
    }

    /**
     * Determines if this source node is already marked with the provided marker class
     *
     * @param source
     *            a JexlNode
     * @param type
     *            the marker type
     * @return true if this node is already marked with the provided marker class
     */
    public static boolean isSourceMarked(JexlNode source, MarkerType type) {
        return QueryPropertyMarker.findInstance(source).isType(type);
    }

    /**
     * Determine if one the source node's ancestors is already marked. This method does not check the source node for markers (see
     * {@link #isSourceMarked(JexlNode, MarkerType)}).
     *
     * Note: This method will recursively ascend the entire Jexl query tree to find a marked node. This imposes a non-zero cost for trees that are excessively
     * deep or unflattened.
     *
     * @param node
     *            a JexlNode
     * @param type
     *            the marker type
     * @return true
     */
    public static boolean isAncestorMarked(JexlNode node, MarkerType type) {
        JexlNode parent = node.jjtGetParent();
        if (parent != null) {
            // marker nodes are by definition the intersection of a marker and a source
            if (parent instanceof ASTAndNode && parent.jjtGetNumChildren() == 2) {
                if (QueryPropertyMarker.findInstance(parent).isType(type)) {
                    return true;
                }
            }
            return isAncestorMarked(parent, type);
        }
        return false;
    }

    /**
     * Unwrap a marker node, fully. Intended to handle the odd edge case when multiple instances of the same marker is applied to the same node
     *
     * @param node
     *            an arbitrary jexl node
     * @param type
     *            the marker type
     * @return the source node, or the original node if the source node is not marked
     */
    public static JexlNode unwrapFully(JexlNode node, MarkerType type) {
        QueryPropertyMarker.Instance instance = QueryPropertyMarker.findInstance(node);
        if (instance.isType(type)) {
            return unwrapFully(instance.getSource(), type);
        }
        return node;
    }

    /**
     * Determine if the specified node represents a query property marker, and return an {@link Instance} with the marker type and sources, if present.
     *
     * @param node
     *            the node
     * @return an {@link Instance}
     */
    public static Instance findInstance(JexlNode node) {
        return QueryPropertyMarkerVisitor.getInstance(node);
    }

    public static final class Instance {

        private static final Instance EMPTY_INSTANCE = new Instance(null, null);

        /**
         * Return an immutable instance of {@link Instance} with a null type and null source.
         *
         * @return the immutable empty instance
         */
        public static Instance of() {
            return EMPTY_INSTANCE;
        }

        /**
         * Return a new {@link Instance} with the specified type and sources.
         *
         * @param type
         *            the marker type
         * @param sources
         *            the sources
         * @return the new {@link Instance}
         */
        public static Instance of(MarkerType type, List<JexlNode> sources) {
            return new Instance(type, sources);
        }

        /**
         * Return a new {@link Instance} with the specified type and source.
         *
         * @param type
         *            the marker type
         * @param source
         *            the source
         * @return the new {@link Instance}
         */
        public static Instance of(MarkerType type, JexlNode source) {
            return new Instance(type, Lists.newArrayList(source));
        }

        /**
         * The {@link QueryPropertyMarker} type that the node this {@link Instance} represents is.
         */
        private final MarkerType type;

        private final List<JexlNode> sources;

        private Instance(MarkerType type, List<JexlNode> sources) {
            this.type = type;
            this.sources = sources == null ? Collections.emptyList() : sources;
        }

        /**
         * Return the class of the {@link QueryPropertyMarker} type that the node this instance represents is, or null if the node is not any marker type.
         *
         * @return the marker type this instance is, or null if the instance is not a marker
         */
        public MarkerType getType() {
            return type;
        }

        /**
         * Return whether this instance represents a node that is any {@link QueryPropertyMarker} type.
         *
         * @return true if this instance is any {@link QueryPropertyMarker} or false otherwise
         */
        public boolean isAnyType() {
            return type != null;
        }

        /**
         * Return whether this instance has the specified type.
         *
         * @param type
         *            the type
         * @return true if this instance has the specified type, or false otherwise
         */
        public boolean isType(MarkerType type) {
            return type == this.type;
        }

        /**
         * Return whether this instance represents a node that is a marker of any of the specified types.
         *
         * @param types
         *            the types
         * @return true if this instance is a marker of the specified types, or false otherwise
         */
        public boolean isAnyTypeOf(MarkerType... types) {
            return isAnyType() && Arrays.stream(types).anyMatch(this::isType);
        }

        /**
         * Return whether this instance represents a node that is a marker of the specified types.
         *
         * @param types
         *            the marker types
         * @return true if this instance is a marker of the specified types, or false otherwise
         * @throws java.lang.NullPointerException
         *             if the provided collection is null
         */
        public boolean isAnyTypeOf(Collection<MarkerType> types) {
            return isAnyType() && types.stream().anyMatch(this::isType);
        }

        /**
         * Return whether this instance represents a node that is a marker, but is not a marker of the specified types.
         *
         * @param types
         *            the types
         * @return true if this instance is a marker that is not any of the specified types, or false otherwise
         */
        public boolean isAnyTypeExcept(MarkerType... types) {
            return isAnyType() && Arrays.stream(types).noneMatch(this::isType);
        }

        /**
         * Return whether this instance represents a node that is a marker, but is not a marker of the specified types.
         *
         * @param types
         *            the types
         * @return true if this instance is a marker that is not any of the specified types, or false otherwise
         * @throws java.lang.NullPointerException
         *             if the provided collection is null
         */
        public boolean isAnyTypeExcept(Collection<MarkerType> types) {
            return isAnyType() && types.stream().noneMatch(this::isType);
        }

        /**
         * Return whether this instance represents a node that is not a marker of the specified types. It is possible that this instance is not a marker at all.
         *
         * @param types
         *            the types
         * @return true if this instance is not a marker of the specified types, or false otherwise
         */
        public boolean isNotAnyTypeOf(MarkerType... types) {
            return !isAnyType() || Arrays.stream(types).noneMatch(this::isType);
        }

        /**
         * Return whether this instance represents a node that is not a marker of the specified types. It is possible that this instance is not a marker at all.
         *
         * @param types
         *            the types
         * @return true if this instance is not a marker of any of the specified types, or false otherwise
         * @throws java.lang.NullPointerException
         *             if the provided collection is null
         */
        public boolean isNotAnyTypeOf(Collection<MarkerType> types) {
            return !isAnyType() || types.stream().noneMatch(this::isType);
        }

        /**
         * Return whether this instance is any delayed predicate type.
         *
         * @return true if this instance is a delayed predicate type, or false otherwise
         */
        public boolean isDelayedPredicate() {
            return isAnyTypeOf(DELAYED_TYPES);
        }

        /**
         * Return whether this instance is any ivarator type.
         *
         * @return true if this instance is an ivarator type, or false otherwise
         */
        public boolean isIvarator() {
            return isAnyTypeOf(IVARATOR_TYPES);
        }

        /**
         * Return the list of all source nodes found if this instance is a marker, or an empty list if no marker was found.
         *
         * @return the list of sources
         */
        public List<JexlNode> getSources() {
            return sources;
        }

        /**
         * Return the total number of sources.
         *
         * @return the number of sources
         */
        public int totalSources() {
            return sources.size();
        }

        /**
         * Return whether there is more than one source.
         *
         * @return true if there is more than one source or false otherwise
         */
        public boolean hasMutipleSources() {
            return totalSources() > 1;
        }

        /**
         * Return a singular source node. If only one source node was found, that specific source will be returned. If multiple source nodes were found, a new
         * unwrapped {@link ASTAndNode} will be returned with the source nodes as its children. NOTE: original parentage is preserved; the new
         * {@link ASTAndNode} will not be set as the parent for the source nodes, and thus a node with an invalid structure will be returned if there are
         * multiple sources.
         *
         * @return a singular source node if this instance is a marker, or null otherwise
         */
        public JexlNode getSource() {
            int totalSources = sources.size();
            // If there is only one source node, return that node.
            if (totalSources == 1) {
                return sources.get(0);
            } else if (totalSources > 1) {
                // If multiple source nodes are found, return them as children of a new AND node.
                ASTAndNode andNode = new ASTAndNode(ParserTreeConstants.JJTANDNODE);
                JexlNodes.ensureCapacity(andNode, totalSources);
                for (int i = 0; i < totalSources; i++) {
                    andNode.jjtAddChild(sources.get(i), i);
                    // The parents of the source nodes are intentionally not updated to the new AND node.
                }
                return andNode;
            }
            // Return null if there are no sources.
            return null;
        }
    }
}
