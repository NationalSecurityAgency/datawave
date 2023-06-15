package datawave.query.jexl.nodes;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.visitors.QueryPropertyMarkerVisitor;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTDelayedPredicate;
import org.apache.commons.jexl2.parser.ASTEvaluationOnly;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.JexlNodes;
import org.apache.commons.jexl2.parser.ParserTreeConstants;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * This is a node that can be put in place of an underlying reference node to place a property on an underlying query sub-tree (e.g. ExceededValueThreshold)
 */
public abstract class QueryPropertyMarker extends ASTReference {

    /**
     * Return the {@link QueryPropertyMarker}'s designated label. This method must be overridden in implementations of {@link QueryPropertyMarker} and is used
     * to identify instances of {@link QueryPropertyMarker} nodes in a query tree.
     *
     * @return the marker's label
     */
    public static String label() {
        throw new IllegalStateException("Label hasn't been configured in subclass.");
    }

    /**
     * Create and return a new query property marker with the given source node. If the source has a parent, the source will be replaced with the new marker
     * node in the parent's children.
     *
     * @param source
     *            the source node
     * @param constructor
     *            the marker type constructor
     * @param <MARKER>
     *            the {@link QueryPropertyMarker} type
     * @return a new marker instance
     */
    public static <MARKER extends QueryPropertyMarker> MARKER create(JexlNode source, Function<JexlNode,MARKER> constructor) {
        JexlNode parent = source.jjtGetParent();
        MARKER marker = constructor.apply(source);
        if (parent != null) {
            JexlNodes.replaceChild(parent, source, marker);
        }
        return marker;
    }

    /**
     * Create and return a new query property marker of the supplied marker type with the specified source via reflection. If the source node or one of its
     * ancestors is already marked with the supplied marker type the original source node is returned instead of a new marker instance.
     *
     * @param source
     *            the source node
     * @param markerType
     *            the marker type
     * @param <MARKER>
     *            the marker type
     * @return the new query property marker instance, or the original source if the marker has already been applied
     */
    public static <MARKER extends QueryPropertyMarker> JexlNode create(JexlNode source, Class<MARKER> markerType) {
        try {
            if (isSourceMarked(source, markerType) || isAncestorMarked(source, markerType)) {
                return source;
            }
            Constructor<MARKER> constructor = markerType.getConstructor(JexlNode.class);
            return constructor.newInstance(source);
        } catch (Exception e) {
            throw new IllegalArgumentException("Unable to create new instance of " + markerType, e);
        }
    }

    // @formatter:off
    @SuppressWarnings("unchecked")
    private static final Set<Class<? extends QueryPropertyMarker>> DELAYED_PREDICATES = ImmutableSet.of(
                    IndexHoleMarkerJexlNode.class,
                    ASTDelayedPredicate.class,
                    ASTEvaluationOnly.class,
                    ExceededOrThresholdMarkerJexlNode.class,
                    ExceededTermThresholdMarkerJexlNode.class,
                    ExceededValueThresholdMarkerJexlNode.class
    );
    // @formatter:on

    // @formatter:off
    private static final Set<Class<? extends QueryPropertyMarker>> IVARATORS = ImmutableSet.of(
                    ExceededTermThresholdMarkerJexlNode.class,
                    ExceededOrThresholdMarkerJexlNode.class,
                    ExceededValueThresholdMarkerJexlNode.class);
    // @formatter:on

    public QueryPropertyMarker() {
        this(ParserTreeConstants.JJTREFERENCE);
    }

    public QueryPropertyMarker(int id) {
        super(id);
    }

    /**
     * Returns a new instance of a query property marker with the specified source. This node's will be built around the provided source with the resulting
     * expression <code>((label = true) &amp;&amp; ({source}))</code> with the label supplied by {@link #getLabel()}, and the following tree structure:
     *
     * <pre>
     * Reference
     *  ReferenceExpression
     *    AndNode
     *      Reference
     *        ReferenceExpression
     *          Assignment
     *            Reference
     *              Identifier:{label}
     *            TrueNode
     *      Reference
     *        ReferenceExpression
     *          {source}
     * </pre>
     *
     * @param source
     *            the source of this property marker
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
        JexlNode parent = source.jjtGetParent();
        this.jjtSetParent(parent);

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

        // inserts new marker node in place of the original source
        if (parent != null) {
            JexlNodes.replaceChild(parent, source, this);
        }
    }

    /**
     * Determines if this source node is already marked with the provided marker class
     *
     * @param source
     *            a JexlNode
     * @param markerClass
     *            an instance of a {@link QueryPropertyMarker}
     * @return true if this node is already marked with the provided marker class
     */
    public static boolean isSourceMarked(JexlNode source, Class<? extends QueryPropertyMarker> markerClass) {
        return QueryPropertyMarker.findInstance(source).isType(markerClass);
    }

    /**
     * Determine if one the source node's ancestors is already marked. This method does not check the source node for markers (see
     * {@link #isSourceMarked(JexlNode, Class)}).
     *
     * Note: This method will recursively ascend the entire Jexl query tree to find a marked node. This imposes a non-zero cost for trees that are excessively
     * deep or unflattened.
     *
     * @param node
     *            a JexlNode
     * @param markerClass
     *            the marker class
     * @return true
     */
    public static boolean isAncestorMarked(JexlNode node, Class<? extends QueryPropertyMarker> markerClass) {
        JexlNode parent = node.jjtGetParent();
        if (parent != null) {
            // marker nodes are by definition the intersection of a marker and a source
            if (parent instanceof ASTAndNode && parent.jjtGetNumChildren() == 2) {
                if (QueryPropertyMarker.findInstance(parent).isType(markerClass)) {
                    return true;
                }
            }
            return isAncestorMarked(parent, markerClass);
        }
        return false;
    }

    /**
     * Unwrap a marker node, fully. Intended to handle the odd edge case when multiple instances of the same marker is applied to the same node
     *
     * @param node
     *            an arbitrary jexl node
     * @param marker
     *            the property marker
     * @return the source node, or the original node if the source node is not marked
     */
    public static JexlNode unwrapFully(JexlNode node, Class<? extends QueryPropertyMarker> marker) {
        QueryPropertyMarker.Instance instance = QueryPropertyMarker.findInstance(node);
        if (instance.isType(marker)) {
            return unwrapFully(instance.getSource(), marker);
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
         *            the type
         * @param sources
         *            the sources
         * @return the new {@link Instance}
         */
        public static Instance of(Class<? extends QueryPropertyMarker> type, List<JexlNode> sources) {
            return new Instance(type, sources);
        }

        /**
         * Return a new {@link Instance} with the specified type and source.
         *
         * @param type
         *            the type
         * @param source
         *            the source
         * @return the new {@link Instance}
         */
        public static Instance of(Class<? extends QueryPropertyMarker> type, JexlNode source) {
            return new Instance(type, Lists.newArrayList(source));
        }

        /**
         * The {@link QueryPropertyMarker} type that the node this {@link Instance} represents is.
         */
        private final Class<? extends QueryPropertyMarker> type;

        private final List<JexlNode> sources;

        private Instance(Class<? extends QueryPropertyMarker> type, List<JexlNode> sources) {
            this.type = type;
            this.sources = sources == null ? Collections.emptyList() : sources;
        }

        /**
         * Return the class of the {@link QueryPropertyMarker} type that the node this instance represents is, or null if the node is not any marker type.
         *
         * @return the marker type this instance is, or null if the instance is not a marker
         */
        public Class<? extends QueryPropertyMarker> getType() {
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
        public boolean isType(Class<? extends QueryPropertyMarker> type) {
            return Objects.equals(type, this.type);
        }

        /**
         * Return whether this instance represents a node that is a marker of any of the specified types.
         *
         * @param types
         *            the types
         * @return true if this instance is a marker of the specified types, or false otherwise
         */
        @SafeVarargs
        public final boolean isAnyTypeOf(Class<? extends QueryPropertyMarker>... types) {
            return isAnyType() && Arrays.stream(types).anyMatch(this::isType);
        }

        /**
         * Return whether this instance represents a node that is a marker of the specified types.
         *
         * @param types
         *            the types
         * @return true if this instance is a marker of the specified types, or false otherwise
         * @throws java.lang.NullPointerException
         *             if the provided collection is null
         */
        public boolean isAnyTypeOf(Collection<Class<? extends QueryPropertyMarker>> types) {
            return isAnyType() && types.stream().anyMatch(this::isType);
        }

        /**
         * Return whether this instance represents a node that is a marker, but is not a marker of the specified types.
         *
         * @param types
         *            the types
         * @return true if this instance is a marker that is not any of the specified types, or false otherwise
         */
        @SafeVarargs
        public final boolean isAnyTypeExcept(Class<? extends QueryPropertyMarker>... types) {
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
        public boolean isAnyTypeExcept(Collection<Class<? extends QueryPropertyMarker>> types) {
            return isAnyType() && types.stream().noneMatch(this::isType);
        }

        /**
         * Return whether this instance represents a node that is not a marker of the specified types. It is possible that this instance is not a marker at all.
         *
         * @param types
         *            the types
         * @return true if this instance is not a marker of the specified types, or false otherwise
         */
        @SafeVarargs
        public final boolean isNotAnyTypeOf(Class<? extends QueryPropertyMarker>... types) {
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
        public boolean isNotAnyTypeOf(Collection<Class<? extends QueryPropertyMarker>> types) {
            return !isAnyType() || types.stream().noneMatch(this::isType);
        }

        /**
         * Return whether this instance is any delayed predicate type.
         *
         * @return true if this instance is a delayed predicate type, or false otherwise
         */
        public boolean isDelayedPredicate() {
            return isAnyTypeOf(DELAYED_PREDICATES);
        }

        /**
         * Return whether this instance is any ivarator type.
         *
         * @return true if this instance is an ivarator type, or false otherwise
         */
        public boolean isIvarator() {
            return isAnyTypeOf(IVARATORS);
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
