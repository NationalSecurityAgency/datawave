package datawave.query.jexl.nodes;

import com.google.common.collect.ImmutableSet;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.visitors.QueryPropertyMarkerVisitor;
import org.apache.commons.jexl2.parser.ASTDelayedPredicate;
import org.apache.commons.jexl2.parser.ASTEvaluationOnly;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.Parser;
import org.apache.commons.jexl2.parser.ParserTreeConstants;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;

/**
 * This is a node that can be put in place of an underlying reference node to place a property on an underlying query sub-tree (e.g. ExceededValueThreshold)
 */
public abstract class QueryPropertyMarker extends ASTReference {
    
    public static String label() {
        throw new IllegalStateException("Label hasn't been configured in subclass.");
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
    
    public QueryPropertyMarker(Parser p, int id) {
        super(p, id);
    }
    
    /**
     * Returns a new instance of a query property marker with the specified source. This node's will be built around the provided source with the resulting
     * expression <code>(({markerClassName} = true) && ({source}))</code> with the following structure:
     *
     * <pre>
     * Reference
     *  ReferenceExpression
     *    AndNode
     *      Reference
     *        ReferenceExpression
     *          Assignment
     *            Reference
     *              Identifier:{class name}
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
     * Determine if the specified node represents a query property marker, and return an {@link Instance} with the marker type and source, if present.
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
         * Return a new {@link Instance} with the specified type and source.
         *
         * @param type
         *            the type
         * @param source
         *            the source
         * @return the new {@link Instance}
         */
        public static Instance of(Class<? extends QueryPropertyMarker> type, JexlNode source) {
            return new Instance(type, source);
        }
        
        /**
         * The {@link QueryPropertyMarker} type that the node this {@link Instance} represents is.
         */
        private final Class<? extends QueryPropertyMarker> type;
        
        private final JexlNode source;
        
        private Instance(Class<? extends QueryPropertyMarker> type, JexlNode source) {
            this.type = type;
            this.source = source;
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
         * Return whether or not this instance represents a node that is any {@link QueryPropertyMarker} type.
         *
         * @return true if this instance is any {@link QueryPropertyMarker} or false otherwise
         */
        public boolean isAnyType() {
            return type != null;
        }
        
        /**
         * Return whether or not this instance has the specified type.
         *
         * @param type
         *            the type
         * @return true if this instance has the specified type, or false otherwise
         */
        public boolean isType(Class<? extends QueryPropertyMarker> type) {
            return Objects.equals(type, this.type);
        }
        
        /**
         * Return whether or not this instance represents a node that is a marker of any of the specified types.
         *
         * @param types
         *            the types
         * @return true if this instance is a marker of any of the specified types, or false otherwise
         */
        @SafeVarargs
        public final boolean isAnyTypeOf(Class<? extends QueryPropertyMarker>... types) {
            return isAnyType() && Arrays.stream(types).anyMatch(this::isType);
        }
        
        /**
         * Return whether or not this instance represents a node that is a marker of any of the specified types.
         *
         * @param types
         *            the types
         * @return true if this instance is a marker of any of the specified types, or false otherwise
         * @throws java.lang.NullPointerException
         *             if the provided collection is null
         */
        public boolean isAnyTypeOf(Collection<Class<? extends QueryPropertyMarker>> types) {
            return isAnyType() && types.stream().anyMatch(this::isType);
        }
        
        /**
         * Return whether or not this instance represents a node that is a marker, but is not a marker of any of the specified types.
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
         * Return whether or not this instance represents a node that is a marker, but is not a marker of any of the specified types.
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
         * Return whether or not this instance represents a node that is not a marker of any of the specified types. It is possible that this instance is not a
         * marker at all.
         *
         * @param types
         *            the types
         * @return true if this instance is not a marker of any of the specified types, or false otherwise
         */
        @SafeVarargs
        public final boolean isNotAnyTypeOf(Class<? extends QueryPropertyMarker>... types) {
            return !isAnyType() || Arrays.stream(types).noneMatch(this::isType);
        }
        
        /**
         * Return whether or not this instance represents a node that is not a marker of any of the specified types. It is possible that this instance is not a
         * marker at all.
         *
         * @param types
         *            the types
         * @return true if this instance is not a marker of any of the specified types, or false otherwise
         * @throws java.lang.NullPointerException
         *             if the provided collection is null
         */
        public final boolean isNotAnyTypeOf(Collection<Class<? extends QueryPropertyMarker>> types) {
            return !isAnyType() || types.stream().noneMatch(this::isType);
        }
        
        /**
         * Return whether or not this instance is any delayed predicate type.
         *
         * @return true if this instance is a delayed predicate type, or false otherwise
         */
        public boolean isDelayedPredicate() {
            return isAnyTypeOf(DELAYED_PREDICATES);
        }
        
        /**
         * Return whether or not this instance is any ivarator type.
         *
         * @return true if this instance is an ivarator type, or false otherwise
         */
        public boolean isIvarator() {
            return isAnyTypeOf(IVARATORS);
        }
        
        /**
         * Return the source node for the query property if this instance is a marker, or null otherwise.
         *
         * @return the source node, possibly null
         */
        public JexlNode getSource() {
            return source;
        }
    }
}
