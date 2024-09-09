package datawave.query.jexl.visitors;

import static datawave.query.jexl.functions.ContentFunctions.CONTENT_FUNCTION_NAMESPACE;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTGENode;
import org.apache.commons.jexl3.parser.ASTGTNode;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTLENode;
import org.apache.commons.jexl3.parser.ASTLTNode;
import org.apache.commons.jexl3.parser.ASTNENode;
import org.apache.commons.jexl3.parser.ASTNRNode;
import org.apache.commons.jexl3.parser.ASTNotNode;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.ASTReference;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.log4j.Logger;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.functions.ContentFunctionsDescriptor;
import datawave.query.jexl.functions.FunctionJexlNodeVisitor;
import datawave.query.jexl.functions.JexlFunctionArgumentDescriptorFactory;
import datawave.query.jexl.functions.arguments.JexlArgumentDescriptor;
import datawave.query.jexl.nodes.ExceededOr;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.util.TypeMetadata;

/**
 * Visitor that returns the <b>effective set</b> of ingest types associated with an arbitrary JexlNode.
 * <p>
 * The effective set is calculated by applying union and intersection logic to produce the reduced set of ingest types for complex tree structures. For example:
 * <p>
 * <code>(A AND B)</code> when the A term maps to ingest type 1 and the B term maps to ingest types 1, 2, and 3.
 * <p>
 * The full set of ingest types is {1, 2, 3}, but the <b>effective set</b> is just ingest type 1.
 * <p>
 * Much of this code is originated from the {@link IngestTypePruningVisitor}.
 * <p>
 * Note: IngestType and Datatype are used interchangeably, but Datatype does not refer to a data type such as {@link datawave.data.type.LcType}
 */
public class IngestTypeVisitor extends BaseVisitor {

    private static final Logger log = Logger.getLogger(IngestTypeVisitor.class);

    // in the case of arithmetic or a miss in the TypeMetadata
    public static final String UNKNOWN_TYPE = "UNKNOWN_TYPE";

    // in the case of a null literal or negated term (effectively non-executable terms)
    public static final String IGNORED_TYPE = "IGNORED_TYPE";

    // cache expensive calls to get ingest types per field
    private final TypeMetadata typeMetadata;
    private final Map<String,Set<String>> ingestTypeCache;

    // flag that determines how 'non-executable' nodes are handled
    private boolean external = false;

    public IngestTypeVisitor(TypeMetadata typeMetadata) {
        this.typeMetadata = typeMetadata;
        this.ingestTypeCache = new HashMap<>();
    }

    /**
     * Static entrypoint for determining the ingest types associated with a JexlNode
     *
     * @param node
     *            a JexlNode
     * @param typeMetadata
     *            the TypeMetadata
     * @return the set of ingest types for this node, or an empty set if no ingest types could be calculated
     */
    @SuppressWarnings("unchecked")
    public static Set<String> getIngestTypes(JexlNode node, TypeMetadata typeMetadata) {
        IngestTypeVisitor visitor = new IngestTypeVisitor(typeMetadata);
        Object o = node.jjtAccept(visitor, null);
        if (o instanceof Set) {
            Set<String> ingestTypes = (Set<String>) o;
            if (ingestTypes.contains(UNKNOWN_TYPE)) {
                // return just the UNKNOWN_TYPE
                ingestTypes.retainAll(Collections.singleton(UNKNOWN_TYPE));
                return ingestTypes;
            }
            return ingestTypes;
        }
        return new HashSet<>();
    }

    /**
     * Entrypoint for when this visitor is reused
     *
     * @param node
     *            a JexlNode
     * @return a set of ingest types for this node
     */
    public Set<String> getIngestTypes(JexlNode node) {
        if (isJunction(node)) {
            return getIngestTypesForJunction(node);
        } else {
            return getIngestTypesForLeaf(node);
        }
    }

    /**
     * Entrypoint for when this visitor is reused
     *
     * @param node
     *            a JexlNode
     * @param external
     *            a flag that determines how 'non-executable' node are handled
     * @return a set of ingest types for this node
     */
    public Set<String> getIngestTypes(JexlNode node, boolean external) {
        this.external = external;
        return (Set<String>) node.jjtAccept(this, null);
    }

    // leaf nodes

    @Override
    public Object visit(ASTEQNode node, Object data) {
        return getIngestTypes(node);
    }

    @Override
    public Object visit(ASTNENode node, Object data) {
        return getIngestTypes(node);
    }

    @Override
    public Object visit(ASTLTNode node, Object data) {
        return getIngestTypes(node);
    }

    @Override
    public Object visit(ASTGTNode node, Object data) {
        return getIngestTypes(node);
    }

    @Override
    public Object visit(ASTLENode node, Object data) {
        return getIngestTypes(node);
    }

    @Override
    public Object visit(ASTGENode node, Object data) {
        return getIngestTypes(node);
    }

    @Override
    public Object visit(ASTERNode node, Object data) {
        return getIngestTypes(node);
    }

    @Override
    public Object visit(ASTNRNode node, Object data) {
        return getIngestTypes(node);
    }

    // junction nodes

    @Override
    public Object visit(ASTJexlScript node, Object data) {
        return node.jjtGetChild(0).jjtAccept(this, data);
    }

    @Override
    public Object visit(ASTNotNode node, Object data) {
        if (external) {
            return node.jjtGetChild(0).jjtAccept(this, data);
        }
        Set<String> types = new HashSet<>();
        types.add(IGNORED_TYPE);
        return types;
    }

    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        return getIngestTypesForFunction(node);
    }

    @Override
    public Object visit(ASTReference node, Object data) {
        return getIngestTypes(node);
    }

    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        return getIngestTypes(node);
    }

    @Override
    public Object visit(ASTOrNode node, Object data) {
        return getIngestTypes(node);
    }

    /**
     * ASTAndNodes is where all the pruning logic is applied
     *
     * @param node
     *            an ASTAndNode
     * @param data
     *            some data
     * @return a set of ingest types
     */
    @Override
    public Object visit(ASTAndNode node, Object data) {

        QueryPropertyMarker.Instance instance = QueryPropertyMarker.findInstance(node);
        if (instance.isAnyType()) {
            return visitMarker(instance, node, data);
        }

        // special logic for getting types from an intersection
        return getIngestTypesForIntersection(node);
    }

    /**
     * Marker node visit is broken out for two primary reasons
     * <p>
     * First, the source may be an ExceededOrThresholdMarkerJexlNode which requires special handling
     * <p>
     * Second, the first child is an assignment node so the visitor must recurse through the second child
     *
     * @param instance
     *            a QueryPropertyMarker Instance
     * @param node
     *            the QueryPropertyMarker's root node
     * @param data
     *            the data
     * @return the set of ingest types associated with this node
     */
    @SuppressWarnings("unchecked")
    private Set<String> visitMarker(QueryPropertyMarker.Instance instance, JexlNode node, Object data) {

        // ExceededOr marker can be handled on its own
        if (instance.isType(QueryPropertyMarker.MarkerType.EXCEEDED_OR)) {
            String field = new ExceededOr(instance.getSource()).getField();
            return getIngestTypesForField(field);
        }

        JexlNode source = node.jjtGetChild(1);
        return (Set<String>) source.jjtAccept(this, data);
    }

    /**
     * A 'junction' node is not only a union or intersection, it is a non-leaf node.
     *
     * @param node
     *            a JexlNode
     * @return true if the node is a non-leaf
     */
    private boolean isJunction(JexlNode node) {
        JexlNode deref = JexlASTHelper.dereference(node);
        //  @formatter:off
        return deref instanceof ASTAndNode ||
                        deref instanceof ASTOrNode ||
                        deref instanceof ASTReference ||
                        deref instanceof ASTReferenceExpression ||
                        deref instanceof ASTNotNode;
        //  @formatter:on
    }

    @SuppressWarnings("unchecked")
    public Set<String> getIngestTypesForJunction(JexlNode node) {
        if (node instanceof ASTAndNode) {
            return getIngestTypesForIntersection((ASTAndNode) node);
        }

        Set<String> ingestTypes = new HashSet<>();
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            Set<String> found = (Set<String>) node.jjtGetChild(i).jjtAccept(this, null);
            ingestTypes.addAll(found);
        }

        if (ingestTypes.size() > 1) {
            ingestTypes.remove(IGNORED_TYPE);
        }

        return ingestTypes;
    }

    /**
     * In most cases a leaf will have a single field. In certain cases a function may produce more than one field, and in rare cases one may see leaf nodes like
     * <code>FIELD1 == FIELD2</code>
     *
     * @param node
     *            the leaf node
     * @return a set of ingestTypes
     */
    public Set<String> getIngestTypesForLeaf(JexlNode node) {
        node = JexlASTHelper.dereference(node);
        Set<String> ingestTypes = new HashSet<>();

        Object literal = JexlASTHelper.getLiteralValueSafely(node);
        if (literal == null && !external) {
            ingestTypes.add(IGNORED_TYPE);
            return ingestTypes;
        }

        Set<String> fields = getFieldsForLeaf(node);
        for (String field : fields) {
            ingestTypes.addAll(getIngestTypesForField(field));
        }
        if (fields.isEmpty()) {
            // could have nodes like arithmetic
            ingestTypes.add(UNKNOWN_TYPE);
        }
        return ingestTypes;
    }

    /**
     * Get fields for a leaf node
     *
     * @param node
     *            a leaf node
     * @return a set of ingest types
     */
    public Set<String> getFieldsForLeaf(JexlNode node) {
        Set<String> identifiers;
        JexlNode deref = JexlASTHelper.dereference(node);
        if (deref instanceof ASTFunctionNode) {
            try {
                identifiers = getFieldsForFunctionNode((ASTFunctionNode) deref);
            } catch (Exception e) {
                // if a FunctionsDescriptor throws an exception for any reason then return an empty collection
                // so the node gets treated as an unknown type
                return new HashSet<>();
            }
        } else {
            identifiers = JexlASTHelper.getIdentifierNames(deref);
        }

        //  @formatter:off
        return identifiers.stream()
                        .map(JexlASTHelper::deconstructIdentifier)
                        .collect(Collectors.toSet());
        //  @formatter:on
    }

    /**
     * Functions require a separate
     *
     * @param node
     *            an ASTFunctionNode
     * @return the set of ingest types
     */
    public Set<String> getIngestTypesForFunction(ASTFunctionNode node) {
        Set<String> fields = getFieldsForFunctionNode(node);

        if (fields == null) {
            throw new IllegalStateException("no fields should be an empty collection");
        }

        // trim any identifiers
        fields = fields.stream().map(JexlASTHelper::deconstructIdentifier).collect(Collectors.toSet());

        Set<String> types = new HashSet<>();

        // function fields are always treated as a union. there might be one exception to this.
        for (String field : fields) {
            types.addAll(getIngestTypesForField(field));
        }

        if (types.isEmpty()) {
            types.add(UNKNOWN_TYPE);
        }

        return types;
    }

    /**
     * Use the functions descriptor when getting fields for an {@link ASTFunctionNode}.
     *
     * @param node
     *            a function node
     * @return the function fields
     */
    private Set<String> getFieldsForFunctionNode(ASTFunctionNode node) {
        FunctionJexlNodeVisitor visitor = FunctionJexlNodeVisitor.eval(node);
        if (visitor.namespace().equals(CONTENT_FUNCTION_NAMESPACE)) {
            // all content function fields are added
            ContentFunctionsDescriptor.ContentJexlArgumentDescriptor contentDescriptor = new ContentFunctionsDescriptor().getArgumentDescriptor(node);
            return contentDescriptor.fieldsAndTerms(Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), null).getFields();
        } else {
            JexlArgumentDescriptor descriptor = JexlFunctionArgumentDescriptorFactory.F.getArgumentDescriptor(node);
            if (descriptor == null) {
                log.warn("could not get descriptor for function: " + JexlStringBuildingVisitor.buildQuery(node));
                return new HashSet<>();
            }
            return descriptor.fields(null, Collections.emptySet());
        }
    }

    /**
     * Wrapper around {@link TypeMetadata#getDataTypesForField(String)} that supports caching the results of a potentially expensive call.
     *
     * @param field
     *            the query field
     * @return the ingest types associated with the provided field
     */
    public Set<String> getIngestTypesForField(String field) {
        if (!ingestTypeCache.containsKey(field)) {
            Set<String> types = typeMetadata.getDataTypesForField(field);
            if (types.isEmpty()) {
                types.add(UNKNOWN_TYPE);
            }
            ingestTypeCache.put(field, types);
        }
        return ingestTypeCache.get(field);
    }

    /**
     * Get the effective ingest types for an intersection. This is not as simple as it first appears.
     * <p>
     * Consider the following queries where field A maps to datatype 1 and field B maps to datatype 2:
     * <p>
     * <code>A == '1' &amp;&amp; !(B == '2')</code>
     * </p>
     * <p>
     * <code>A == '1' &amp;&amp; B == null</code>
     * </p>
     * <p>
     * Both queries appear to be non-executable due to exclusive datatypes. A normal intersection of the A and B terms should produce an empty set. However, the
     * A term is executable while in both cases the B term acts as a filter. The B term is always true by definition of being an exclusive datatype, so this
     * visitor will return ingest type 1 for this intersection. The IngestTypePruningVisitor will correctly detect that the B term is prunable and remove it
     * from the query.
     *
     * @param node
     *            an AndNode
     * @return the effective ingest types for this intersection
     */
    @SuppressWarnings("unchecked")
    public Set<String> getIngestTypesForIntersection(ASTAndNode node) {
        Set<String> ingestTypes = new HashSet<>();
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            JexlNode child = JexlASTHelper.dereference(node.jjtGetChild(i));
            Set<String> childIngestTypes = (Set<String>) child.jjtAccept(this, null);

            if (childIngestTypes == null) {
                // we could have a malformed query or a query with a _Drop_ marker
                log.warn("potentially malformed query");
                childIngestTypes = new HashSet<>();
                childIngestTypes.add(IGNORED_TYPE);
            }

            if (ingestTypes.isEmpty()) {
                ingestTypes = childIngestTypes;
            } else {
                ingestTypes = intersectTypes(ingestTypes, childIngestTypes);
            }

            if (ingestTypes.isEmpty()) {
                // short circuit. no need to continue traversing the intersection.
                break;
            }
        }

        if (ingestTypes.size() > 1) {
            ingestTypes.remove(IGNORED_TYPE);
        }

        return ingestTypes;
    }

    /**
     * If either side of the intersection contains an UNKNOWN_TYPE we must persist that.
     *
     * @param typesA
     *            types for left side
     * @param typesB
     *            types for right side
     * @return the intersection of two sets of types, with special handling if an UNKNOWN type is present on either side.
     */
    private Set<String> intersectTypes(Set<String> typesA, Set<String> typesB) {
        if (typesA.contains(UNKNOWN_TYPE) || typesB.contains(UNKNOWN_TYPE)) {
            Set<String> unknown = new HashSet<>();
            unknown.add(UNKNOWN_TYPE);
            return unknown;
        }

        if ((typesA.contains(IGNORED_TYPE) && !typesB.contains(IGNORED_TYPE)) || (!typesA.contains(IGNORED_TYPE) && typesB.contains(IGNORED_TYPE))) {
            typesA.addAll(typesB);
            typesA.remove(IGNORED_TYPE);
            return typesA;
        }

        typesA.retainAll(typesB);
        return typesA;
    }
}
