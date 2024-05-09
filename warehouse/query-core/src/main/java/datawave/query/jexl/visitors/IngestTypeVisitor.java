package datawave.query.jexl.visitors;

import static datawave.query.jexl.functions.ContentFunctions.CONTENT_FUNCTION_NAMESPACE;
import static datawave.query.jexl.functions.EvaluationPhaseFilterFunctions.EVAL_PHASE_FUNCTION_NAMESPACE;
import static datawave.query.jexl.functions.GroupingRequiredFilterFunctions.GROUPING_REQUIRED_FUNCTION_NAMESPACE;

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

import datawave.core.query.jexl.functions.FunctionJexlNodeVisitor;
import datawave.core.query.jexl.nodes.ExceededOr;
import datawave.core.query.jexl.nodes.QueryPropertyMarker;
import datawave.core.query.jexl.visitors.BaseVisitor;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.functions.ContentFunctionsDescriptor;
import datawave.query.jexl.functions.EvaluationPhaseFilterFunctionsDescriptor;
import datawave.query.jexl.functions.GeoWaveFunctions;
import datawave.query.jexl.functions.GeoWaveFunctionsDescriptor;
import datawave.query.jexl.functions.GroupingRequiredFilterFunctionsDescriptor;
import datawave.query.jexl.functions.QueryFunctions;
import datawave.query.jexl.functions.QueryFunctionsDescriptor;
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
 */
public class IngestTypeVisitor extends BaseVisitor {

    private static final Logger log = Logger.getLogger(IngestTypeVisitor.class);

    protected static final String UNKNOWN_TYPE = "UNKNOWN_TYPE";
    // cache expensive calls to get ingest types per field
    private final TypeMetadata typeMetadata;
    private final Map<String,Set<String>> ingestTypeCache;

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
            if (!ingestTypes.contains(UNKNOWN_TYPE)) {
                return ingestTypes;
            }
        }
        return Collections.emptySet();
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
        return getIngestTypes(node);
    }

    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        return getIngestTypes(node);
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

        // getting ingest types for an intersection is different
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
        Set<String> ingestTypes = new HashSet<>();
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            Set<String> found = (Set<String>) node.jjtGetChild(i).jjtAccept(this, null);
            ingestTypes.addAll(found);
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
        if (node instanceof ASTEQNode) {
            Object literal = JexlASTHelper.getLiteralValueSafely(node);
            if (literal == null) {
                return Collections.singleton(UNKNOWN_TYPE);
            }
        }

        Set<String> ingestTypes = new HashSet<>();
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
        JexlNode deref = JexlASTHelper.dereference(node);
        if (deref instanceof ASTFunctionNode) {
            try {
                return getFieldsForFunctionNode((ASTFunctionNode) deref);
            } catch (Exception e) {
                // if a FunctionsDescriptor throws an exception for any reason then return an empty collection
                // so the node gets treated as an unknown type
                return Collections.emptySet();
            }
        }

        //  @formatter:off
        return JexlASTHelper.getIdentifierNames(deref)
                        .stream()
                        .map(JexlASTHelper::deconstructIdentifier)
                        .collect(Collectors.toSet());
        //  @formatter:on
    }

    private Set<String> getFieldsForFunctionNode(ASTFunctionNode node) {
        FunctionJexlNodeVisitor visitor = FunctionJexlNodeVisitor.eval(node);
        switch (visitor.namespace()) {
            case CONTENT_FUNCTION_NAMESPACE:
                // all content function fields are added
                ContentFunctionsDescriptor.ContentJexlArgumentDescriptor contentDescriptor = new ContentFunctionsDescriptor().getArgumentDescriptor(node);
                return contentDescriptor.fieldsAndTerms(Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), null)[0];
            case EVAL_PHASE_FUNCTION_NAMESPACE:
                // might be able to exclude certain evaluation phase functions from this step
                EvaluationPhaseFilterFunctionsDescriptor.EvaluationPhaseFilterJexlArgumentDescriptor evaluationDescriptor = (EvaluationPhaseFilterFunctionsDescriptor.EvaluationPhaseFilterJexlArgumentDescriptor) new EvaluationPhaseFilterFunctionsDescriptor()
                                .getArgumentDescriptor(node);
                return evaluationDescriptor.fields(null, Collections.emptySet());
            case GeoWaveFunctions.GEOWAVE_FUNCTION_NAMESPACE:
                GeoWaveFunctionsDescriptor.GeoWaveJexlArgumentDescriptor descriptor = (GeoWaveFunctionsDescriptor.GeoWaveJexlArgumentDescriptor) new GeoWaveFunctionsDescriptor()
                                .getArgumentDescriptor(node);
                return descriptor.fields(null, Collections.emptySet());
            case GROUPING_REQUIRED_FUNCTION_NAMESPACE:
                GroupingRequiredFilterFunctionsDescriptor.GroupingRequiredFilterJexlArgumentDescriptor groupingDescriptor = (GroupingRequiredFilterFunctionsDescriptor.GroupingRequiredFilterJexlArgumentDescriptor) new GroupingRequiredFilterFunctionsDescriptor()
                                .getArgumentDescriptor(node);
                return groupingDescriptor.fields(null, Collections.emptySet());
            case QueryFunctions.QUERY_FUNCTION_NAMESPACE:
                QueryFunctionsDescriptor.QueryJexlArgumentDescriptor queryDescriptor = (QueryFunctionsDescriptor.QueryJexlArgumentDescriptor) new QueryFunctionsDescriptor()
                                .getArgumentDescriptor(node);
                return queryDescriptor.fields(null, Collections.emptySet());
            default:
                // do nothing
                log.warn("Unhandled function namespace: " + visitor.namespace());
                return Collections.emptySet();
        }
    }

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

    @SuppressWarnings("unchecked")
    private Set<String> getIngestTypesForIntersection(ASTAndNode node) {
        Set<String> ingestTypes = new HashSet<>();
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            JexlNode child = JexlASTHelper.dereference(node.jjtGetChild(i));
            Set<String> childIngestTypes = (Set<String>) child.jjtAccept(this, null);

            if (childIngestTypes == null) {
                continue; // we could have a malformed query or a query with a _Drop_ marker
            }

            if (ingestTypes.isEmpty()) {
                ingestTypes = childIngestTypes;
            } else {
                if (child instanceof ASTNotNode) {
                    // special handling of negations. negated ingest types get OR'd together
                    ingestTypes.addAll(childIngestTypes);
                } else {
                    ingestTypes = intersectTypes(ingestTypes, childIngestTypes);
                }
            }

            if (ingestTypes.isEmpty()) {
                // short circuit. no need to continue traversing the intersection.
                break;
            }
        }
        return ingestTypes;
    }

    private Set<String> intersectTypes(Set<String> typesA, Set<String> typesB) {
        if (typesA.contains(UNKNOWN_TYPE) || typesB.contains(UNKNOWN_TYPE)) {
            return Collections.singleton(UNKNOWN_TYPE);
        }
        typesA.retainAll(typesB);
        return typesA;
    }
}
