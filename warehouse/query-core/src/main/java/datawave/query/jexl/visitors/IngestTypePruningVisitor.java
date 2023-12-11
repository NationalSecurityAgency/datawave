package datawave.query.jexl.visitors;

import static datawave.query.jexl.functions.ContentFunctions.CONTENT_FUNCTION_NAMESPACE;
import static datawave.query.jexl.functions.ContentFunctionsDescriptor.ContentJexlArgumentDescriptor;
import static datawave.query.jexl.functions.EvaluationPhaseFilterFunctions.EVAL_PHASE_FUNCTION_NAMESPACE;
import static datawave.query.jexl.functions.EvaluationPhaseFilterFunctionsDescriptor.EvaluationPhaseFilterJexlArgumentDescriptor;
import static datawave.query.jexl.functions.GeoWaveFunctionsDescriptor.GeoWaveJexlArgumentDescriptor;
import static datawave.query.jexl.functions.GroupingRequiredFilterFunctions.GROUPING_REQUIRED_FUNCTION_NAMESPACE;
import static datawave.query.jexl.functions.GroupingRequiredFilterFunctionsDescriptor.GroupingRequiredFilterJexlArgumentDescriptor;
import static datawave.query.jexl.functions.QueryFunctionsDescriptor.QueryJexlArgumentDescriptor;

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
import org.apache.commons.jexl3.parser.ASTLENode;
import org.apache.commons.jexl3.parser.ASTLTNode;
import org.apache.commons.jexl3.parser.ASTNENode;
import org.apache.commons.jexl3.parser.ASTNRNode;
import org.apache.commons.jexl3.parser.ASTNotNode;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.ASTReference;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;
import org.apache.commons.jexl3.parser.SimpleNode;
import org.apache.log4j.Logger;

import com.google.common.collect.Sets;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.functions.ContentFunctionsDescriptor;
import datawave.query.jexl.functions.EvaluationPhaseFilterFunctionsDescriptor;
import datawave.query.jexl.functions.FunctionJexlNodeVisitor;
import datawave.query.jexl.functions.GeoWaveFunctions;
import datawave.query.jexl.functions.GeoWaveFunctionsDescriptor;
import datawave.query.jexl.functions.GroupingRequiredFilterFunctionsDescriptor;
import datawave.query.jexl.functions.QueryFunctions;
import datawave.query.jexl.functions.QueryFunctionsDescriptor;
import datawave.query.jexl.nodes.ExceededOr;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.util.TypeMetadata;

/**
 * This visitor addresses the case when multiple ingest types share some but not all fields in a query
 * <p>
 * Consider the query <code>(A AND B)</code> where term A maps to ingest type 1 and term B maps to ingest type 2. No document will ever satisfy this
 * intersection. Thus, this should prune to zero terms.
 * <p>
 * Consider the query <code>(A AND (B OR C))</code> where term A and term B map to ingest type 1 and term C maps to ingest type 2. In this case term C should be
 * pruned from the nested union leaving the intersection <code>(A AND B)</code>
 * <p>
 */
public class IngestTypePruningVisitor extends BaseVisitor {
    private static final Logger log = Logger.getLogger(IngestTypePruningVisitor.class);

    private static final String UNKNOWN_TYPE = "UNKNOWN_TYPE";

    // cache expensive calls to get ingest types per field
    private final TypeMetadata typeMetadata;
    private final Map<String,Set<String>> ingestTypeCache;

    private int termsPruned = 0;
    private int nodesPruned = 0;

    public IngestTypePruningVisitor(TypeMetadata typeMetadata) {
        this.typeMetadata = typeMetadata;
        this.ingestTypeCache = new HashMap<>();
    }

    public static JexlNode prune(JexlNode node, TypeMetadata metadataHelper) {
        IngestTypePruningVisitor visitor = new IngestTypePruningVisitor(metadataHelper);
        node.jjtAccept(visitor, null);
        if (visitor.getTermsPruned() > 0) {
            log.info("pruned " + visitor.getTermsPruned() + " terms and " + visitor.getNodesPruned() + " nodes");
        }
        return node;
    }

    // leaf nodes

    @Override
    public Object visit(ASTEQNode node, Object data) {
        return visitOrPrune(node, data);
    }

    @Override
    public Object visit(ASTNENode node, Object data) {
        return visitOrPrune(node, data);
    }

    @Override
    public Object visit(ASTLTNode node, Object data) {
        return visitOrPrune(node, data);
    }

    @Override
    public Object visit(ASTGTNode node, Object data) {
        return visitOrPrune(node, data);
    }

    @Override
    public Object visit(ASTLENode node, Object data) {
        return visitOrPrune(node, data);
    }

    @Override
    public Object visit(ASTGENode node, Object data) {
        return visitOrPrune(node, data);
    }

    @Override
    public Object visit(ASTERNode node, Object data) {
        return visitOrPrune(node, data);
    }

    @Override
    public Object visit(ASTNRNode node, Object data) {
        return visitOrPrune(node, data);
    }

    // junction nodes

    @Override
    public Object visit(ASTNotNode node, Object data) {
        return visitOrPrune(node, data);
    }

    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        return visitOrPrune(node, data);
    }

    @Override
    public Object visit(ASTReference node, Object data) {
        return visitOrPrune(node, data);
    }

    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        return visitOrPrune(node, data);
    }

    @Override
    public Object visit(ASTOrNode node, Object data) {
        return visitOrPrune(node, data);
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
    @SuppressWarnings("unchecked")
    public Object visit(ASTAndNode node, Object data) {

        QueryPropertyMarker.Instance instance = QueryPropertyMarker.findInstance(node);
        if (instance.isAnyType()) {
            return visitMarker(instance, node, data);
        }

        // getting ingest types for an intersection is different
        Set<String> ingestTypes = getIngestTypesForIntersection(node);

        // automatically prune if there is no common ingest type
        if (ingestTypes.isEmpty()) {
            pruneNodeFromParent(node);
            return Collections.emptySet();
        }

        // the AndNode is where we can generate a set of ingest types used to prune child nodes
        // if the data object passed in is not a set, use the current set of ingest types to prune
        Set<String> pruningTypes;
        if (data instanceof Set) {
            pruningTypes = (Set<String>) data; // using the ingest types passed in
        } else {
            // prune using the aggregated ingest types
            // this handles the case of a nested union
            pruningTypes = ingestTypes;
        }

        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            node.jjtGetChild(i).jjtAccept(this, pruningTypes);
        }

        if (node.jjtGetNumChildren() == 0) {
            pruneNodeFromParent(node);
        }

        return ingestTypes;
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
            Set<String> ingestTypes = getIngestTypesForField(field);
            if (data instanceof Set<?>) {
                return pruneLeaf(ingestTypes, node, data);
            }
            return ingestTypes;
        }

        JexlNode source = node.jjtGetChild(1);
        Set<String> dts = (Set<String>) visit(source, data);

        if (source.jjtGetParent() == null) {
            pruneNodeFromParent(node);
        }

        return dts;
    }

    // pruning methods

    private Set<String> visitOrPrune(JexlNode node, Object data) {

        Set<String> ingestTypes = getIngestTypes(node);

        // check for pruning
        if (data instanceof Set<?>) {
            ingestTypes = prune(ingestTypes, node, data);
        }

        // if all children were pruned, also prune this node
        if (node.jjtGetNumChildren() == 0) {
            pruneNodeFromParent(node);
        }

        return ingestTypes;
    }

    private Set<String> prune(Set<String> ingestTypes, JexlNode node, Object data) {
        if (isJunction(node)) {
            return pruneJunction(node, data);
        } else {
            return pruneLeaf(ingestTypes, node, data);
        }
    }

    @SuppressWarnings("unchecked")
    private Set<String> pruneLeaf(Set<String> ingestTypes, JexlNode node, Object data) {
        boolean prune = shouldPrune(ingestTypes, (Set<String>) data);

        if (prune) {
            pruneNodeFromParent(node);
            termsPruned++;
        }
        return Collections.emptySet();
    }

    /**
     * Helper method that takes two sets of ingestTypes and determines if the current node can be pruned
     *
     * @param ingestTypes
     *            the ingestTypes for the current node
     * @param includes
     *            the ingestTypes used to prune
     * @return true if the current node should be pruned
     */
    private boolean shouldPrune(Set<String> ingestTypes, Set<String> includes) {

        // if either side has an UNKNOWN_TYPE, do not prune this node
        if (ingestTypes.contains(UNKNOWN_TYPE) || includes.contains(UNKNOWN_TYPE)) {
            return false;
        }

        // prune if there was no overlap
        return Sets.intersection(ingestTypes, includes).isEmpty();
    }

    private Set<String> pruneJunction(JexlNode node, Object data) {
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            node.jjtGetChild(i).jjtAccept(this, data);
        }
        return Collections.emptySet();
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

    // get ingest types

    private Set<String> getIngestTypes(JexlNode node) {
        if (isJunction(node)) {
            return getIngestTypesForJunction(node);
        } else {
            return getIngestTypesForLeaf(node);
        }
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
            return getFieldsForFunctionNode((ASTFunctionNode) deref);
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
                ContentJexlArgumentDescriptor contentDescriptor = new ContentFunctionsDescriptor().getArgumentDescriptor(node);
                return contentDescriptor.fieldsAndTerms(Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), null)[0];
            case EVAL_PHASE_FUNCTION_NAMESPACE:
                // might be able to exclude certain evaluation phase functions from this step
                EvaluationPhaseFilterJexlArgumentDescriptor evaluationDescriptor = (EvaluationPhaseFilterJexlArgumentDescriptor) new EvaluationPhaseFilterFunctionsDescriptor()
                                .getArgumentDescriptor(node);
                return evaluationDescriptor.fields(null, Collections.emptySet());
            case GeoWaveFunctions.GEOWAVE_FUNCTION_NAMESPACE:
                GeoWaveJexlArgumentDescriptor descriptor = (GeoWaveJexlArgumentDescriptor) new GeoWaveFunctionsDescriptor().getArgumentDescriptor(node);
                return descriptor.fields(null, Collections.emptySet());
            case GROUPING_REQUIRED_FUNCTION_NAMESPACE:
                GroupingRequiredFilterJexlArgumentDescriptor groupingDescriptor = (GroupingRequiredFilterJexlArgumentDescriptor) new GroupingRequiredFilterFunctionsDescriptor()
                                .getArgumentDescriptor(node);
                return groupingDescriptor.fields(null, Collections.emptySet());
            case QueryFunctions.QUERY_FUNCTION_NAMESPACE:
                QueryJexlArgumentDescriptor queryDescriptor = (QueryJexlArgumentDescriptor) new QueryFunctionsDescriptor().getArgumentDescriptor(node);
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
            JexlNode child = node.jjtGetChild(i);
            Set<String> childIngestTypes = (Set<String>) child.jjtAccept(this, null);

            ingestTypes = ingestTypes.isEmpty() ? childIngestTypes : intersectTypes(ingestTypes, childIngestTypes);

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
        return Sets.intersection(typesA, typesB);
    }

    private void pruneNodeFromParent(JexlNode node) {
        JexlNodes.removeFromParent(node.jjtGetParent(), node);
        nodesPruned++;
    }

    public int getTermsPruned() {
        return termsPruned;
    }

    public int getNodesPruned() {
        return nodesPruned;
    }
}
