package datawave.query.jexl.visitors;

import static datawave.query.jexl.visitors.IngestTypeVisitor.IGNORED_TYPE;
import static datawave.query.jexl.visitors.IngestTypeVisitor.UNKNOWN_TYPE;

import java.util.HashSet;
import java.util.Set;

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
import org.apache.commons.jexl3.parser.JexlNodes;
import org.apache.log4j.Logger;

import com.google.common.collect.Sets;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.nodes.ExceededOr;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.util.TypeMetadata;

/**
 * <p>
 * This visitor prunes terms from a query based on exclusive ingest types. The decision to prune and the type of pruning depends on whether the term contributes
 * positively or negatively to the overall evaluation, the type of term, and the parent junction.
 * </p>
 * <p>
 * Consider the query <code>(A AND B)</code> where term A maps to ingest type 1 and term B maps to ingest type 2. No document will ever satisfy this
 * intersection due to fully exclusive ingest types. Thus, this should prune to zero terms.
 * </p>
 * <p>
 * Consider the query <code>(A AND (B OR C))</code> where term A and term B map to ingest type 1 and term C maps to ingest type 2. A union requires outside
 * context to determine inclusive vs. exclusive ingest types, in this case the A term provides context. The C term is fully exclusive and is pruned leaving the
 * intersection <code>(A AND B)</code>.
 * </p>
 * <p>
 * Null terms for exclusive types evaluates to true by default and may be pruned from intersections, e.g. <code>A AND C == null</code>. A null term should prune
 * a whole union.
 * </p>
 * <p>
 * Negated terms for exclusive types may be pruned, e.g. <code>A AND !B</code>
 * </p>
 * <p>
 * Not Null terms should not be pruned in this visitor, e.g. <code>A AND !(C == null)</code>. A separate visitor to handle not null terms of exclusive types
 * should be used, similar to the {@link IsNotNullPruningVisitor}.
 * </p>
 * <p>
 * A not null term for exclusive types evaluates to false and may be pruned from an union, e.g. <code>A OR !(C == null)</code>. A not null term should prune a
 * whole intersection.
 * </p>
 */
public class IngestTypePruningVisitor extends BaseVisitor {
    private static final Logger log = Logger.getLogger(IngestTypePruningVisitor.class);

    private int termsPruned = 0;
    private int nodesPruned = 0;

    private final IngestTypeVisitor ingestTypeVisitor;

    private boolean globalExternal = false;
    private boolean localExternal = false;

    public IngestTypePruningVisitor(TypeMetadata typeMetadata) {
        this.ingestTypeVisitor = new IngestTypeVisitor(typeMetadata);
    }

    /**
     * Constructor for performing a self prune on the query tree
     *
     * @param node
     *            a JexlNode
     * @param metadataHelper
     *            an instance of TypeMetadata
     * @return a pruned query tree
     */
    public static JexlNode prune(JexlNode node, TypeMetadata metadataHelper) {
        return prune(node, metadataHelper, null);
    }

    /**
     * Constructor for pruning a query given a set of ingest types
     *
     * @param node
     *            a JexlNode
     * @param metadataHelper
     *            an instance of TypeMetadata
     * @param ingestTypes
     *            a set of ingest types used to prune the query tree
     * @return a pruned query tree
     */
    public static JexlNode prune(JexlNode node, TypeMetadata metadataHelper, Set<String> ingestTypes) {
        IngestTypePruningVisitor visitor = new IngestTypePruningVisitor(metadataHelper);
        visitor.setGlobalExternal();
        node.jjtAccept(visitor, ingestTypes);
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
    public Object visit(ASTJexlScript node, Object data) {
        return node.jjtGetChild(0).jjtAccept(this, data);
    }

    @Override
    public Object visit(ASTNotNode node, Object data) {
        JexlNode child = node.jjtGetChild(0);
        Object o = child.jjtAccept(this, data);

        if (child.jjtGetNumChildren() == 0) {
            pruneNodeFromParent(child);
        }

        if (node.jjtGetNumChildren() == 0) {
            pruneNodeFromParent(node);
        }

        return o;
    }

    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        return visitOrPrune(node, data);
    }

    @Override
    public Object visit(ASTReference node, Object data) {
        JexlNode child = node.jjtGetChild(0);
        Object o = child.jjtAccept(this, data);

        if (child.jjtGetNumChildren() == 0) {
            pruneNodeFromParent(child);
        }

        if (node.jjtGetNumChildren() == 0) {
            pruneNodeFromParent(node);
        }
        return o;
    }

    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        JexlNode child = node.jjtGetChild(0);
        Object o = child.jjtAccept(this, data);

        if (child.jjtGetNumChildren() == 0) {
            pruneNodeFromParent(child);
        }

        if (node.jjtGetNumChildren() == 0) {
            pruneNodeFromParent(node);
        }
        return o;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object visit(ASTOrNode node, Object data) {

        Set<String> types;
        if (data == null) {
            // normal visit

            types = new HashSet<>();
            // must traverse the children in reverse order because of pruning
            for (int i = node.jjtGetNumChildren() - 1; i >= 0; i--) {
                Set<String> childTypes = (Set<String>) node.jjtGetChild(i).jjtAccept(this, data);
                types.addAll(childTypes);
            }
        } else {
            // pruning visit
            Set<String> pruningTypes = (Set<String>) data;

            // must traverse the children in reverse order because of pruning
            for (int i = node.jjtGetNumChildren() - 1; i >= 0; i--) {
                JexlNode child = node.jjtGetChild(i);

                boolean prunableNegation = isNullEquality(child) || isNegatedLeaf(child);
                if (prunableNegation && shouldPruneChild(pruningTypes, child)) {
                    // null equalities for exclusive types evaluate to true
                    // negated leaf nodes for exclusive types evaluate to true
                    // in either case the whole union evaluates to true and can be pruned
                    JexlNodes.setChildren(node);
                    break;
                } else if (isNotNullEquality(child) && shouldPruneChild(pruningTypes, child)) {
                    // not null equality and exclusive type, term evaluates to false, drop from union
                    pruneNodeFromParent(child);
                } else {
                    child.jjtAccept(this, pruningTypes);
                }
            }
            types = pruningTypes;
        }

        // all children could self-prune, for example (A && B) || (C && D) when no term maps to the same datatype
        if (node.jjtGetNumChildren() == 0) {
            pruneNodeFromParent(node);
        }

        return types;
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
            int numChildren = node.jjtGetNumChildren();
            Object o = visitMarker(instance, node, data);
            if (node.jjtGetNumChildren() < numChildren) {
                pruneNodeFromParent(node);
            }
            return o;
        }

        // getting ingest types for an intersection is different
        Set<String> ingestTypes = ingestTypeVisitor.getIngestTypesForIntersection(node);

        // automatically prune if there is no common ingest type
        if (ingestTypes.isEmpty()) {
            pruneNodeFromParent(node);
            return new HashSet<>();
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

            if (isTopLevelIntersection(node)) {
                localExternal = true;
            }
        }

        // must traverse the children in reverse order because this visitor prunes as it visits
        for (int i = node.jjtGetNumChildren() - 1; i >= 0; i--) {
            JexlNode child = node.jjtGetChild(i);
            if (isNullEquality(child) && shouldPruneChild(pruningTypes, child)) {
                // null term for exclusive type causes the term to be dropped because it evaluates to true
                pruneNodeFromParent(child);
            } else if (isNotNullEquality(child) && shouldPruneChild(pruningTypes, child)) {
                // not null exclusive terms cause the whole intersection to be dropped
                JexlNodes.setChildren(node);
                break;
            } else {
                child.jjtAccept(this, pruningTypes);
            }
        }

        if (isTopLevelIntersection(node)) {
            localExternal = false;
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
            Set<String> ingestTypes = ingestTypeVisitor.getIngestTypesForField(field);
            if (data instanceof Set<?>) {
                return pruneLeaf(ingestTypes, node, data);
            }
            return ingestTypes;
        }

        JexlNode source = node.jjtGetChild(1);
        Set<String> dts = (Set<String>) source.jjtAccept(this, data);

        if (source.jjtGetParent() == null || source.jjtGetNumChildren() == 0) {
            pruneNodeFromParent(node);
        }

        return dts;
    }

    // pruning methods

    private Set<String> visitOrPrune(JexlNode node, Object data) {
        Set<String> ingestTypes = ingestTypeVisitor.getIngestTypes(node, (localExternal || globalExternal));

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
        return new HashSet<>();
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

        if (includes.contains(IGNORED_TYPE)) {
            throw new IllegalStateException("Should not attempt to prune using an IGNORED_TYPE");
        }

        if (ingestTypes.contains(IGNORED_TYPE)) {
            // technically shouldn't get here
            return false;
        }

        // prune if there was no overlap
        return Sets.intersection(ingestTypes, includes).isEmpty();
    }

    /**
     * Should the provided node be pruned based on a set of ingest types
     *
     * @param ingestTypes
     *            the ingest types that drive the prune
     * @param child
     *            an arbitrary jexl node
     * @return true if the jexl node should be pruned
     */
    private boolean shouldPruneChild(Set<String> ingestTypes, JexlNode child) {
        Set<String> nodeTypes = ingestTypeVisitor.getIngestTypes(child, globalExternal);
        return shouldPrune(ingestTypes, nodeTypes);
    }

    private Set<String> pruneJunction(JexlNode node, Object data) {
        // must traverse the children in reverse order because this visitor prunes as it visits
        for (int i = node.jjtGetNumChildren() - 1; i >= 0; i--) {
            node.jjtGetChild(i).jjtAccept(this, data);
        }
        return new HashSet<>();
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

    private boolean isTopLevelIntersection(JexlNode node) {
        if (!(node instanceof ASTAndNode)) {
            return false;
        }

        while (node.jjtGetParent() != null) {
            node = node.jjtGetParent();

            if (node instanceof ASTAndNode) {
                return false;
            }
        }

        return true;
    }

    private boolean isNullEquality(JexlNode node) {
        node = JexlASTHelper.dereference(node);

        if (node instanceof ASTEQNode) {
            return JexlASTHelper.getLiteralValueSafely(node) == null;
        }

        return false;
    }

    private boolean isNotNullEquality(JexlNode node) {
        node = JexlASTHelper.dereference(node);

        if (node instanceof ASTNotNode) {
            node = node.jjtGetChild(0);
            node = JexlASTHelper.dereference(node);
        } else {
            return false;
        }

        if (node instanceof ASTEQNode) {
            return JexlASTHelper.getLiteralValueSafely(node) == null;
        }

        return false;
    }

    private boolean isNegatedLeaf(JexlNode node) {
        node = JexlASTHelper.dereference(node);

        if (!(node instanceof ASTNotNode)) {
            return false;
        }

        node = node.jjtGetChild(0);
        node = JexlASTHelper.dereference(node);

        if (JexlASTHelper.getLiteralValueSafely(node) == null) {
            // if the final node has a null literal ignore it. Another method will handle this case.
            return false;
        }

        return !(node instanceof ASTAndNode || node instanceof ASTOrNode);
    }

    public void setGlobalExternal() {
        globalExternal = true;
    }
}
