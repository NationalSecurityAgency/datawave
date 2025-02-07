package datawave.query.jexl.visitors;

import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.BOUNDED_RANGE;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.EVALUATION_ONLY;

import java.util.Set;

import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;
import org.apache.log4j.Logger;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.LiteralRange;
import datawave.query.jexl.nodes.QueryPropertyMarker;

/**
 * Visitor meant to 'push down' predicates for expressions that are not executable against the global OR field index.
 */
public class PushdownUnindexedFieldsVisitor extends RebuildingVisitor {

    private static final Logger log = Logger.getLogger(PushdownUnindexedFieldsVisitor.class);

    protected Set<String> unindexedFields;

    /**
     * Construct the visitor
     *
     * @param unindexedFields
     *            the fields being considered unindexed
     */
    public PushdownUnindexedFieldsVisitor(Set<String> unindexedFields) {
        this.unindexedFields = unindexedFields;
    }

    /**
     * helper method that constructs and applies the visitor.
     *
     * @param unindexedFields
     *            the fields considered unindexed
     * @param queryTree
     *            the query tree
     * @param <T>
     *            type of the query tree
     * @return a reference to the node
     */
    public static <T extends JexlNode> T pushdownPredicates(T queryTree, Set<String> unindexedFields) {
        PushdownUnindexedFieldsVisitor visitor = new PushdownUnindexedFieldsVisitor(unindexedFields);
        return (T) (queryTree.jjtAccept(visitor, null));
    }

    @Override
    public Object visit(ASTAndNode node, Object data) {
        // if not already delayed somehow
        if (QueryPropertyMarker.findInstance(node).isType(BOUNDED_RANGE)) {
            LiteralRange range = JexlASTHelper.findRange().getRange(node);

            if (range != null) {
                return delayBoundedIndexHole(range, node);
            } else {
                JexlNode andNode = JexlNodes.newInstanceOfType(node);
                JexlNodes.copyIdentifierOrLiteral(node, andNode);
                andNode.jjtSetParent(node.jjtGetParent());

                // We have no bounded range to replace, just proceed as normal
                JexlNodes.ensureCapacity(andNode, node.jjtGetNumChildren());
                for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                    JexlNode newChild = (JexlNode) node.jjtGetChild(i).jjtAccept(this, data);
                    andNode.jjtAddChild(newChild, i);
                    newChild.jjtSetParent(andNode);
                }
                return andNode;
            }
        } else {
            return node;
        }
    }

    /**
     * Delay the ranges that overlap holes. The range map is expected to only be indexed ranges.
     *
     * @param range
     *            the range
     * @param currentNode
     *            the current node
     * @return a jexl node
     */
    protected JexlNode delayBoundedIndexHole(LiteralRange range, ASTAndNode currentNode) {

        if (missingIndexRange(range)) {
            if (log.isDebugEnabled()) {
                log.debug("Pushing down unindexed " + range);
            }
            return QueryPropertyMarker.create(currentNode, EVALUATION_ONLY);
        } else {
            return currentNode;
        }

    }

    @Override
    public Object visit(ASTEQNode node, Object data) {
        if (missingIndexRange(node)) {
            if (log.isDebugEnabled()) {
                log.debug("Pushing down unindexed " + JexlStringBuildingVisitor.buildQuery(node));
            }
            return QueryPropertyMarker.create(node, EVALUATION_ONLY);
        }
        return node;
    }

    @Override
    public Object visit(ASTERNode node, Object data) {
        if (missingIndexRange(node)) {
            if (log.isDebugEnabled()) {
                log.debug("Pushing down unindexed " + JexlStringBuildingVisitor.buildQuery(node));
            }
            return QueryPropertyMarker.create(node, EVALUATION_ONLY);
        }
        return node;
    }

    private boolean missingIndexRange(ASTEQNode node) {
        return unindexedFields.contains(JexlASTHelper.getIdentifier(node));
    }

    private boolean missingIndexRange(ASTERNode node) {
        return unindexedFields.contains(JexlASTHelper.getIdentifier(node));
    }

    private boolean missingIndexRange(LiteralRange range) {
        return (unindexedFields.contains(range.getFieldName()));
    }

}
