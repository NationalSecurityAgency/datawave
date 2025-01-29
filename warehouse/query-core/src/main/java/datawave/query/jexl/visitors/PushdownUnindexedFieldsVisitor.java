package datawave.query.jexl.visitors;

import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.BOUNDED_RANGE;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.INDEX_HOLE;

import java.util.Set;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;
import org.apache.log4j.Logger;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.LiteralRange;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.util.MetadataHelper;

/**
 * Visitor meant to 'push down' predicates for expressions that are not executable against the index.
 */
public class PushdownUnindexedFieldsVisitor extends RebuildingVisitor {

    private static final Logger log = Logger.getLogger(PushdownUnindexedFieldsVisitor.class);

    protected ShardQueryConfiguration config;
    protected Set<String> unindexedFields;
    protected MetadataHelper helper;
    protected Set<String> dataTypeFilter;

    /**
     * Construct the visitor
     *
     * @param config
     *            the logic configuration
     * @param unindexedFields
     *            the fields being considered unindexed
     * @param helper
     *            the metadata helper
     * @param dataTypeFilter
     *            the data type filter
     */
    public PushdownUnindexedFieldsVisitor(ShardQueryConfiguration config, Set<String> unindexedFields, MetadataHelper helper, Set<String> dataTypeFilter) {
        this.config = config;
        this.unindexedFields = unindexedFields;
        this.helper = helper;
        this.dataTypeFilter = dataTypeFilter;
    }

    /**
     * helper method that constructs and applies the visitor.
     *
     * @param config
     *            a config
     * @Param unindexedFields the fields considered unindexed
     * @param queryTree
     *            the query tree
     * @param helper
     *            the metadata helper
     * @param dataTypeFilter
     *            the data type filter
     * @param <T>
     *            type of the query tree
     * @return a reference to the node
     */
    public static <T extends JexlNode> T pushdownPredicates(T queryTree, ShardQueryConfiguration config, Set<String> unindexedFields, MetadataHelper helper,
                    Set<String> dataTypeFilter) {
        PushdownUnindexedFieldsVisitor visitor = new PushdownUnindexedFieldsVisitor(config, unindexedFields, helper, dataTypeFilter);
        return (T) (queryTree.jjtAccept(visitor, null));
    }

    @Override
    public Object visit(ASTAndNode node, Object data) {
        // if not already delayed somehow
        if (!QueryPropertyMarker.findInstance(node).isAnyTypeExcept(BOUNDED_RANGE)) {
            LiteralRange range = JexlASTHelper.findRange().getRange(node);

            if (range != null) {
                return delayBoundedIndexHole(range, node, data);
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
     * @param data
     *            the node data
     * @param range
     *            the range
     * @param currentNode
     *            the current node
     * @return a jexl node
     */
    protected JexlNode delayBoundedIndexHole(LiteralRange range, ASTAndNode currentNode, Object data) {

        if (missingIndexRange(range)) {
            return QueryPropertyMarker.create(currentNode, INDEX_HOLE);
        } else {
            return currentNode;
        }

    }

    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        // if not already delayed somehow
        if (!QueryPropertyMarker.findInstance(node).isAnyTypeExcept(BOUNDED_RANGE)) {
            return super.visit(node, data);
        }
        return node;
    }

    @Override
    public Object visit(ASTEQNode node, Object data) {
        if (isIndexed(node) && missingIndexRange(node)) {
            return QueryPropertyMarker.create(node, INDEX_HOLE);
        }
        return node;
    }

    @Override
    public Object visit(ASTERNode node, Object data) {
        if (isIndexed(node) && missingIndexRange(node)) {
            return QueryPropertyMarker.create(node, INDEX_HOLE);
        }
        return node;
    }

    public boolean isIndexed(JexlNode node) {
        String field = JexlASTHelper.getIdentifier(node);
        try {
            return (field != null && this.helper.isIndexed(field, this.dataTypeFilter));
        } catch (TableNotFoundException e) {
            throw new IllegalStateException("Unable to find metadata table", e);
        }
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
