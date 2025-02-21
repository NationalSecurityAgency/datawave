package datawave.query.jexl.visitors;

import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.EVALUATION_ONLY;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.EXCEEDED_OR;

import java.util.Set;

import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTEWNode;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTGENode;
import org.apache.commons.jexl3.parser.ASTGTNode;
import org.apache.commons.jexl3.parser.ASTIdentifier;
import org.apache.commons.jexl3.parser.ASTLENode;
import org.apache.commons.jexl3.parser.ASTLTNode;
import org.apache.commons.jexl3.parser.ASTMethodNode;
import org.apache.commons.jexl3.parser.ASTNENode;
import org.apache.commons.jexl3.parser.ASTNEWNode;
import org.apache.commons.jexl3.parser.ASTNRNode;
import org.apache.commons.jexl3.parser.ASTNSWNode;
import org.apache.commons.jexl3.parser.ASTSWNode;
import org.apache.commons.jexl3.parser.JexlNode;
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
        QueryPropertyMarker.Instance marker = QueryPropertyMarker.findInstance(node);
        if (!marker.isAnyType()) {
            return super.visit(node, data);
        } else {
            switch (marker.getType()) {
                case EVALUATION_ONLY:
                    return copy(node);
                case BOUNDED_RANGE:
                    return delayBoundedRange(node);
                case EXCEEDED_VALUE:
                case EXCEEDED_OR:
                case DELAYED:
                    return delayByReplaceMarker(marker, node);
                case INDEX_HOLE:
                case DROPPED:
                case LENIENT:
                case STRICT:
                case EXCEEDED_TERM:
                default:
                    return super.visit(node, data);
            }
        }
    }

    @Override
    public Object visit(ASTEQNode node, Object data) {
        return delayExpression(node);
    }

    @Override
    public Object visit(ASTNENode node, Object data) {
        return delayExpression(node);
    }

    @Override
    public Object visit(ASTLTNode node, Object data) {
        return delayExpression(node);
    }

    @Override
    public Object visit(ASTGTNode node, Object data) {
        return delayExpression(node);
    }

    @Override
    public Object visit(ASTLENode node, Object data) {
        return delayExpression(node);
    }

    @Override
    public Object visit(ASTGENode node, Object data) {
        return delayExpression(node);
    }

    @Override
    public Object visit(ASTERNode node, Object data) {
        return delayExpression(node);
    }

    @Override
    public Object visit(ASTNRNode node, Object data) {
        return delayExpression(node);
    }

    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        // no need to delay functions
        return copy(node);
    }

    @Override
    public Object visit(ASTMethodNode node, Object data) {
        // no need to delay methods
        return copy(node);
    }

    @Override
    protected Object visit(ASTSWNode node, Object data) {
        return delayExpression(node);
    }

    @Override
    protected Object visit(ASTNSWNode node, Object data) {
        return delayExpression(node);
    }

    @Override
    protected Object visit(ASTEWNode node, Object data) {
        return delayExpression(node);
    }

    @Override
    protected Object visit(ASTNEWNode node, Object data) {
        return delayExpression(node);
    }

    /**
     * Delay the ranges that overlap holes. The range map is expected to only be indexed ranges.
     *
     * @param node
     *            the current node
     * @return a jexl node
     */
    protected JexlNode delayBoundedRange(ASTAndNode node) {
        node = (ASTAndNode) copy(node);
        LiteralRange range = JexlASTHelper.findRange().getRange(node);

        if (range != null && missingIndexRange(range)) {
            if (log.isDebugEnabled()) {
                log.debug("Pushing down unindexed " + range);
            }
            return QueryPropertyMarker.create(node, EVALUATION_ONLY);
        } else {
            return node;
        }
    }

    protected JexlNode delayExpression(JexlNode node) {
        node = copy(node);
        if (missingIndex(node)) {
            if (log.isDebugEnabled()) {
                log.debug("Pushing down unindexed expression " + JexlStringBuildingVisitor.buildQuery(node));
            }
            return QueryPropertyMarker.create(node, EVALUATION_ONLY);
        } else {
            return node;
        }
    }

    protected JexlNode delayByReplaceMarker(QueryPropertyMarker.Instance marker, JexlNode node) {
        if (missingIndex(marker.getSource())) {
            // in the case of a list ivarator, we cannot simply make this eval only as the source node
            // cannot be evaluated per-se. This visitor must be executed before pushing down long lists.
            if (marker.isType(EXCEEDED_OR)) {
                throw new IllegalStateException("Cannot make a list ivarator evaluation only");
            }
            if (log.isDebugEnabled()) {
                log.debug("Pushing down unindexed expression " + JexlStringBuildingVisitor.buildQuery(node));
            }
            return QueryPropertyMarker.create(copy(marker.getSource()), EVALUATION_ONLY);
        } else {
            return copy(node);
        }
    }

    protected boolean missingIndex(JexlNode node) {
        for (ASTIdentifier identifier : JexlASTHelper.getIdentifiers(node)) {
            if (unindexedFields.contains(JexlASTHelper.deconstructIdentifier(identifier))) {
                return true;
            }
        }
        return false;
    }

    private boolean missingIndexRange(LiteralRange range) {
        return (unindexedFields.contains(range.getFieldName()));
    }

}
