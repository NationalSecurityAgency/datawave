package datawave.query.jexl.visitors;

import java.util.Set;

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
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.JexlNode;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.functions.FunctionJexlNodeVisitor;
import datawave.query.jexl.nodes.QueryPropertyMarker;

/**
 * Determines if a query can disable evaluation when a grouping function is present
 * <p>
 * Evaluation cannot be disabled if any content, query, or filter functions exist, or if delayed or evaluation only markers are present.
 */
public class DisableEvaluationForGroupingVisitor extends ShortCircuitBaseVisitor {

    private boolean canDisableEvaluation = true;

    private final Set<String> indexedFields;
    private final Set<String> indexOnlyFields;

    /**
     * Public entrypoint, this visitor requires the full set of indexed and index only fields
     *
     * @param node
     *            the JexlNode
     * @param indexedFields
     *            the set of indexed fields
     * @param indexOnlyFields
     *            the set of index only fields
     * @return true if evaluation can be disabled
     */
    public static boolean canDisableEvaluation(JexlNode node, Set<String> indexedFields, Set<String> indexOnlyFields) {
        DisableEvaluationForGroupingVisitor visitor = new DisableEvaluationForGroupingVisitor(indexedFields, indexOnlyFields);
        node.jjtAccept(visitor, null);
        return visitor.canDisableEvaluation;
    }

    /**
     * Private constructor to force static access
     */
    private DisableEvaluationForGroupingVisitor(Set<String> indexedFields, Set<String> indexOnlyFields) {
        this.indexedFields = indexedFields;
        this.indexOnlyFields = indexOnlyFields;
    }

    @Override
    public Object visit(ASTAndNode node, Object data) {
        if (!canDisableEvaluation) {
            return data;
        }

        QueryPropertyMarker.Instance instance = QueryPropertyMarker.findInstance(node);
        if (instance.isAnyType()) {
            switch (instance.getType()) {
                case EVALUATION_ONLY:
                case DELAYED:
                    canDisableEvaluation = false;
                    return data;
                case BOUNDED_RANGE:
                case EXCEEDED_OR:
                case EXCEEDED_TERM:
                case EXCEEDED_VALUE:
                    // continue recursing
                    break;
                default:
                    // query planner should handled value, range terms
                    return data;
            }
        }

        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTOrNode node, Object data) {
        if (!canDisableEvaluation) {
            return data;
        }

        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTNENode node, Object data) {
        if (!canDisableEvaluation || !isFieldIndexed(node)) {
            return data;
        }
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTEQNode node, Object data) {
        if (!canDisableEvaluation || !isFieldIndexed(node)) {
            return data;
        }

        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTLTNode node, Object data) {
        if (!canDisableEvaluation || !isFieldIndexed(node)) {
            return data;
        }

        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTGTNode node, Object data) {
        if (!canDisableEvaluation || !isFieldIndexed(node)) {
            return data;
        }

        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTLENode node, Object data) {
        if (!canDisableEvaluation || !isFieldIndexed(node)) {
            return data;
        }

        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTGENode node, Object data) {
        if (!canDisableEvaluation || !isFieldIndexed(node)) {
            return data;
        }

        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTERNode node, Object data) {
        if (!canDisableEvaluation || !isFieldIndexed(node)) {
            return data;
        }

        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTNRNode node, Object data) {
        if (!canDisableEvaluation || !isFieldIndexed(node)) {
            return data;
        }

        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        if (!canDisableEvaluation) {
            return data;
        }

        FunctionJexlNodeVisitor visitor = new FunctionJexlNodeVisitor();
        visitor.visit(node, data);

        switch (visitor.namespace()) {
            case "filter":
            case "content":
            case "f":
                // must evaluate for these functions
                canDisableEvaluation = false;
                return data;
        }

        node.childrenAccept(this, data);
        return data;
    }

    private boolean isFieldIndexed(JexlNode node) {
        String field = JexlASTHelper.getIdentifier(node);
        if (field == null) {
            return false;
        }

        Object literal = JexlASTHelper.getLiteralValue(node);
        if (literal == null) {
            // term like (FIELD == null) requires an evaluation
            canDisableEvaluation = false;
            return false;
        }

        boolean indexed = indexedFields.contains(field) || indexOnlyFields.contains(field);

        if (!indexed) {
            canDisableEvaluation = false;
        }

        return indexed;
    }
}
