package datawave.next;

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
import org.apache.commons.jexl3.parser.ASTMethodNode;
import org.apache.commons.jexl3.parser.ASTNENode;
import org.apache.commons.jexl3.parser.ASTNRNode;
import org.apache.commons.jexl3.parser.ASTNotNode;
import org.apache.commons.jexl3.parser.JexlNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.nodes.ExceededOr;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.jexl.visitors.BaseVisitor;
import datawave.query.jexl.visitors.PushdownNegationVisitor;
import datawave.query.jexl.visitors.RewriteNegationsVisitor;

/**
 * Validates that a query is composed of indexed terms with simple boolean logic. No index-only fields or functions.
 */
public class SimpleQueryVisitor extends BaseVisitor {

    private static final Logger log = LoggerFactory.getLogger(SimpleQueryVisitor.class);

    private final Set<String> indexedFields;
    private final Set<String> indexOnlyFields;

    // assume noble intentions
    private boolean valid = true;
    private boolean negationsExist = false;
    private boolean atLeastOneFieldIndexed = false;

    public SimpleQueryVisitor(Set<String> indexedFields, Set<String> indexOnlyFields) {
        this.indexedFields = indexedFields;
        this.indexOnlyFields = indexOnlyFields;
    }

    public boolean isValid() {
        // TODO: detect top level union, in which case all fields must be indexed
        boolean valid = this.valid && this.atLeastOneFieldIndexed && !negationsExist;
        if (!valid) {
            log.warn("DocumentScheduler rejected query, valid: {}, atLeastOneFieldIndexed: {}, negationsExist: {}", this.valid, atLeastOneFieldIndexed,
                            negationsExist);
        }
        return valid;
    }

    public static boolean validate(ASTJexlScript script, Set<String> indexedFields, Set<String> indexOnlyFields) {
        SimpleQueryVisitor visitor = new SimpleQueryVisitor(indexedFields, indexOnlyFields);
        script.jjtAccept(visitor, null);
        return visitor.isValid();
    }

    public Object visit(ASTAndNode node, Object data) {
        QueryPropertyMarker.Instance instance = QueryPropertyMarker.findInstance(node);
        if (instance != null && instance.getType() != null) {
            switch (instance.getType()) {
                case BOUNDED_RANGE:
                    handleBoundedRange(instance.getSource());
                    return data;
                case EXCEEDED_OR:
                    handleListMarker(instance.getSource());
                    return data;
                case EXCEEDED_VALUE:
                    node.childrenAccept(this, data);
                    return data;
                case INDEX_HOLE:
                case EVALUATION_ONLY:
                case DELAYED:
                case EXCEEDED_TERM:
                case DROPPED:
                case STRICT:
                case LENIENT:
                default:
                    return data;
            }
        }
        node.childrenAccept(this, data);
        return data;
    }

    private void handleBoundedRange(JexlNode node) {
        validateFields(node);
    }

    private void handleListMarker(JexlNode node) {
        ExceededOr exceededOr = new ExceededOr(node);
        validateField(exceededOr.getField());
    }

    @Override
    public Object visit(ASTEQNode node, Object data) {
        if (valid) {
            visitLeaf(node);
            node.childrenAccept(this, data);
        }
        return data;
    }

    @Override
    public Object visit(ASTNENode node, Object data) {
        valid = false;
        negationsExist = true;
        return data;
    }

    @Override
    public Object visit(ASTLTNode node, Object data) {
        valid = false; // range operators must be bounded
        return data;
    }

    @Override
    public Object visit(ASTGTNode node, Object data) {
        valid = false; // range operators must be bounded
        return data;
    }

    @Override
    public Object visit(ASTLENode node, Object data) {
        valid = false; // range operators must be bounded
        return data;
    }

    @Override
    public Object visit(ASTGENode node, Object data) {
        valid = false; // range operators must be bounded
        return data;
    }

    @Override
    public Object visit(ASTERNode node, Object data) {
        if (valid) {
            visitLeaf(node);
            node.childrenAccept(this, data);
        }
        return data;
    }

    @Override
    public Object visit(ASTNRNode node, Object data) {
        valid = false;
        negationsExist = true;
        return data;
    }

    /**
     * Negations are allowed provided the {@link RewriteNegationsVisitor} and {@link PushdownNegationVisitor} have run
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return the data
     */
    @Override
    public Object visit(ASTNotNode node, Object data) {
        if (valid) {
            JexlNode source = JexlASTHelper.dereference(node.jjtGetChild(0));
            if (source instanceof ASTEQNode) {
                Object literal = JexlASTHelper.getLiteralValue(source);
                if (literal == null) {
                    // this is a not-null term like "!(FIELD == null)"
                    return data;
                }
            }

            node.childrenAccept(this, data);
        }
        return data;
    }

    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        // assume all functions are okay when punting to the query iterator in a doc range
        return data;
    }

    @Override
    public Object visit(ASTMethodNode node, Object data) {
        // cannot handle methods
        valid = false;
        return data;
    }

    private void visitLeaf(JexlNode node) {

        // check the literal first. A term like "FIELD == null" does not contribute to finding candidate documents, but shouldn't fail the visitor either
        Object value = JexlASTHelper.getLiteralValue(node);
        if (value == null) {
            return;
        }

        validateFields(node);
    }

    private void validateFields(JexlNode node) {
        Set<String> fields = JexlASTHelper.getIdentifierNames(node);
        if (fields.isEmpty()) {
            // likely dealing with method functions
            valid = false;
        }

        for (String field : fields) {
            String deconstructed = JexlASTHelper.deconstructIdentifier(field);
            validateField(deconstructed);
        }
    }

    private void validateField(String field) {
        if (indexedFields.contains(field) || indexOnlyFields.contains(field)) {
            atLeastOneFieldIndexed = true;
        }
    }
}
