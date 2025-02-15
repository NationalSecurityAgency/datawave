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

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.jexl.visitors.BaseVisitor;

/**
 * Validates that a query is composed of indexed terms with simple boolean logic. No index-only fields or functions.
 */
public class SimpleQueryVisitor extends BaseVisitor {

    private final Set<String> indexedFields;
    private final Set<String> indexOnlyFields;
    private final Set<String> nonEventFields;

    // assume noble intentions
    private boolean valid = true;
    private boolean negationsExist = false;
    private boolean atLeastOneFieldIndexed = false;

    public SimpleQueryVisitor(Set<String> indexedFields, Set<String> indexOnlyFields, Set<String> nonEventFields) {
        this.indexedFields = indexedFields;
        this.indexOnlyFields = indexOnlyFields;
        this.nonEventFields = nonEventFields;
    }

    public boolean isValid() {
        return this.valid && this.atLeastOneFieldIndexed && !negationsExist;
    }

    public static boolean validate(ASTJexlScript script, Set<String> indexedFields, Set<String> indexOnlyFields, Set<String> nonEventFields) {
        SimpleQueryVisitor visitor = new SimpleQueryVisitor(indexedFields, indexOnlyFields, nonEventFields);
        script.jjtAccept(visitor, null);
        return visitor.isValid();
    }

    public Object visit(ASTAndNode node, Object data) {
        QueryPropertyMarker.Instance instance = QueryPropertyMarker.findInstance(node);
        if (instance != null && instance.getType() != null) {
            switch (instance.getType()) {
                case BOUNDED_RANGE:
                case EXCEEDED_OR:
                    // assume we're good, even when we're not
                    valid = true;
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

    @Override
    public Object visit(ASTNotNode node, Object data) {
        valid = false;
        negationsExist = true;
        return data;
    }

    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        // be aggressive when invalidating a query. For example, query functions are okay. But we're going to ignore that for now.
        // valid = false;
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
        Set<String> fields = JexlASTHelper.getIdentifierNames(node);
        if (fields.isEmpty()) {
            // likely dealing with method functions
            valid = false;
        }

        for (String field : fields) {
            if (indexedFields.contains(field) || indexOnlyFields.contains(field)) {
                atLeastOneFieldIndexed = true;
                break;
            }
        }

        Object value = JexlASTHelper.getLiteralValue(node);
        if (value == null) {
            valid = false;
        }
    }
}
