package datawave.query.jexl.visitors.pushdown;

import java.util.Set;

import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTAssignment;
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

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.jexl.visitors.ShortCircuitBaseVisitor;

/**
 * Determines if a subtree is an anchor for a given query
 * <p>
 * An anchor is defined as an executable leaf or subtree.
 */
public class AnchorDetectionVisitor extends ShortCircuitBaseVisitor {

    private final Set<String> indexedFields;
    private final Set<String> indexOnlyFields;

    /**
     * Default constructor
     *
     * @param indexedFields
     *            the set of indexed query fields
     * @param indexOnlyFields
     *            the set of index only query fields
     */
    public AnchorDetectionVisitor(Set<String> indexedFields, Set<String> indexOnlyFields) {
        this.indexedFields = indexedFields;
        this.indexOnlyFields = indexOnlyFields;
    }

    public boolean isAnchor(JexlNode node) {
        return (boolean) node.jjtAccept(this, null);
    }

    // pass through nodes

    @Override
    public Object visit(ASTJexlScript node, Object data) {
        return node.jjtGetChild(0).jjtAccept(this, data);
    }

    @Override
    public Object visit(ASTReference node, Object data) {
        return node.jjtGetChild(0).jjtAccept(this, data);
    }

    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        return node.jjtGetChild(0).jjtAccept(this, data);
    }

    @Override
    public Object visit(ASTAssignment node, Object data) {
        return false;
    }

    @Override
    public Object visit(ASTNotNode node, Object data) {
        return false;
    }

    // junction nodes

    /**
     * An OrNode is considered an anchor if and only if all children are anchor nodes
     *
     * @param node
     *            a JexlNode
     * @param data
     *            an Object
     * @return True if this node is an anchor
     */
    @Override
    public Object visit(ASTOrNode node, Object data) {
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            boolean childIsAnchor = (boolean) node.jjtGetChild(i).jjtAccept(this, data);
            if (!childIsAnchor) {
                return false;
            }
        }
        return true;
    }

    /**
     * An AndNode is considered an anchor if at least one child node is an anchor
     *
     * @param node
     *            a JexlNode
     * @param data
     *            an Object
     * @return True if this node is an anchor
     */
    @Override
    public Object visit(ASTAndNode node, Object data) {
        QueryPropertyMarker.Instance instance = QueryPropertyMarker.findInstance(node);
        if (instance.isAnyType()) {
            return visitMarker(instance);
        }

        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            boolean isChildAnchor = (boolean) node.jjtGetChild(i).jjtAccept(this, data);
            if (isChildAnchor) {
                return true;
            }
        }
        return false;
    }

    // leaf nodes

    @Override
    public Object visit(ASTEQNode node, Object data) {
        return visitLeaf(node);
    }

    @Override
    public Object visit(ASTNENode node, Object data) {
        return visitLeaf(node);
    }

    @Override
    public Object visit(ASTLTNode node, Object data) {
        return visitLeaf(node);
    }

    @Override
    public Object visit(ASTGTNode node, Object data) {
        return visitLeaf(node);
    }

    @Override
    public Object visit(ASTLENode node, Object data) {
        return visitLeaf(node);
    }

    @Override
    public Object visit(ASTGENode node, Object data) {
        return visitLeaf(node);
    }

    @Override
    public Object visit(ASTERNode node, Object data) {
        return visitLeaf(node);
    }

    @Override
    public Object visit(ASTNRNode node, Object data) {
        return visitLeaf(node);
    }

    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        return false;
    }

    private boolean visitLeaf(JexlNode node) {
        String field = JexlASTHelper.getIdentifier(node, true);
        if (indexedFields.contains(field) || indexOnlyFields.contains(field)) {
            if (node instanceof ASTEQNode || node instanceof ASTNENode) {
                Object value = JexlASTHelper.getLiteralValue(node);
                return value != null;
            }
            return true;
        }
        return false;
    }

    private Object visitMarker(QueryPropertyMarker.Instance instance) {

        if (instance == null || instance.getType() == null) {
            return false;
        }

        // might need to handle double markers, such as delayed bounded ranges

        switch (instance.getType()) {
            case BOUNDED_RANGE:
            case EXCEEDED_OR:
            case EXCEEDED_TERM:
            case EXCEEDED_VALUE:
                return true;
            case DELAYED:
            case EVALUATION_ONLY:
            default:
                return false;
        }
    }

}
