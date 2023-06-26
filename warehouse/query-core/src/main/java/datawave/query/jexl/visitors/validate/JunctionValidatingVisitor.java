package datawave.query.jexl.visitors.validate;

import datawave.query.jexl.visitors.BaseVisitor;
import org.apache.commons.jexl3.parser.ASTAddNode;
import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTArrayAccess;
import org.apache.commons.jexl3.parser.ASTArrayLiteral;
import org.apache.commons.jexl3.parser.ASTAssignment;
import org.apache.commons.jexl3.parser.ASTBitwiseAndNode;
import org.apache.commons.jexl3.parser.ASTBitwiseComplNode;
import org.apache.commons.jexl3.parser.ASTBitwiseOrNode;
import org.apache.commons.jexl3.parser.ASTBitwiseXorNode;
import org.apache.commons.jexl3.parser.ASTBlock;
import org.apache.commons.jexl3.parser.ASTConstructorNode;
import org.apache.commons.jexl3.parser.ASTDivNode;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTEmptyFunction;
import org.apache.commons.jexl3.parser.ASTFalseNode;
import org.apache.commons.jexl3.parser.ASTForeachStatement;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTGENode;
import org.apache.commons.jexl3.parser.ASTGTNode;
import org.apache.commons.jexl3.parser.ASTIdentifier;
import org.apache.commons.jexl3.parser.ASTIfStatement;
import org.apache.commons.jexl3.parser.ASTLENode;
import org.apache.commons.jexl3.parser.ASTLTNode;
import org.apache.commons.jexl3.parser.ASTMapEntry;
import org.apache.commons.jexl3.parser.ASTMapLiteral;
import org.apache.commons.jexl3.parser.ASTMethodNode;
import org.apache.commons.jexl3.parser.ASTModNode;
import org.apache.commons.jexl3.parser.ASTMulNode;
import org.apache.commons.jexl3.parser.ASTNENode;
import org.apache.commons.jexl3.parser.ASTNRNode;
import org.apache.commons.jexl3.parser.ASTNotNode;
import org.apache.commons.jexl3.parser.ASTNullLiteral;
import org.apache.commons.jexl3.parser.ASTNumberLiteral;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.ASTReference;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.ASTReturnStatement;
import org.apache.commons.jexl3.parser.ASTSizeFunction;
import org.apache.commons.jexl3.parser.ASTStringLiteral;
import org.apache.commons.jexl3.parser.ASTSubNode;
import org.apache.commons.jexl3.parser.ASTTernaryNode;
import org.apache.commons.jexl3.parser.ASTTrueNode;
import org.apache.commons.jexl3.parser.ASTUnaryMinusNode;
import org.apache.commons.jexl3.parser.ASTVar;
import org.apache.commons.jexl3.parser.ASTWhileStatement;
import org.apache.commons.jexl3.parser.JexlNode;

/**
 * Validates that every ASTAndNode and every ASTOrNode has more than one child
 */
public class JunctionValidatingVisitor extends BaseVisitor {

    private boolean isValid = true;

    public static boolean validate(JexlNode script) {
        JunctionValidatingVisitor visitor = new JunctionValidatingVisitor();
        script.jjtAccept(visitor, null);
        return visitor.isValid();
    }

    private JunctionValidatingVisitor() {}

    public boolean isValid() {
        return this.isValid;
    }

    /*
     * Pass through visit to ASTJexlScript
     *
     * Attempt to short circuit on ASTReference, ASTReferenceExpressions, SimpleNode, ASTFunctionNode, and ASTNotNode
     */

    @Override
    public Object visit(ASTReference node, Object data) {
        if (isValid) {
            node.childrenAccept(this, data);
        }
        return data;
    }

    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        if (isValid) {
            node.childrenAccept(this, data);
        }
        return data;
    }

    // check the argument nodes of a function
    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        if (isValid) {
            node.childrenAccept(this, data);
        }
        return data;
    }

    // descend into negated branches of the query
    @Override
    public Object visit(ASTNotNode node, Object data) {
        if (isValid) {
            node.childrenAccept(this, data);
        }
        return data;
    }

    /*
     * Validation methods
     */

    @Override
    public Object visit(ASTOrNode node, Object data) {
        return visitJunction(node, data);
    }

    @Override
    public Object visit(ASTAndNode node, Object data) {
        return visitJunction(node, data);
    }

    private Object visitJunction(JexlNode node, Object data) {
        if (!isValid) {
            return data;
        } else if (!isJunctionValid(node)) {
            isValid = false;
            return data;
        } else {
            node.childrenAccept(this, data);
            return data;
        }
    }

    /**
     * A junction is valid if it has two or more children
     *
     * @param node
     *            an ASTOrNode or ASTAndNode
     * @return true if the junction is valid
     */
    private boolean isJunctionValid(JexlNode node) {
        return node.jjtGetNumChildren() >= 2;
    }

    /*
     * Short circuits
     */

    @Override
    public Object visit(ASTBlock node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTIfStatement node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTWhileStatement node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTForeachStatement node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTAssignment node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTTernaryNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTBitwiseOrNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTBitwiseXorNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTBitwiseAndNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTEQNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTNENode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTLTNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTGTNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTLENode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTGENode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTERNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTNRNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTAddNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTSubNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTMulNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTDivNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTModNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTUnaryMinusNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTBitwiseComplNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTIdentifier node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTNullLiteral node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTTrueNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTFalseNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTStringLiteral node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTArrayLiteral node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTMapLiteral node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTMapEntry node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTEmptyFunction node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTSizeFunction node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTMethodNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTConstructorNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTArrayAccess node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTReturnStatement node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTVar node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTNumberLiteral node, Object data) {
        return data;
    }

}
