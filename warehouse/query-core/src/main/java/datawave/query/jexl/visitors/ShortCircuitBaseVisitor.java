package datawave.query.jexl.visitors;

import org.apache.commons.jexl2.parser.ASTAdditiveNode;
import org.apache.commons.jexl2.parser.ASTAdditiveOperator;
import org.apache.commons.jexl2.parser.ASTAmbiguous;
import org.apache.commons.jexl2.parser.ASTArrayAccess;
import org.apache.commons.jexl2.parser.ASTArrayLiteral;
import org.apache.commons.jexl2.parser.ASTBitwiseAndNode;
import org.apache.commons.jexl2.parser.ASTBitwiseComplNode;
import org.apache.commons.jexl2.parser.ASTBitwiseOrNode;
import org.apache.commons.jexl2.parser.ASTBitwiseXorNode;
import org.apache.commons.jexl2.parser.ASTBlock;
import org.apache.commons.jexl2.parser.ASTConstructorNode;
import org.apache.commons.jexl2.parser.ASTDivNode;
import org.apache.commons.jexl2.parser.ASTEmptyFunction;
import org.apache.commons.jexl2.parser.ASTFalseNode;
import org.apache.commons.jexl2.parser.ASTFloatLiteral;
import org.apache.commons.jexl2.parser.ASTForeachStatement;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTIfStatement;
import org.apache.commons.jexl2.parser.ASTIntegerLiteral;
import org.apache.commons.jexl2.parser.ASTMapEntry;
import org.apache.commons.jexl2.parser.ASTMapLiteral;
import org.apache.commons.jexl2.parser.ASTMethodNode;
import org.apache.commons.jexl2.parser.ASTModNode;
import org.apache.commons.jexl2.parser.ASTMulNode;
import org.apache.commons.jexl2.parser.ASTNullLiteral;
import org.apache.commons.jexl2.parser.ASTNumberLiteral;
import org.apache.commons.jexl2.parser.ASTReturnStatement;
import org.apache.commons.jexl2.parser.ASTSizeFunction;
import org.apache.commons.jexl2.parser.ASTSizeMethod;
import org.apache.commons.jexl2.parser.ASTStringLiteral;
import org.apache.commons.jexl2.parser.ASTTernaryNode;
import org.apache.commons.jexl2.parser.ASTTrueNode;
import org.apache.commons.jexl2.parser.ASTUnaryMinusNode;
import org.apache.commons.jexl2.parser.ASTWhileStatement;
import org.apache.commons.jexl2.parser.SimpleNode;

/**
 * <p>
 * This class seeks to cut down on query planning time. While there are some nodes that we always want to traverse (AND, OR, NOT, REF, REF_EXPR - which this
 * class never overrides), we can short circuit recursion at most, if not all, leaf nodes (EQ, NE, ER, NR, GT, LT, GE, LE - this class assumes any classes that
 * extend it will implement their own short-circuiting of leaf nodes). Limiting certain visitor recursion has been shown to cut visit time on the query tree by
 * over half.
 * </p>
 * <p>
 * It should be noted that a visitor that needs to visit queries like the ones found in FunctionalSetTest.testFunctionsAsArguments() will not visit the whole
 * tree. In most cases that shouldn't be an issue, but is worth noting if a particularly complicated or fancy query is required.
 * </p>
 */
public class ShortCircuitBaseVisitor extends BaseVisitor {

    @Override
    public Object visit(SimpleNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTBlock node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTAmbiguous node, Object data) {
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
    public Object visit(ASTAdditiveNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTAdditiveOperator node, Object data) {
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
    @SuppressWarnings("deprecation")
    public Object visit(ASTIntegerLiteral node, Object data) {
        return data;
    }

    @Override
    @SuppressWarnings("deprecation")
    public Object visit(ASTFloatLiteral node, Object data) {
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
    public Object visit(ASTSizeMethod node, Object data) {
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
    public Object visit(ASTNumberLiteral node, Object data) {
        return data;
    }

}
