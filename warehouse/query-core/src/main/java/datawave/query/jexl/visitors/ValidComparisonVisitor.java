package datawave.query.jexl.visitors;

import org.apache.commons.jexl3.parser.ASTAddNode;
import org.apache.commons.jexl3.parser.ASTAssignment;
import org.apache.commons.jexl3.parser.ASTDivNode;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTFalseNode;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTGENode;
import org.apache.commons.jexl3.parser.ASTGTNode;
import org.apache.commons.jexl3.parser.ASTLENode;
import org.apache.commons.jexl3.parser.ASTLTNode;
import org.apache.commons.jexl3.parser.ASTMethodNode;
import org.apache.commons.jexl3.parser.ASTModNode;
import org.apache.commons.jexl3.parser.ASTMulNode;
import org.apache.commons.jexl3.parser.ASTNENode;
import org.apache.commons.jexl3.parser.ASTNRNode;
import org.apache.commons.jexl3.parser.ASTNullLiteral;
import org.apache.commons.jexl3.parser.ASTNumberLiteral;
import org.apache.commons.jexl3.parser.ASTStringLiteral;
import org.apache.commons.jexl3.parser.ASTSubNode;
import org.apache.commons.jexl3.parser.ASTTrueNode;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.lang3.mutable.MutableInt;

import datawave.query.exceptions.DatawaveFatalQueryException;

/**
 * Validates all expressions in a query tree (e.g. literal == literal is considered invalid)
 */
public class ValidComparisonVisitor extends BaseVisitor {

    public static void check(JexlNode node) {
        ValidComparisonVisitor visitor = new ValidComparisonVisitor();
        node.jjtAccept(visitor, null);
    }

    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        // not concerned with literals for function nodes
        return data;
    }

    @Override
    public Object visit(ASTMethodNode node, Object data) {
        // not concerned with literals for method nodes
        return data;
    }

    @Override
    public Object visit(ASTAddNode node, Object data) {
        // not concerned with literals for additive nodes
        return data;
    }

    @Override
    public Object visit(ASTSubNode node, Object data) {
        // not concerned with literals for additive nodes
        return data;
    }

    @Override
    public Object visit(ASTMulNode node, Object data) {
        // not concerned with literals for mul nodes
        return data;
    }

    @Override
    public Object visit(ASTDivNode node, Object data) {
        // not concerned with literals for div nodes
        return data;
    }

    @Override
    public Object visit(ASTModNode node, Object data) {
        // not concerned with literals for mod nodes
        return data;
    }

    @Override
    public Object visit(ASTEQNode node, Object data) {
        validateExpression(node, super.visit(node, new MutableInt()));
        return data;
    }

    @Override
    public Object visit(ASTNENode node, Object data) {
        validateExpression(node, super.visit(node, new MutableInt()));
        return data;
    }

    @Override
    public Object visit(ASTLTNode node, Object data) {
        validateExpression(node, super.visit(node, new MutableInt()));
        return data;
    }

    @Override
    public Object visit(ASTGTNode node, Object data) {
        validateExpression(node, super.visit(node, new MutableInt()));
        return data;
    }

    @Override
    public Object visit(ASTLENode node, Object data) {
        validateExpression(node, super.visit(node, new MutableInt()));
        return data;
    }

    @Override
    public Object visit(ASTGENode node, Object data) {
        validateExpression(node, super.visit(node, new MutableInt()));
        return data;
    }

    @Override
    public Object visit(ASTERNode node, Object data) {
        validateExpression(node, super.visit(node, new MutableInt()));
        return data;
    }

    @Override
    public Object visit(ASTNRNode node, Object data) {
        validateExpression(node, super.visit(node, new MutableInt()));
        return data;
    }

    @Override
    public Object visit(ASTAssignment node, Object data) {
        validateExpression(node, super.visit(node, new MutableInt()));
        return data;
    }

    /* literal visitors */
    @Override
    public Object visit(ASTNumberLiteral node, Object data) {
        return visitLiteral(data);
    }

    @Override
    public Object visit(ASTTrueNode node, Object data) {
        return visitLiteral(data);
    }

    @Override
    public Object visit(ASTFalseNode node, Object data) {
        return visitLiteral(data);
    }

    @Override
    public Object visit(ASTNullLiteral node, Object data) {
        return visitLiteral(data);
    }

    @Override
    public Object visit(ASTStringLiteral node, Object data) {
        return visitLiteral(data);
    }

    private Object visitLiteral(Object data) {
        if (data instanceof MutableInt) {
            ((MutableInt) data).increment();
        }
        return data;
    }

    private void validateExpression(JexlNode node, Object data) {
        if ((data instanceof MutableInt) && (((MutableInt) data).intValue() > 1)) {
            throw new DatawaveFatalQueryException("Cannot compare two literals.  Invalid expression: " + JexlStringBuildingVisitor.buildQuery(node));
        }
    }

}
