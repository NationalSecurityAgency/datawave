package datawave.query.jexl.visitors;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTNumberLiteral;
import org.apache.commons.jexl3.parser.ASTUnaryMinusNode;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;

import datawave.core.query.jexl.visitors.RebuildingVisitor;

public class FixNegativeNumbersVisitor extends RebuildingVisitor {

    public static ASTJexlScript fix(JexlNode root) {
        FixNegativeNumbersVisitor visitor = new FixNegativeNumbersVisitor();
        return (ASTJexlScript) root.jjtAccept(visitor, null);
    }

    @Override
    public Object visit(ASTUnaryMinusNode astumn, Object data) {
        if (astumn.jjtGetNumChildren() == 1 && astumn.jjtGetChild(0) instanceof ASTNumberLiteral) {
            ASTNumberLiteral node = (ASTNumberLiteral) astumn.jjtGetChild(0);
            ASTNumberLiteral newNode = JexlNodes.makeNumberLiteral();
            Number value = negate(node.getLiteral());
            if (value == null) {
                throw new IllegalArgumentException("Could not ascertain type of ASTNumberLiteral: " + node);
            }
            JexlNodes.setLiteral(newNode, value);
            newNode.jjtSetParent(node.jjtGetParent());
            return newNode;
        } else {
            return super.visit(astumn, data);
        }
    }

    private Number negate(Number number) {
        Number negated = null;
        if (number instanceof Byte) {
            negated = -number.byteValue();
        } else if (number instanceof Short) {
            negated = -number.shortValue();
        } else if (number instanceof Integer) {
            negated = -number.intValue();
        } else if (number instanceof Long) {
            negated = -number.longValue();
        } else if (number instanceof BigInteger) {
            negated = ((BigInteger) number).negate();
        } else if (number instanceof Float) {
            negated = -number.floatValue();
        } else if (number instanceof Double) {
            negated = -number.doubleValue();
        } else if (number instanceof BigDecimal) {
            negated = ((BigDecimal) number).negate();
        }
        return negated;
    }

}
