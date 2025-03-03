package datawave.ingest.mapreduce.handler.edge.evaluation;

import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.JexlException;
import org.apache.commons.jexl3.JexlOptions;
import org.apache.commons.jexl3.internal.Frame;
import org.apache.commons.jexl3.internal.Interpreter;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTNENode;
import org.apache.commons.jexl3.parser.ASTNRNode;
import org.apache.commons.jexl3.parser.ASTOrNode;

public class EdgeJexlInterpreter extends Interpreter {

    public EdgeJexlInterpreter(EdgeJexlEngine engine, JexlOptions opts, JexlContext context, Frame eFrame) {
        super(engine, opts, context, eFrame);
    }

    // we want to avoid short circuiting an OR so we generate all possible edges if they are group aware
    @Override
    public Object visit(ASTOrNode node, Object data) {
        Object left = node.jjtGetChild(0).jjtAccept(this, data);
        boolean matchesL = false;
        boolean matchesR = false;
        try {
            boolean leftValue = arithmetic.toBoolean(left);
            if (leftValue) {
                matchesL = true;
            }
        } catch (ArithmeticException xrt) {
            throw new JexlException(node.jjtGetChild(0), "boolean coercion error", xrt);
        }
        Object right = node.jjtGetChild(1).jjtAccept(this, data);
        try {
            boolean rightValue = arithmetic.toBoolean(right);
            if (rightValue) {
                matchesR = true;
            }
        } catch (ArithmeticException xrt) {
            throw new JexlException(node.jjtGetChild(1), "boolean coercion error", xrt);
        }
        return (matchesL || matchesR);
    }

    @Override
    protected Object visit(ASTNENode node, Object data) {
        Object left = node.jjtGetChild(0).jjtAccept(this, data);
        Object right = node.jjtGetChild(1).jjtAccept(this, data);

        try {
            if (this.arithmetic instanceof EdgePreconditionArithmetic) {
                Object result = ((EdgePreconditionArithmetic) this.arithmetic).notEquals(left, right);
                return result;
            } else {
                Object result = !this.arithmetic.equals(left, right);
                return result;
            }

        } catch (ArithmeticException xrt) {
            throw new JexlException(this.findNullOperand(node, left, right), "!= error", xrt);
        }
    }

    @Override
    protected Object visit(final ASTERNode node, final Object data) {
        final Object left = node.jjtGetChild(0).jjtAccept(this, data);
        final Object right = node.jjtGetChild(1).jjtAccept(this, data);

        return this.arithmetic.contains(left, right);
    }

    @Override
    protected Object visit(final ASTNRNode node, final Object data) {
        final Object left = node.jjtGetChild(0).jjtAccept(this, data);
        final Object right = node.jjtGetChild(1).jjtAccept(this, data);
        try {
            if (this.arithmetic instanceof EdgePreconditionArithmetic) {
                Object result = ((EdgePreconditionArithmetic) this.arithmetic).notContains(left, right);
                return result;
            } else {
                Object result = !this.arithmetic.contains(left, right);
                return result;
            }

        } catch (ArithmeticException xrt) {
            throw new JexlException(this.findNullOperand(node, left, right), "=~ error", xrt);
        }
    }

}
