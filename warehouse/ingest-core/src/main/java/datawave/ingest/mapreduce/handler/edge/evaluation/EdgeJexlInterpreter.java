package datawave.ingest.mapreduce.handler.edge.evaluation;

import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.JexlException;
import org.apache.commons.jexl3.JexlOptions;
import org.apache.commons.jexl3.internal.Frame;
import org.apache.commons.jexl3.internal.Interpreter;
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

}
