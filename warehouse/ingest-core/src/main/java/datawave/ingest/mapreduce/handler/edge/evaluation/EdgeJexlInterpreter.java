package datawave.ingest.mapreduce.handler.edge.evaluation;

import org.apache.commons.jexl2.Interpreter;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlException;
import org.apache.commons.jexl2.parser.ASTOrNode;

public class EdgeJexlInterpreter extends Interpreter {
    
    public EdgeJexlInterpreter(EdgeJexlEngine edgeJexlEngine, JexlContext context, boolean strictFlag, boolean silentFlag) {
        super(edgeJexlEngine, context, strictFlag, silentFlag);
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
