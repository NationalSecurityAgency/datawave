package nsa.datawave.query.jexl;

import org.apache.commons.jexl2.Interpreter;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.parser.ASTFloatLiteral;
import org.apache.commons.jexl2.parser.ASTIntegerLiteral;
import org.apache.commons.lang.math.NumberUtils;

@Deprecated
public class DatawaveInterpreter extends Interpreter {
    
    public DatawaveInterpreter(JexlEngine jexl, JexlContext aContext) {
        super(jexl, aContext);
    }
    
    @Override
    public Object visit(ASTFloatLiteral node, Object data) {
        // The JEXL grammar treats both Float and Double values
        // as a Float and this causes errors. Find out what this really is.
        Number n = null;
        if (null != node.jjtGetValue())
            n = NumberUtils.createNumber(node.jjtGetValue().toString());
        if (null == n) {
            n = NumberUtils.createNumber(node.image);
            node.jjtSetValue(n);
        }
        if (n instanceof Double)
            return n.doubleValue();
        else
            return n.floatValue();
    }
    
    @Override
    public Object visit(ASTIntegerLiteral node, Object data) {
        // The JEXL grammar treats both Integer and Long values
        // as a Integer and this causes errors. Find out what this really is.
        Number n = null;
        if (data != null) {
            n = NumberUtils.createNumber(node.image);
            return getAttribute(data, n, node);
        } else {
            if (null != node.jjtGetValue())
                n = NumberUtils.createNumber(node.jjtGetValue().toString());
            if (null == n) {
                n = NumberUtils.createNumber(node.image);
                node.jjtSetValue(n);
            }
            if (n instanceof Long)
                return n.longValue();
            else
                return n.intValue();
        }
    }
}
