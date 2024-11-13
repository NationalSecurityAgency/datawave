package datawave.query.jexl;

import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.JexlOptions;
import org.apache.commons.jexl3.internal.Engine;
import org.apache.commons.jexl3.internal.Frame;
import org.apache.commons.jexl3.internal.Interpreter;
import org.apache.commons.jexl3.introspection.JexlPermissions;
import org.apache.commons.jexl3.parser.ASTJexlScript;

/**
 * Extension of the JexlEngine.
 *
 */
public class DatawaveJexlEngine extends Engine {
    public DatawaveJexlEngine() {
        super(new JexlBuilder().debug(false).namespaces(ArithmeticJexlEngines.functions()).permissions(JexlPermissions.UNRESTRICTED));
    }

    public DatawaveJexlEngine(JexlBuilder conf) {
        super(conf);
    }

    @Override
    protected Interpreter createInterpreter(JexlContext context, Frame frame, JexlOptions opts) {
        return new DatawaveInterpreter(this, opts, context, frame);
    }

    public ASTJexlScript parse(String expression) {
        return super.parse(null, true, expression, null);
    }
}
