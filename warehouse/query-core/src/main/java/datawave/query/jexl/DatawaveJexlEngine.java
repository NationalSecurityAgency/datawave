package datawave.query.jexl;

import java.util.Map;

import org.apache.commons.jexl2.Interpreter;
import org.apache.commons.jexl2.JexlArithmetic;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.introspection.Uberspect;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.logging.Log;

/**
 * Extension of the JexlEngine.
 * 
 */
public class DatawaveJexlEngine extends JexlEngine {
    
    public DatawaveJexlEngine() {
        super();
        registerFunctions();
    }
    
    public DatawaveJexlEngine(Uberspect anUberspect, JexlArithmetic anArithmetic, Map<String,Object> theFunctions, Log log) {
        super(anUberspect, anArithmetic, theFunctions, log);
    }
    
    private void registerFunctions() {
        this.setFunctions(ArithmeticJexlEngines.functions());
    }
    
    @Override
    protected Interpreter createInterpreter(JexlContext context, boolean strictFlag, boolean silentFlag) {
        return new DatawaveInterpreter(this, context, strictFlag, silentFlag);
    }
    
    public ASTJexlScript parse(CharSequence expression) {
        return super.parse(expression, null, null);
    }
}
