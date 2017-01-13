package nsa.datawave.query.jexl;

import java.util.HashMap;
import java.util.Map;

import nsa.datawave.query.functions.ContentFunctions;
import nsa.datawave.query.functions.EvaluationPhaseFilterFunctions;
import nsa.datawave.query.functions.GeoFunctions;
import nsa.datawave.query.functions.NormalizationFunctions;
import nsa.datawave.query.functions.QueryFunctions;

import org.apache.commons.jexl2.Interpreter;
import org.apache.commons.jexl2.JexlArithmetic;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.introspection.Uberspect;
import org.apache.commons.logging.Log;

/**
 * Extension of the JexlEngine that uses the DatawaveInterpreter.
 *
 */
@Deprecated
public class DatawaveJexlEngine extends JexlEngine {
    
    public DatawaveJexlEngine() {
        super();
        registerFunctions();
    }
    
    public DatawaveJexlEngine(Uberspect anUberspect, JexlArithmetic anArithmetic, Map<String,Object> theFunctions, Log log) {
        super(anUberspect, anArithmetic, theFunctions, log);
        registerFunctions();
    }
    
    @Override
    protected Interpreter createInterpreter(JexlContext context) {
        if (context == null) {
            context = EMPTY_CONTEXT;
        }
        return new DatawaveInterpreter(this, context);
    }
    
    private void registerFunctions() {
        Map<String,Object> funcs = new HashMap<>();
        funcs.put("f", QueryFunctions.class);
        funcs.put("geo", GeoFunctions.class);
        funcs.put("content", ContentFunctions.class);
        funcs.put("normalize", NormalizationFunctions.class);
        funcs.put("filter", EvaluationPhaseFilterFunctions.class);
        this.setFunctions(funcs);
    }
}
