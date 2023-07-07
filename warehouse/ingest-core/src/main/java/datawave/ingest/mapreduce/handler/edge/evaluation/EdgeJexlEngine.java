package datawave.ingest.mapreduce.handler.edge.evaluation;

import java.util.Map;

import org.apache.commons.jexl2.Interpreter;
import org.apache.commons.jexl2.JexlArithmetic;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.introspection.Uberspect;
import org.apache.commons.logging.Log;

public class EdgeJexlEngine extends JexlEngine {

    public EdgeJexlEngine(Uberspect anUberspect, JexlArithmetic anArithmetic, Map<String,Object> theFunctions, Log log) {
        super(anUberspect, anArithmetic, theFunctions, log);
    }

    @Override
    protected Interpreter createInterpreter(JexlContext context, boolean strictFlag, boolean silentFlag) {
        return new EdgeJexlInterpreter(this, context, strictFlag, silentFlag);
    }
}
