package datawave.ingest.mapreduce.handler.edge.evaluation;

import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.JexlOptions;
import org.apache.commons.jexl3.internal.Engine;
import org.apache.commons.jexl3.internal.Frame;
import org.apache.commons.jexl3.internal.Interpreter;

public class EdgeJexlEngine extends Engine {
    public EdgeJexlEngine(JexlBuilder conf) {
        super(conf);
    }
    
    @Override
    protected Interpreter createInterpreter(JexlContext context, Frame frame, JexlOptions opts) {
        return new EdgeJexlInterpreter(this, opts, context, frame);
    }
}
