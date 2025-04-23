package datawave.ingest.mapreduce.handler.edge.evaluation;

import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.JexlFeatures;
import org.apache.commons.jexl3.JexlInfo;
import org.apache.commons.jexl3.JexlOptions;
import org.apache.commons.jexl3.internal.Engine;
import org.apache.commons.jexl3.internal.Frame;
import org.apache.commons.jexl3.internal.Interpreter;
import org.apache.commons.jexl3.internal.Script;

public class EdgeJexlEngine extends Engine {

    public EdgeJexlEngine(JexlBuilder conf) {
        super(conf);
    }

    @Override
    protected Interpreter createInterpreter(JexlContext context, Frame frame, JexlOptions opts) {
        return new EdgeJexlInterpreter(this, opts, context, frame);
    }

    /**
     * Inject our own JexlInfo and JexlFeatures
     *
     * @param features
     *            A set of features that will be enforced during parsing
     * @param info
     *            An info structure to carry debugging information if needed
     * @param scriptText
     *            A string containing valid JEXL syntax
     * @param names
     *            The script parameter names used during parsing; a corresponding array of arguments containing values should be used during evaluation
     * @return a Script
     */
    @Override
    public Script createScript(JexlFeatures features, JexlInfo info, String scriptText, String... names) {
        if (features == null) {
            features = EdgeJexlHelper.jexlFeatures();
        }
        if (info == null) {
            info = EdgeJexlHelper.jexlInfo();
        }
        return super.createScript(features, info, scriptText, names);
    }

}
