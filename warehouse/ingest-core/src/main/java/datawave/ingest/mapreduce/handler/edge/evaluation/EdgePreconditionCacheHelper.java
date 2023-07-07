package datawave.ingest.mapreduce.handler.edge.evaluation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.Script;

import datawave.ingest.mapreduce.handler.edge.define.EdgeDefinition;
import datawave.ingest.mapreduce.handler.edge.define.EdgeDefinitionConfigurationHelper;

/**
 * Creates a cache of compiled Jexl scripts
 */
public class EdgePreconditionCacheHelper {

    private JexlEngine engine;

    public EdgePreconditionCacheHelper(EdgePreconditionArithmetic arithmetic) {
        createEngine(arithmetic);
    }

    private void createEngine(EdgePreconditionArithmetic arithmetic) {
        this.setEngine(new EdgeJexlEngine(null, arithmetic, null, null));
        this.getEngine().setDebug(false); // Turn off debugging to make things go faster
        this.getEngine().setCache(50); // Set cache size lower than default value of 512
    }

    public Map<String,Script> createScriptCacheFromEdges(Map<String,EdgeDefinitionConfigurationHelper> edges) {

        Map<String,Script> scriptCache = new HashMap<>();

        for (String dataTypeKey : edges.keySet()) {
            List<EdgeDefinition> edgeList = edges.get(dataTypeKey).getEdges();
            for (EdgeDefinition edge : edgeList) {
                if (edge.hasJexlPrecondition()) {
                    scriptCache.put(edge.getJexlPrecondition(), createScriptFromString(edge.getJexlPrecondition()));
                }
            }
        }

        return scriptCache;
    }

    public Script createScriptFromString(String jexlPrecondition) {
        return engine.createScript(jexlPrecondition);
    }

    public JexlEngine getEngine() {
        return engine;
    }

    public void setEngine(JexlEngine engine) {
        this.engine = engine;
    }
}
