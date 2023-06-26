package datawave.ingest.mapreduce.handler.edge.evaluation;

import datawave.ingest.mapreduce.handler.edge.define.EdgeDefinition;
import datawave.ingest.mapreduce.handler.edge.define.EdgeDefinitionConfigurationHelper;
import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlScript;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Creates a cache of compiled Jexl scripts
 */
public class EdgePreconditionCacheHelper {

    private JexlEngine engine;

    public EdgePreconditionCacheHelper(EdgePreconditionArithmetic arithmetic) {
        createEngine(arithmetic);
    }

    private void createEngine(EdgePreconditionArithmetic arithmetic) {
        // @formatter:off
        this.setEngine(new EdgeJexlEngine(new JexlBuilder()
                .arithmetic(arithmetic)
                .debug(false) // Turn off debugging to make things go faster
                .cache(50))); // Set cache size lower than default value of 512
        // @formatter:on
    }

    public Map<String,JexlScript> createScriptCacheFromEdges(Map<String,EdgeDefinitionConfigurationHelper> edges) {

        Map<String,JexlScript> scriptCache = new HashMap<>();

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

    public JexlScript createScriptFromString(String jexlPrecondition) {
        return engine.createScript(jexlPrecondition);
    }

    public JexlEngine getEngine() {
        return engine;
    }

    public void setEngine(JexlEngine engine) {
        this.engine = engine;
    }
}
