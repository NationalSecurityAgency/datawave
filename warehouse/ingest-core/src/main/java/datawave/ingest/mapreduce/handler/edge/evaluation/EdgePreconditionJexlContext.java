package datawave.ingest.mapreduce.handler.edge.evaluation;

import com.google.common.collect.Multimap;
import datawave.attribute.EventFieldValueTuple;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.mapreduce.handler.edge.define.EdgeDefinition;
import datawave.ingest.mapreduce.handler.edge.define.EdgeDefinitionConfigurationHelper;
import datawave.ingest.mapreduce.handler.fact.functions.MultimapContext;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.Script;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class EdgePreconditionJexlContext extends MultimapContext {
    
    private static final Logger log = LoggerFactory.getLogger(EdgePreconditionJexlContext.class);
    
    private HashSet<String> filterFieldKeys;
    
    /**
     * This constructor creates a context based on a single list of edge definitions
     *
     */
    public EdgePreconditionJexlContext() {
        super();
    }
    
    /**
     * This constructor creates a context based on a single list of edge definitions
     * 
     * @param edges
     *            the edge definitions
     */
    public EdgePreconditionJexlContext(List<EdgeDefinition> edges) {
        super();
        filterFieldKeys = createFilterKeysFromEdgeDefinitions(edges);
    }
    
    /**
     * This constructor creates a context from a Map of edge definitions by datatype
     * 
     * @param edgesByDataType
     *            a map of edge definitions
     */
    public EdgePreconditionJexlContext(Map<String,EdgeDefinitionConfigurationHelper> edgesByDataType) {
        super();
        
        filterFieldKeys = new HashSet<>();
        for (String dataTypeKey : edgesByDataType.keySet()) {
            EdgeDefinitionConfigurationHelper helper = edgesByDataType.get(dataTypeKey);
            filterFieldKeys.addAll(createFilterKeysFromEdgeDefinitions(helper.getEdges()));
        }
    }
    
    private HashSet<String> createFilterKeysFromEdgeDefinitions(List<EdgeDefinition> edges) {
        long start = System.currentTimeMillis();
        JexlEngine engine = new JexlEngine();
        Script script;
        HashSet<String> filterFields = new HashSet<>();
        for (EdgeDefinition edgeDef : edges) {
            if (edgeDef.hasJexlPrecondition()) {
                
                script = engine.createScript(edgeDef.getJexlPrecondition());
                filterFields.addAll(extractTermsFromJexlScript(script));
            }
        }
        
        if (log.isTraceEnabled()) {
            log.trace("Time to create filtered keys from edge definitions: " + (System.currentTimeMillis() - start) + "ms.");
        }
        
        return filterFields;
    }
    
    private HashSet<String> extractTermsFromJexlScript(Script script) {
        HashSet<String> terms = new HashSet<>();
        Set<List<String>> scriptVariables = script.getVariables();
        
        for (List<String> termList : scriptVariables) {
            
            for (String term : termList) {
                
                // Add the cleaned term to our list
                terms.add(cleanJexlTerm(term));
            }
            
        }
        return terms;
    }
    
    // Small helper method, removes the leading $ escape char from the edge definition term so it may
    // be used as a key within the context.
    private String cleanJexlTerm(String term) {
        return term.replace("$", "");
    }
    
    /**
     * This method will load a filtered sub-set of all fields in an event. The filter defines the set of fields that could be evaluated by any Jexl
     * Precondition.
     *
     * This should only be called once per process() call, and clears the map of event values
     *
     * @param nciEvent
     *            -- A MultiMap of type NormalizedContentInterface representation of the event.
     */
    public void setFilteredContextForNormalizedContentInterface(Multimap<String,NormalizedContentInterface> nciEvent) {
        clearContext();
        
        for (String filterFieldKey : filterFieldKeys) {
            Collection<NormalizedContentInterface> nciCollection = nciEvent.get(filterFieldKey);
            
            if (null != nciCollection) {
                for (NormalizedContentInterface nci : nciCollection) {
                    EventFieldValueTuple tuple = new EventFieldValueTuple();
                    
                    if (log.isTraceEnabled()) {
                        log.trace("Adding: " + filterFieldKey + "." + nci.getEventFieldValue() + " to context.");
                    }
                    tuple.setFieldName(nci.getEventFieldName() == null ? nci.getIndexedFieldName() : nci.getEventFieldName());
                    tuple.setValue(nci.getEventFieldValue());
                    this.set(normalizeTerm(filterFieldKey), tuple);
                }
            }
        }
    }
    
    private String normalizeTerm(String filterFieldKey) {
        if (Character.isDigit(filterFieldKey.charAt(0))) {
            return "$" + filterFieldKey;
        }
        return filterFieldKey;
    }
    
    public Set<String> getFilterFieldKeys() {
        return filterFieldKeys;
    }
    
    public void clearContext() {
        this.values.clear();
    }
}
