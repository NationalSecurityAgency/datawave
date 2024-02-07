package datawave.ingest.mapreduce.handler.edge;

import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.commons.jexl2.Script;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.util.Assert;

import datawave.data.normalizer.DateNormalizer;
import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.ConfigurationHelper;
import datawave.ingest.mapreduce.handler.edge.define.EdgeDefinition;
import datawave.ingest.mapreduce.handler.edge.define.EdgeDefinitionConfigurationHelper;
import datawave.ingest.mapreduce.handler.edge.evaluation.EdgePreconditionArithmetic;
import datawave.ingest.mapreduce.handler.edge.evaluation.EdgePreconditionCacheHelper;
import datawave.ingest.mapreduce.handler.edge.evaluation.EdgePreconditionJexlContext;
import datawave.ingest.mapreduce.handler.edge.evaluation.EdgePreconditionJexlEvaluation;

public class EdgeIngestConfiguration {

    public static final String EDGE_TABLE_DISALLOWLIST_VALUES = ".protobufedge.table.disallowlist.values";
    public static final String EDGE_TABLE_DISALLOWLIST_FIELDS = ".protobufedge.table.disallowlist.fields";
    public static final String EDGE_TABLE_METADATA_ENABLE = "protobufedge.table.metadata.enable";
    public static final String EDGE_TABLE_DISALLOWLIST_ENABLE = "protobufedge.table.disallowlist.enable";
    public static final String EDGE_SPRING_CONFIG = "protobufedge.spring.config";
    public static final String EDGE_SPRING_RELATIONSHIPS = "protobufedge.table.relationships";
    public static final String EDGE_SPRING_COLLECTIONS = "protobufedge.table.collections";
    public static final String ACTIVITY_DATE_FUTURE_DELTA = "protobufedge.valid.activitytime.future.delta";
    public static final String ACTIVITY_DATE_PAST_DELTA = "protobufedge.valid.activitytime.past.delta";
    public static final String EVALUATE_PRECONDITIONS = "protobufedge.evaluate.preconditions";
    public static final String INCLUDE_ALL_EDGES = "protobufedge.include.all.edges";
    public static final String EDGE_DEFAULT_DATA_TYPE = "default";
    public static final String TRIM_FIELD_GROUP = ".trim.field.group";

    private boolean enableDisallowList;
    private boolean evaluatePreconditions;
    private boolean includeAllEdges;

    private String springConfigFile;

    private Long futureDelta, pastDelta;

    private Map<String,Map<String,String>> edgeEnrichmentTypeLookup = new HashMap<>();
    private Map<String,Set<String>> disallowlistFieldLookup = new HashMap<>();
    private Map<String,Set<String>> disallowlistValueLookup = new HashMap<>();
    private Map<String,Script> scriptCache;

    private EdgePreconditionJexlContext edgePreconditionContext;
    private EdgePreconditionJexlEvaluation edgePreconditionEvaluation;
    private EdgePreconditionCacheHelper edgePreconditionCacheHelper;
    private EdgePreconditionArithmetic arithmetic = new EdgePreconditionArithmetic();

    protected HashSet<String> edgeRelationships = new HashSet<>();
    protected HashSet<String> collectionType = new HashSet<>();

    protected EdgeKeyVersioningCache versioningCache = null;

    protected Map<String,EdgeDefinitionConfigurationHelper> edges = null;

    private static final Logger log = LoggerFactory.getLogger(EdgeIngestConfiguration.class);

    public EdgeIngestConfiguration(Configuration conf) {

        enableDisallowList = ConfigurationHelper.isNull(conf, EDGE_TABLE_DISALLOWLIST_ENABLE, Boolean.class);

        springConfigFile = ConfigurationHelper.isNull(conf, EDGE_SPRING_CONFIG, String.class);
        futureDelta = ConfigurationHelper.isNull(conf, ACTIVITY_DATE_FUTURE_DELTA, Long.class);
        pastDelta = ConfigurationHelper.isNull(conf, ACTIVITY_DATE_PAST_DELTA, Long.class);

        evaluatePreconditions = Boolean.parseBoolean(conf.get(EVALUATE_PRECONDITIONS));
        includeAllEdges = Boolean.parseBoolean(conf.get(INCLUDE_ALL_EDGES));

        setupVersionCache(conf);
        // This will fail if the TypeRegistry has not been initialized in the VM.
        TypeRegistry registry = TypeRegistry.getInstance(conf);
        readEdgeConfigFile(registry, springConfigFile);
        pruneEdges();

    }

    /**
     * Parse and Store the Edge defs by data type
     */
    public void readEdgeConfigFile(TypeRegistry registry, String springConfigFile) {

        ClassPathXmlApplicationContext ctx;

        edges = new HashMap<>();

        try {
            ctx = new ClassPathXmlApplicationContext(ProtobufEdgeDataTypeHandler.class.getClassLoader().getResource(springConfigFile).toString());

            log.info("Got config on first try!");
        } catch (Exception e) {
            log.error("Problem getting config for ProtobufEdgeDataTypeHandler: {}", e);
            throw e;
        }

        Assert.notNull(ctx);

        registry.put(EDGE_DEFAULT_DATA_TYPE, null);

        if (ctx.containsBean(EDGE_SPRING_RELATIONSHIPS) && ctx.containsBean(EDGE_SPRING_COLLECTIONS)) {
            edgeRelationships.addAll((HashSet<String>) ctx.getBean(EDGE_SPRING_RELATIONSHIPS));
            collectionType.addAll((HashSet<String>) ctx.getBean(EDGE_SPRING_COLLECTIONS));
        } else {
            log.error("Edge relationships and or collection types are not configured correctly. Cannot build edge definitions");
            throw new RuntimeException("Missing some spring configurations");
        }

        for (Map.Entry<String,Type> entry : registry.entrySet()) {
            if (ctx.containsBean(entry.getKey())) {
                EdgeDefinitionConfigurationHelper edgeConfHelper = (EdgeDefinitionConfigurationHelper) ctx.getBean(entry.getKey());

                // Always call init first before getting getting edge defs. This performs validation on the config file
                // and builds the edge pairs/groups
                edgeConfHelper.init(edgeRelationships, collectionType);

                edges.put(entry.getKey(), edgeConfHelper);
                if (edgeConfHelper.getEnrichmentTypeMappings() != null) {
                    edgeEnrichmentTypeLookup.put(entry.getKey(), edgeConfHelper.getEnrichmentTypeMappings());
                }
            }

            if (ctx.containsBean(entry.getKey() + EDGE_TABLE_DISALLOWLIST_VALUES)) {
                Set<String> values = (HashSet<String>) ctx.getBean(entry.getKey() + EDGE_TABLE_DISALLOWLIST_VALUES);
                disallowlistValueLookup.put(entry.getKey(), new HashSet<>(values));
            }

            if (ctx.containsBean(entry.getKey() + EDGE_TABLE_DISALLOWLIST_FIELDS)) {
                Set<String> fields = (HashSet<String>) ctx.getBean(entry.getKey() + EDGE_TABLE_DISALLOWLIST_FIELDS);
                disallowlistFieldLookup.put(entry.getKey(), new HashSet<>(fields));
            }

        }
        ctx.close();

        registry.remove(EDGE_DEFAULT_DATA_TYPE);
    }

    public void pruneEdges() {

        setUpPreconditions();

        removeDisallowListedEdges();

        log.info("Found edge definitions for " + edges.keySet().size() + " data types.");

        StringBuffer sb = new StringBuffer();
        sb.append("Data Types With Defined Edges: ");
        for (String t : edges.keySet()) {
            sb.append(t).append(" ");
        }
        log.info(sb.toString());

    }

    public void setupVersionCache(Configuration conf) {
        if (this.versioningCache == null) {
            this.versioningCache = new EdgeKeyVersioningCache(conf);
        }
    }

    public long getStartDateIfValid() {
        long newFormatStartDate;
        try {
            // Only one known edge key version so we simply grab the first one here

            // expected to be in the "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" format
            String startDate = versioningCache.getEdgeKeyVersionDateChange().entrySet().iterator().next().getValue();
            newFormatStartDate = DateNormalizer.parseDate(startDate, DateNormalizer.FORMAT_STRINGS).getTime();
            log.info("Edge key version change date set to: " + startDate);
        } catch (IOException e) {
            log.error("IO Exception could not get edge key version cache, will not generate edges!");
            throw new RuntimeException("IO Exception could not get edge key version cache " + e.getMessage());
        } catch (ParseException e) {
            log.error("Unable to parse the switchover date will not generate edges!");
            throw new RuntimeException("Protobufedge handler config not set correctly " + e.getMessage());

        } catch (NoSuchElementException e) {
            log.error("edge key version cache existed but was empty, will not generate edges");
            throw new RuntimeException("Edge key versioning cache is empty " + e.getMessage());
        }
        return newFormatStartDate;
    }

    private void removeDisallowListedEdges() {
        // loop through edge definitions and collect any ones that have disallowlisted fields
        if (this.enableDisallowList) {
            Map<String,Set<EdgeDefinition>> disallowlistedEdges = new HashMap<>();
            for (String dType : edges.keySet()) {
                if (!disallowlistedEdges.containsKey(dType)) {
                    disallowlistedEdges.put(dType, new HashSet<>());
                }
                for (EdgeDefinition edgeDef : edges.get(dType).getEdges()) {
                    if (isDisallowListField(dType, edgeDef.getSourceFieldName()) || isDisallowListField(dType, edgeDef.getSinkFieldName())) {
                        disallowlistedEdges.get(dType).add(edgeDef);
                        log.warn("Removing Edge Definition due to disallowlisted Field: DataType: " + dType + " Definition: " + edgeDef.getSourceFieldName()
                                        + "-" + edgeDef.getSinkFieldName());
                    } else if (edgeDef.isEnrichmentEdge()) {
                        if (isDisallowListField(dType, edgeDef.getEnrichmentField())) {
                            disallowlistedEdges.get(dType).add(edgeDef);
                        }
                    }
                }
            }
            // remove the disallowlistedEdges
            int disallowlistedFieldCount = 0;
            for (String dType : disallowlistedEdges.keySet()) {
                for (EdgeDefinition edgeDef : disallowlistedEdges.get(dType)) {
                    edges.get(dType).getEdges().remove(edgeDef);
                    disallowlistedFieldCount++;
                }
            }
            if (disallowlistedFieldCount > 0) {
                log.info("Removed " + disallowlistedFieldCount + " edge definitions because they contain disallowlisted fields.");
            }
        } else {
            log.info("DisallowListing of edges is disabled.");
        }
    }

    /*
     * The evaluate preconditions boolean determines whether or not we want to set up the Jexl Contexts to run preconditions the includeAllEdges boolean
     * determines whether we want the extra edge definitions from the preconditions included There are three scenarios: the first is you want to run and
     * evaluate preconditions. The evaluatePreconditions boolean will be set to true, the edges will be added, the jext contexts will be set up, and the
     * conditional edges will be evaluated.
     *
     * Second, you don't want to evaluate the conditional edges, but you want them included. The evaluatePreconditions boolean will be set to false so the jexl
     * contexts are not set up, but the includeAllEdges boolean will be set to true.
     *
     * Third, you want neither of these done. Both booleans are set to false. The jexl context isn't set up and the conditional edges will be removed as to not
     * waste time evaluating edges where the conditions won't be met
     */
    public void setUpPreconditions() {
        // Set up the EdgePreconditionJexlContext, if enabled
        if (evaluatePreconditions) {
            edgePreconditionContext = new EdgePreconditionJexlContext(edges);
            edgePreconditionEvaluation = new EdgePreconditionJexlEvaluation();
            edgePreconditionCacheHelper = new EdgePreconditionCacheHelper(arithmetic);
            scriptCache = edgePreconditionCacheHelper.createScriptCacheFromEdges(edges);
        } else if (!includeAllEdges) {
            // Else remove edges with a precondition. No conditional edge defs will be evaluated possibly resulting in fewer edges
            log.info("Removing conditional edge definitions, possibly resulting in fewer edges being created");
            removeEdgesWithPreconditions();
        }
    }

    /**
     * When EVALUATE_PRECONDITIONS is false, remove edges with preconditions from consideration.
     */
    private void removeEdgesWithPreconditions() {
        Map<String,Set<EdgeDefinition>> preconditionEdges = new HashMap<>();
        for (String dType : edges.keySet()) {
            if (!preconditionEdges.containsKey(dType)) {
                preconditionEdges.put(dType, new HashSet<>());
            }
            for (EdgeDefinition edgeDef : edges.get(dType).getEdges()) {
                if (edgeDef.hasJexlPrecondition()) {
                    preconditionEdges.get(dType).add(edgeDef);
                }
            }
        }

        // remove the edges with preconditions
        int removedCount = 0;
        for (String dType : preconditionEdges.keySet()) {
            for (EdgeDefinition edgeDef : preconditionEdges.get(dType)) {
                edges.get(dType).getEdges().remove(edgeDef);
                removedCount++;
            }
        }
        if (log.isTraceEnabled()) {
            log.trace("Removed " + removedCount + " edges with preconditions prior to event processing.");
        }

    }

    public Map<String,Set<String>> getDisallowListFieldLookup() {
        return disallowlistFieldLookup;
    }

    public Map<String,Set<String>> getDisallowListValueLookup() {
        return disallowlistValueLookup;
    }

    private boolean isDisallowListField(String dataType, String fieldName) {
        if (disallowlistFieldLookup.containsKey(dataType)) {
            return this.disallowlistFieldLookup.get(dataType).contains(fieldName);
        } else if (disallowlistFieldLookup.containsKey(EDGE_DEFAULT_DATA_TYPE)) {
            // perhaps there is no disallowlist, which is fine
            return this.disallowlistFieldLookup.get(EDGE_DEFAULT_DATA_TYPE).contains(fieldName);
        }
        return false;
    }

    public boolean isDisallowListValue(String dataType, String fieldValue) {
        if (disallowlistValueLookup.containsKey(dataType)) {
            return this.disallowlistValueLookup.get(dataType).contains(fieldValue);
        } else if (disallowlistValueLookup.containsKey(EDGE_DEFAULT_DATA_TYPE)) {
            // perhaps there is no disallowlist, which is fine
            return this.disallowlistValueLookup.get(EDGE_DEFAULT_DATA_TYPE).contains(fieldValue);
        }
        return false;
    }

    public Map<String,EdgeDefinitionConfigurationHelper> getEdges() {
        return edges;
    }

    public void setEdges(Map<String,EdgeDefinitionConfigurationHelper> edges) {
        this.edges = edges;
    }

    public boolean evaluatePreconditions() {
        return evaluatePreconditions;
    }

    public EdgePreconditionJexlContext getPreconditionContext() {
        return edgePreconditionContext;
    }

    public EdgePreconditionJexlEvaluation getEdgePreconditionEvaluation() {
        return edgePreconditionEvaluation;
    }

    public Map<String,Script> getScriptCache() {
        return scriptCache;
    }

    public boolean enableDisallowList() {
        return enableDisallowList;
    }

    public Map<String,Map<String,String>> getEdgeEnrichmentTypeLookup() {
        return edgeEnrichmentTypeLookup;
    }

    public void clearArithmeticMatchingGroups() {
        arithmetic.clearMatchingGroups();
    }

    public Map<String,Set<String>> getArithmeticMatchingGroups() {
        return arithmetic.getMatchingGroups();
    }

    public long getPastDelta() {
        return pastDelta;
    }

    public long getFutureDelta() {
        return futureDelta;
    }
}
