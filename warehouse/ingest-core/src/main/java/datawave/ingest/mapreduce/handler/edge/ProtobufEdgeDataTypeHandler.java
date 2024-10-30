package datawave.ingest.mapreduce.handler.edge;

import static datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler.METADATA_TABLE_LOADER_PRIORITY;
import static datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler.METADATA_TABLE_NAME;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.jexl3.JexlScript;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.util.Assert;

import com.google.common.base.Charsets;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.hash.PrimitiveSink;

import datawave.data.normalizer.DateNormalizer;
import datawave.edge.util.EdgeKey;
import datawave.edge.util.EdgeKey.EDGE_FORMAT;
import datawave.edge.util.EdgeKey.STATS_TYPE;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.ConfigurationHelper;
import datawave.ingest.data.config.GroupedNormalizedContentInterface;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.ingest.IngestHelperInterface;
import datawave.ingest.data.normalizer.SimpleGroupFieldNameParser;
import datawave.ingest.mapreduce.EventMapper;
import datawave.ingest.mapreduce.handler.ExtendedDataTypeHandler;
import datawave.ingest.mapreduce.handler.edge.define.EdgeDataBundle;
import datawave.ingest.mapreduce.handler.edge.define.EdgeDefinition;
import datawave.ingest.mapreduce.handler.edge.define.EdgeDefinitionConfigurationHelper;
import datawave.ingest.mapreduce.handler.edge.define.EdgeDirection;
import datawave.ingest.mapreduce.handler.edge.define.VertexValue;
import datawave.ingest.mapreduce.handler.edge.define.VertexValue.ValueType;
import datawave.ingest.mapreduce.handler.edge.evaluation.EdgePreconditionArithmetic;
import datawave.ingest.mapreduce.handler.edge.evaluation.EdgePreconditionCacheHelper;
import datawave.ingest.mapreduce.handler.edge.evaluation.EdgePreconditionJexlContext;
import datawave.ingest.mapreduce.handler.edge.evaluation.EdgePreconditionJexlEvaluation;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.writer.ContextWriter;
import datawave.ingest.metadata.RawRecordMetadata;
import datawave.ingest.table.config.LoadDateTableConfigHelper;
import datawave.ingest.time.Now;
import datawave.marking.MarkingFunctions;
import datawave.metadata.protobuf.EdgeMetadata.MetadataValue;
import datawave.metadata.protobuf.EdgeMetadata.MetadataValue.Metadata;
import datawave.util.StringUtils;
import datawave.util.time.DateHelper;

public class ProtobufEdgeDataTypeHandler<KEYIN,KEYOUT,VALUEOUT> implements ExtendedDataTypeHandler<KEYIN,KEYOUT,VALUEOUT> {

    private static final Logger log = LoggerFactory.getLogger(ProtobufEdgeDataTypeHandler.class);
    /**
     * Parameter for specifying the name of the edge table.
     */
    public static final String EDGE_TABLE_NAME = "protobufedge.table.name";

    /**
     * Parameter for specifying the loader priority of the edge table.
     */
    public static final String EDGE_TABLE_LOADER_PRIORITY = "protobufedge.table.loader.priority";

    private static final String EDGE_DEFAULT_DATA_TYPE = "default";

    public static final String EDGE_TABLE_DISALLOWLIST_VALUES = ".protobufedge.table.disallowlist.values";
    public static final String EDGE_TABLE_DISALLOWLIST_FIELDS = ".protobufedge.table.disallowlist.fields";

    public static final String EDGE_TABLE_METADATA_ENABLE = "protobufedge.table.metadata.enable";
    public static final String EDGE_TABLE_DISALLOWLIST_ENABLE = "protobufedge.table.disallowlist.enable";

    public static final String EDGE_SPRING_CONFIG = "protobufedge.spring.config";

    public static final String EDGE_SPRING_RELATIONSHIPS = "protobufedge.table.relationships";
    public static final String EDGE_SPRING_COLLECTIONS = "protobufedge.table.collections";

    public static final String EDGE_SETUP_FAILURE_POLICY = "protobufedge.setup.default.failurepolicy";
    public static final String EDGE_PROCESS_FAILURE_POLICY = "protobufedge.process.default.failurepolicy";

    public static final String EDGE_STATS_LOG_USE_BLOOM = "protobufedge.stats.use.bloom";

    public static final String ACTIVITY_DATE_FUTURE_DELTA = "protobufedge.valid.activitytime.future.delta";
    public static final String ACTIVITY_DATE_PAST_DELTA = "protobufedge.valid.activitytime.past.delta";

    public static final String EVALUATE_PRECONDITIONS = "protobufedge.evaluate.preconditions";

    public static final String INCLUDE_ALL_EDGES = "protobufedge.include.all.edges";

    public static final String TRIM_FIELD_GROUP = ".trim.field.group";
    protected boolean trimFieldGroup = false;

    protected static final long ONE_DAY = 1000 * 60 * 60 * 24L;
    private static final Now now = Now.getInstance();

    private Map<String,Map<String,String>> edgeTypeLookup = new HashMap<>();

    private Map<String,Set<String>> disallowlistFieldLookup = new HashMap<>();
    private Map<String,Set<String>> disallowlistValueLookup = new HashMap<>();
    private boolean enableDisallowlist = false;

    private boolean evaluatePreconditions = false;
    private boolean includeAllEdges;
    private EdgePreconditionJexlContext edgePreconditionContext;
    private EdgePreconditionJexlEvaluation edgePreconditionEvaluation;
    private EdgePreconditionCacheHelper edgePreconditionCacheHelper;
    private EdgePreconditionArithmetic arithmetic = new EdgePreconditionArithmetic();
    private Map<String,JexlScript> scriptCache;

    protected String edgeTableName = null;
    protected String metadataTableName = null;
    protected boolean enableMetadata = false;
    protected MarkingFunctions markingFunctions;

    protected Map<String,EdgeDefinitionConfigurationHelper> edges = null;

    protected TaskAttemptContext taskAttemptContext = null;
    protected boolean useStatsLogBloomFilter = false;
    protected FailurePolicy setUpFailurePolicy = FailurePolicy.FAIL_JOB;
    protected FailurePolicy processFailurePolicy = FailurePolicy.FAIL_JOB;

    protected EdgeKeyVersioningCache versioningCache = null;

    protected HashSet<String> edgeRelationships = new HashSet<>();
    protected HashSet<String> collectionType = new HashSet<>();

    long futureDelta, pastDelta;
    long newFormatStartDate;

    SimpleGroupFieldNameParser fieldParser = new SimpleGroupFieldNameParser();

    public enum FailurePolicy {
        CONTINUE, FAIL_JOB
    }

    @Override
    public void setup(TaskAttemptContext context) {

        log.info("Running SpringProtobuf Edge Handler setup");
        this.taskAttemptContext = context;
        setup(context.getConfiguration());
    }

    public void setup(Configuration conf) {

        markingFunctions = MarkingFunctions.Factory.createMarkingFunctions();

        // This will fail if the TypeRegistry has not been initialized in the VM.
        TypeRegistry registry = TypeRegistry.getInstance(conf);

        // Grab the edge table name
        this.edgeTableName = ConfigurationHelper.isNull(conf, EDGE_TABLE_NAME, String.class);
        this.useStatsLogBloomFilter = conf.getBoolean(EDGE_STATS_LOG_USE_BLOOM, false);
        this.metadataTableName = ConfigurationHelper.isNull(conf, METADATA_TABLE_NAME, String.class);

        this.enableDisallowlist = ConfigurationHelper.isNull(conf, EDGE_TABLE_DISALLOWLIST_ENABLE, Boolean.class);
        this.enableMetadata = ConfigurationHelper.isNull(conf, EDGE_TABLE_METADATA_ENABLE, Boolean.class);

        setUpFailurePolicy = FailurePolicy.valueOf(conf.get(EDGE_SETUP_FAILURE_POLICY));
        processFailurePolicy = FailurePolicy.valueOf(conf.get(EDGE_PROCESS_FAILURE_POLICY));

        String springConfigFile = ConfigurationHelper.isNull(conf, EDGE_SPRING_CONFIG, String.class);
        futureDelta = ConfigurationHelper.isNull(conf, ACTIVITY_DATE_FUTURE_DELTA, Long.class);
        pastDelta = ConfigurationHelper.isNull(conf, ACTIVITY_DATE_PAST_DELTA, Long.class);

        evaluatePreconditions = Boolean.parseBoolean(conf.get(EVALUATE_PRECONDITIONS));
        includeAllEdges = Boolean.parseBoolean(conf.get(INCLUDE_ALL_EDGES));

        if (this.versioningCache == null) {
            this.versioningCache = new EdgeKeyVersioningCache(conf);
        }

        try {
            // Only one known edge key version so we simply grab the first one here

            // expected to be in the "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" format
            String startDate = versioningCache.getEdgeKeyVersionDateChange().entrySet().iterator().next().getValue();
            newFormatStartDate = DateNormalizer.parseDate(startDate, DateNormalizer.FORMAT_STRINGS).getTime();
            log.info("Edge key version change date set to: " + startDate);
        } catch (IOException e) {
            log.error("IO Exception could not get edge key version cache, will not generate edges!");
            if (setUpFailurePolicy == FailurePolicy.FAIL_JOB) {
                throw new RuntimeException("IO Exception could not get edge key version cache " + e.getMessage());
            } else {
                return; // no edges will be created but the ingest job will continue
            }
        } catch (ParseException e) {
            log.error("Unable to parse the switchover date will not generate edges!");
            if (setUpFailurePolicy == FailurePolicy.FAIL_JOB) {
                throw new RuntimeException("Protobufedge handler config not set correctly " + e.getMessage());
            } else {
                return; // no edges will be created but the ingest job will continue
            }
        } catch (NoSuchElementException e) {
            log.error("edge key version cache existed but was empty, will not generate edges");
            if (setUpFailurePolicy == FailurePolicy.FAIL_JOB) {
                throw new RuntimeException("Edge key versioning cache is empty " + e.getMessage());
            } else {
                return; // no edges will be created but the ingest job will continue
            }
        }

        // long term store edge definitions indexed by data type.
        /**
         * Parse and Store the Edge defs by data type
         */
        edges = new HashMap<>();
        ClassPathXmlApplicationContext ctx = null;
        try {
            ctx = new ClassPathXmlApplicationContext(ProtobufEdgeDataTypeHandler.class.getClassLoader().getResource(springConfigFile).toString());

            log.info("Got config on first try!");
        } catch (Exception e) {
            log.error("Problem getting config for ProtobufEdgeDataTypeHandler: {}", e);
            throw e;
        }

        Assert.notNull(ctx);

        registry.put(EDGE_DEFAULT_DATA_TYPE, null);

        // HashSet<String> edgeRelationships, collectionType;

        if (ctx.containsBean(EDGE_SPRING_RELATIONSHIPS) && ctx.containsBean(EDGE_SPRING_COLLECTIONS)) {
            edgeRelationships.addAll((HashSet<String>) ctx.getBean(EDGE_SPRING_RELATIONSHIPS));
            collectionType.addAll((HashSet<String>) ctx.getBean(EDGE_SPRING_COLLECTIONS));
        } else {
            log.error("Edge relationships and or collection types are not configured correctly. Cannot build edge definitions");
            if (setUpFailurePolicy == FailurePolicy.FAIL_JOB) {
                throw new RuntimeException("Missing some spring configurations");
            } else {
                return; // no edges will be created but the ingest job will continue
            }
        }

        for (Entry<String,Type> entry : registry.entrySet()) {
            if (ctx.containsBean(entry.getKey())) {
                EdgeDefinitionConfigurationHelper thing = (EdgeDefinitionConfigurationHelper) ctx.getBean(entry.getKey());

                // Always call init first before getting getting edge defs. This performs validation on the config file
                // and builds the edge pairs/groups
                thing.init(edgeRelationships, collectionType);

                edges.put(entry.getKey(), thing);
                if (thing.getEnrichmentTypeMappings() != null) {
                    edgeTypeLookup.put(entry.getKey(), thing.getEnrichmentTypeMappings());
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

        /*
         * The evaluate preconditions boolean determines whether or not we want to set up the Jexl Contexts to run preconditions the includeAllEdges boolean
         * determines whether we want the extra edge definitions from the preconditions included There are three scenarios: the first is you want to run and
         * evaluate preconditions. The evaluatePreconditions boolean will be set to true, the edges will be added, the jext contexts will be set up, and the
         * conditional edges will be evaluated.
         *
         * Second, you don't want to evaluate the conditional edges, but you want them included. The evaluatePreconditions boolean will be set to false so the
         * jext contexts are not set up, but the includeAllEdges boolean will be set to true.
         *
         * Third, you want neither of these done. Both booleans are set to false. The jexl context isn't set up and the conditional edges will be removed as to
         * not waste time evaluating edges where the conditions won't be met
         */
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
        registry.remove(EDGE_DEFAULT_DATA_TYPE);

        // loop through edge definitions and collect any ones that have disallowlisted fields
        if (this.enableDisallowlist) {
            Map<String,Set<EdgeDefinition>> disallowlistedEdges = new HashMap<>();
            for (String dType : edges.keySet()) {
                if (!disallowlistedEdges.containsKey(dType)) {
                    disallowlistedEdges.put(dType, new HashSet<>());
                }
                for (EdgeDefinition edgeDef : edges.get(dType).getEdges()) {
                    if (isDisallowlistField(dType, edgeDef.getSourceFieldName()) || isDisallowlistField(dType, edgeDef.getSinkFieldName())) {
                        disallowlistedEdges.get(dType).add(edgeDef);
                        log.warn("Removing Edge Definition due to disallowlisted Field: DataType: " + dType + " Definition: " + edgeDef.getSourceFieldName()
                                        + "-" + edgeDef.getSinkFieldName());
                    } else if (edgeDef.isEnrichmentEdge()) {
                        if (isDisallowlistField(dType, edgeDef.getEnrichmentField())) {
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
            log.info("disallowlisting of edges is disabled.");
        }

        log.info("Found edge definitions for " + edges.keySet().size() + " data types.");

        StringBuffer sb = new StringBuffer();
        sb.append("Data Types With Defined Edges: ");
        for (String t : edges.keySet()) {
            sb.append(t).append(" ");
        }
        log.info(sb.toString());
        log.info("ProtobufEdgeDataTypeHandler configured.");

    }

    public void setUpPreconditions() {
        // Set up the EdgePreconditionJexlContext, if enabled
        if (evaluatePreconditions) {
            edgePreconditionContext = new EdgePreconditionJexlContext(edges);
            edgePreconditionEvaluation = new EdgePreconditionJexlEvaluation();
            edgePreconditionCacheHelper = new EdgePreconditionCacheHelper(arithmetic);
            scriptCache = edgePreconditionCacheHelper.createScriptCacheFromEdges(edges);
        } else {

            // Else remove edges with a precondition
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

    protected static final class KeyFunnel implements Funnel<Key> {

        /**
         *
         */
        private static final long serialVersionUID = 3536725437637012624L;

        public KeyFunnel() {}

        @Override
        public void funnel(Key from, PrimitiveSink into) {
            into.putInt(from.hashCode());
        }

    }

    public Map<String,EdgeDefinitionConfigurationHelper> getEdges() {
        return edges;
    }

    public void setEdges(Map<String,EdgeDefinitionConfigurationHelper> edges) {
        this.edges = edges;
    }

    public Map<String,Set<String>> getDisallowlistFieldLookup() {
        return disallowlistFieldLookup;
    }

    public Map<String,Set<String>> getDisallowlistValueLookup() {
        return disallowlistValueLookup;
    }

    private boolean isDisallowlistField(String dataType, String fieldName) {
        if (disallowlistFieldLookup.containsKey(dataType)) {
            return this.disallowlistFieldLookup.get(dataType).contains(fieldName);
        } else if (disallowlistFieldLookup.containsKey(EDGE_DEFAULT_DATA_TYPE)) {
            // perhaps there is no disallowlist, which is fine
            return this.disallowlistFieldLookup.get(EDGE_DEFAULT_DATA_TYPE).contains(fieldName);
        }
        return false;
    }

    private boolean isdisallowlistValue(String dataType, String fieldValue) {
        if (disallowlistValueLookup.containsKey(dataType)) {
            return this.disallowlistValueLookup.get(dataType).contains(fieldValue);
        } else if (disallowlistValueLookup.containsKey(EDGE_DEFAULT_DATA_TYPE)) {
            // perhaps there is no disallowlist, which is fine
            return this.disallowlistValueLookup.get(EDGE_DEFAULT_DATA_TYPE).contains(fieldValue);
        }
        return false;
    }

    // used so we don't write duplicate stats entries for events with multiple field values;
    protected Set<HashCode> activityLog = null;
    protected Set<HashCode> durationLog = null;

    protected BloomFilter<Key> activityLogBloom = null;
    protected BloomFilter<Key> durationLogBloom = null;

    @Override
    public long process(KEYIN key, RawRecordContainer event, Multimap<String,NormalizedContentInterface> fields,
                    TaskInputOutputContext<KEYIN,? extends RawRecordContainer,KEYOUT,VALUEOUT> context, ContextWriter<KEYOUT,VALUEOUT> contextWriter)
                    throws IOException, InterruptedException {
        long edgesCreated = 0;
        long activityDate = -1;
        boolean validActivityDate = false;
        boolean activityEqualsEvent = false;
        String edgeAttribute2 = null, edgeAttribute3 = null;

        String loadDateStr = null;

        if (event.fatalError()) {
            return edgesCreated;
        } // early short circuit return

        // get edge definitions for this event type
        String typeName = getTypeName(event.getDataType());

        List<EdgeDefinition> edgeDefs = null;
        EdgeDefinitionConfigurationHelper edgeDefConfigs = null;
        if (!edges.containsKey(typeName)) {
            return edgesCreated; // short circuit, no edges defined for this type

        }
        edgeDefConfigs = edges.get(typeName);
        edgeDefs = edgeDefConfigs.getEdges();

        trimFieldGroup = context.getConfiguration().getBoolean(typeName + TRIM_FIELD_GROUP, false);

        /**
         * If enabled, set the filtered context from the NormalizedContentInterface and create the script cache
         */
        if (evaluatePreconditions) {
            setupPreconditionEvaluation(fields);

        }

        // Get the load date of the event from the fields map
        loadDateStr = getLoadDateString(fields);

        /*
         * normalize field names with groups
         */
        Multimap<String,NormalizedContentInterface> normalizedFields = HashMultimap.create();
        Map<String,Multimap<String,NormalizedContentInterface>> depthFirstList = new HashMap<>();
        Multimap<String,NormalizedContentInterface> tmp = null;
        for (Entry<String,NormalizedContentInterface> e : fields.entries()) {
            NormalizedContentInterface value = e.getValue();
            String subGroup = null;
            if (value instanceof GroupedNormalizedContentInterface) {
                subGroup = ((GroupedNormalizedContentInterface) value).getSubGroup();
            }
            String fieldName = getGroupedFieldName(value);
            tmp = depthFirstList.get(fieldName);
            if (tmp == null) {
                tmp = HashMultimap.create();
            }
            tmp.put(subGroup, value);
            depthFirstList.put(fieldName, tmp);

            normalizedFields.put(fieldName, value);
        }

        // get the edgeAttribute2 from the event fields map
        if (normalizedFields.containsKey(edgeDefConfigs.getEdgeAttribute2())) {
            edgeAttribute2 = normalizedFields.get(edgeDefConfigs.getEdgeAttribute2()).iterator().next().getIndexedFieldValue();
        }

        // get the edgeAttribute3 from the event fields map
        if (normalizedFields.containsKey(edgeDefConfigs.getEdgeAttribute3())) {
            edgeAttribute3 = normalizedFields.get(edgeDefConfigs.getEdgeAttribute3()).iterator().next().getIndexedFieldValue();
        }

        // get the activity date from the event fields map
        if (normalizedFields.containsKey(edgeDefConfigs.getActivityDateField())) {
            String actDate = normalizedFields.get(edgeDefConfigs.getActivityDateField()).iterator().next().getEventFieldValue();
            try {
                activityDate = DateNormalizer.parseDate(actDate, DateNormalizer.FORMAT_STRINGS).getTime();
                validActivityDate = validateActivityDate(activityDate, event.getDate());
            } catch (ParseException e1) {
                log.error("Parse exception when getting the activity date: " + actDate + " for edge creation " + e1.getMessage());
            }
        }

        // If the activity date is valid check to see if it is on the same day as the event date
        if (validActivityDate) {
            activityEqualsEvent = compareActivityAndEvent(activityDate, event.getDate());
        }

        // Track metadata for this event
        Map<Key,Set<Metadata>> eventMetadataRegistry = new HashMap<>();

        if (useStatsLogBloomFilter) {
            activityLogBloom = BloomFilter.create(new KeyFunnel(), 5000000);
            durationLogBloom = BloomFilter.create(new KeyFunnel(), 5000000);
            log.info("ProtobufEdgeDataTypeHandler using bloom filters");
        } else {
            activityLog = new HashSet<>();
            durationLog = new HashSet<>();
        }

        /*
         * Create Edge Values from Edge Definitions
         */

        Multimap<String,NormalizedContentInterface> mSource = null;
        Multimap<String,NormalizedContentInterface> mSink = null;

        for (EdgeDefinition edgeDef : edgeDefs) {
            arithmetic.clear();
            Map<String,Set<String>> matchingGroups = new HashMap<>();
            Map<String,Set<String>> excludedGroups = new HashMap<>();

            String jexlPreconditions = null;

            // don't bother evaluating preconditions if we know this event doesn't have the necessary fields for this edge
            if (depthFirstList.containsKey(edgeDef.getSourceFieldName()) && depthFirstList.containsKey(edgeDef.getSinkFieldName())) {
                mSource = depthFirstList.get(edgeDef.getSourceFieldName());
                mSink = depthFirstList.get(edgeDef.getSinkFieldName());
            } else {
                continue;
            }

            // bail if the event doesn't contain any values for the source or sink field
            if (null == mSource || null == mSink) {
                continue;
            }
            if (mSource.isEmpty() || mSink.isEmpty()) {
                continue;
            }

            /**
             * Fail fast for setting up precondition evaluations
             */
            if (evaluatePreconditions) {

                if (edgeDef.hasJexlPrecondition()) {
                    jexlPreconditions = edgeDef.getJexlPrecondition();

                    long start = System.currentTimeMillis();
                    if (!edgePreconditionEvaluation.apply(scriptCache.get(jexlPreconditions))) {

                        if (log.isTraceEnabled()) {
                            log.trace("Time to evaluate event(-): " + (System.currentTimeMillis() - start) + "ms.");
                        }
                        continue;

                    } else {
                        matchingGroups = arithmetic.getMatchingGroups();
                        excludedGroups = arithmetic.getExcludedGroups();

                        for (Entry excluded : excludedGroups.entrySet()) {
                            for (Object value : (HashSet) excluded.getValue()) {
                                if (matchingGroups.containsKey(excluded.getKey())) {
                                    matchingGroups.get(excluded.getKey()).remove(value);
                                    if (matchingGroups.get(excluded.getKey()).isEmpty()) {
                                        matchingGroups.remove(excluded.getKey());
                                    }
                                }
                            }

                        }

                        if (log.isTraceEnabled()) {
                            log.trace("Time to evaluate event(+): " + (System.currentTimeMillis() - start) + "ms.");
                        }

                        if (edgeDef.isGroupAware() && matchingGroups.size() == 0) {
                            continue;
                        }

                    }
                }
            }

            if (null != edgeDef.getEnrichmentField()) {
                if (normalizedFields.containsKey(edgeDef.getEnrichmentField()) && !(normalizedFields.get(edgeDef.getEnrichmentField()).isEmpty())) {
                    edgeDef.setEnrichmentEdge(true);
                } else {
                    edgeDef.setEnrichmentEdge(false);
                }
            }

            String enrichmentFieldName = getEnrichmentFieldName(edgeDef);

            String sourceGroup = getEdgeDefGroup(edgeDef.getSourceFieldName());
            String sinkGroup = getEdgeDefGroup(edgeDef.getSinkFieldName());

            // leave the old logic alone
            boolean sourceGroupEqualsSinkGroup = sourceGroup.equals(sinkGroup) && !sourceGroup.equals(NO_GROUP);
            if (!edgeDef.hasJexlPrecondition() || !edgeDef.isGroupAware()
                            || !(matchingGroups.containsKey(sourceGroup) || matchingGroups.containsKey(sinkGroup))) {

                // If within the same group, then within each subgroup that are in common for both the sink and source
                if (sourceGroupEqualsSinkGroup) {
                    Set<String> commonKeys = mSource.keySet();
                    commonKeys.retainAll(mSink.keySet());
                    /**
                     *
                     * We are using the intersection of 2 sets here to make sure we only loop over edges that we will create. Previously we would loop over all
                     * the possible edges even if they were at different levels of nesting.
                     *
                     */
                    for (String subGroup : commonKeys) {
                        for (NormalizedContentInterface ifaceSource : mSource.get(subGroup)) {
                            for (NormalizedContentInterface ifaceSink : mSink.get(subGroup)) {
                                if (ifaceSource == ifaceSink) {
                                    continue;
                                }
                                EdgeDataBundle edgeValue = createEdge(edgeDef, event, ifaceSource, sourceGroup, subGroup, ifaceSink, sinkGroup, subGroup,
                                                edgeAttribute2, edgeAttribute3, normalizedFields, depthFirstList, loadDateStr, activityDate, validActivityDate);
                                if (edgeValue != null) {

                                    // have to write out the keys as the edge values are generated, so counters get updated
                                    // and the system doesn't timeout.
                                    edgesCreated += writeEdges(edgeValue, context, contextWriter, validActivityDate, activityEqualsEvent, event.getDate());

                                    if (this.enableMetadata) {
                                        registerEventMetadata(eventMetadataRegistry, enrichmentFieldName, edgeValue, jexlPreconditions);
                                    }
                                }

                            }
                        }
                    }
                }
                // Otherwise we want to multiplex across all values for the sink and source regardless of subgroup
                else {
                    for (String sourceSubGroup : mSource.keySet()) {
                        for (NormalizedContentInterface ifaceSource : mSource.get(sourceSubGroup)) {
                            for (String sinkSubGroup : mSink.keySet()) {
                                for (NormalizedContentInterface ifaceSink : mSink.get(sinkSubGroup)) {
                                    EdgeDataBundle edgeValue = createEdge(edgeDef, event, ifaceSource, sourceGroup, sourceSubGroup, ifaceSink, sinkGroup,
                                                    sinkSubGroup, edgeAttribute2, edgeAttribute3, normalizedFields, depthFirstList, loadDateStr, activityDate,
                                                    validActivityDate);
                                    if (edgeValue != null) {

                                        // have to write out the keys as the edge values are generated, so counters get updated
                                        // and the system doesn't timeout.
                                        edgesCreated += writeEdges(edgeValue, context, contextWriter, validActivityDate, activityEqualsEvent, event.getDate());

                                        if (this.enableMetadata) {
                                            registerEventMetadata(eventMetadataRegistry, enrichmentFieldName, edgeValue, jexlPreconditions);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

            } else {
                if (sourceGroupEqualsSinkGroup) {
                    Set<String> commonKeys = matchingGroups.get(sourceGroup);
                    for (String subGroup : commonKeys) {
                        for (NormalizedContentInterface ifaceSource : mSource.get(subGroup)) {
                            for (NormalizedContentInterface ifaceSink : mSink.get(subGroup)) {
                                if (ifaceSource == ifaceSink) {
                                    continue;
                                }
                                EdgeDataBundle edgeValue = createEdge(edgeDef, event, ifaceSource, sourceGroup, subGroup, ifaceSink, sinkGroup, subGroup,
                                                edgeAttribute2, edgeAttribute3, normalizedFields, depthFirstList, loadDateStr, activityDate, validActivityDate);
                                if (edgeValue != null) {

                                    // have to write out the keys as the edge values are generated, so counters get updated
                                    // and the system doesn't timeout.
                                    edgesCreated += writeEdges(edgeValue, context, contextWriter, validActivityDate, activityEqualsEvent, event.getDate());

                                    if (this.enableMetadata) {
                                        registerEventMetadata(eventMetadataRegistry, enrichmentFieldName, edgeValue, jexlPreconditions);
                                    }
                                }

                            }
                        }
                    }

                } else {
                    Set<String> sourceSubGroups = matchingGroups.get(sourceGroup) != null ? matchingGroups.get(sourceGroup) : mSource.keySet();
                    Set<String> sinkSubGroups = matchingGroups.get(sinkGroup) != null ? matchingGroups.get(sinkGroup) : mSink.keySet();

                    for (String sourceSubGroup : sourceSubGroups) {
                        for (NormalizedContentInterface ifaceSource : mSource.get(sourceSubGroup)) {
                            for (String sinkSubGroup : sinkSubGroups) {
                                for (NormalizedContentInterface ifaceSink : mSink.get(sinkSubGroup)) {
                                    EdgeDataBundle edgeValue = createEdge(edgeDef, event, ifaceSource, sourceGroup, sourceSubGroup, ifaceSink, sinkGroup,
                                                    sinkSubGroup, edgeAttribute2, edgeAttribute3, normalizedFields, depthFirstList, loadDateStr, activityDate,
                                                    validActivityDate);
                                    if (edgeValue != null) {

                                        // have to write out the keys as the edge values are generated, so counters get updated
                                        // and the system doesn't timeout.
                                        edgesCreated += writeEdges(edgeValue, context, contextWriter, validActivityDate, activityEqualsEvent, event.getDate());

                                        if (this.enableMetadata) {
                                            registerEventMetadata(eventMetadataRegistry, enrichmentFieldName, edgeValue, jexlPreconditions);
                                        }
                                    }

                                }
                            }
                        }
                    }
                }
            }

        } // end edge defs

        if (this.enableMetadata) {
            writeMetadataMap(context, contextWriter, eventMetadataRegistry);
        }
        postProcessEdges(event, context, contextWriter, edgesCreated, loadDateStr);

        return edgesCreated;
    }

    private String getTypeName(Type dataType) {
        String typeName = dataType.typeName();
        String outputName = dataType.outputName();

        typeName = edges.containsKey(outputName) ? outputName : typeName;

        return typeName;
    }

    private void setupPreconditionEvaluation(Multimap<String,NormalizedContentInterface> fields) {
        long start = System.currentTimeMillis();
        edgePreconditionContext.setFilteredContextForNormalizedContentInterface(fields);
        edgePreconditionEvaluation.setJexlContext(edgePreconditionContext);
        if (log.isTraceEnabled()) {
            long time = System.currentTimeMillis() - start;
            // only worth logging those that took some time....
            if (time > 1) {
                if (log.isTraceEnabled()) {
                    log.trace("Time to set terms on the filtered context & EdgePreconditionJexlEvaluations: " + time + "ms.");
                }
            }
        }
    }

    // this could be moved to a more generic class
    private String getLoadDateString(Multimap<String,NormalizedContentInterface> fields) {
        String loadDateStr;
        Collection<NormalizedContentInterface> loadDates = fields.get(EventMapper.LOAD_DATE_FIELDNAME);
        if (!loadDates.isEmpty()) {
            NormalizedContentInterface nci = loadDates.iterator().next();
            Date date = new Date(Long.parseLong(nci.getEventFieldValue()));
            loadDateStr = DateHelper.format(date);
        } else {
            // If fields does not include the load date then use the current system time as load date
            loadDateStr = DateHelper.format(new Date(now.get()));
        }
        return loadDateStr;
    }

    protected void postProcessEdges(RawRecordContainer event, TaskInputOutputContext<KEYIN,? extends RawRecordContainer,KEYOUT,VALUEOUT> context,
                    ContextWriter<KEYOUT,VALUEOUT> contextWriter, long edgesCreated, String loadDateStr) throws IOException, InterruptedException {}

    protected EdgeDataBundle createEdge(EdgeDefinition edgeDef, RawRecordContainer event, NormalizedContentInterface ifaceSource, String sourceGroup,
                    String sourceSubGroup, NormalizedContentInterface ifaceSink, String sinkGroup, String sinkSubGroup, String edgeAttribute2,
                    String edgeAttribute3, Multimap<String,NormalizedContentInterface> normalizedFields,
                    Map<String,Multimap<String,NormalizedContentInterface>> depthFirstList, String loadDate, long activityDate, boolean validActivityDate) {
        // Get the edge value
        EdgeDataBundle edgeDataBundle = new EdgeDataBundle(edgeDef, ifaceSource, ifaceSink, event, this.getHelper(event.getDataType()));

        if (edgeAttribute2 != null) {
            edgeDataBundle.setEdgeAttribute2(edgeAttribute2);
        }
        if (edgeAttribute3 != null) {
            edgeDataBundle.setEdgeAttribute3(edgeAttribute3);
        }

        edgeDataBundle.setLoadDate(loadDate);
        edgeDataBundle.setActivityDate(activityDate);
        edgeDataBundle.setValidActivityDate(validActivityDate);

        // setup duration
        if (edgeDef.hasDuration()) {
            if (edgeDef.getUDDuration()) {
                NormalizedContentInterface upnci = getNullKeyedNCI(edgeDef.getUpTime(), normalizedFields);
                NormalizedContentInterface downnci = getNullKeyedNCI(edgeDef.getDownTime(), normalizedFields);
                if (null != upnci && null != downnci) {
                    edgeDataBundle.initDuration(upnci, downnci);
                }
            } else {
                NormalizedContentInterface elnci = getNullKeyedNCI(edgeDef.getElapsedTime(), normalizedFields);
                if (null != elnci) {
                    edgeDataBundle.initDuration(elnci);
                }
            }
        }

        String typeName = getTypeName(event.getDataType());

        // if the edgeDef is an enrichment definition, fill in the enrichedValue
        if (edgeDef.isEnrichmentEdge()) {
            if (edgeTypeLookup.containsKey(typeName)) {
                // if the group is the same as the sink or source,
                // then ensure we use the correct subgroup, otherwise we will enrich with the first value found
                Collection<NormalizedContentInterface> ifaceEnrichs = normalizedFields.get(edgeDef.getEnrichmentField());
                if (null != ifaceEnrichs && !ifaceEnrichs.isEmpty()) {
                    String enrichGroup = getEdgeDefGroup(edgeDef.getEnrichmentField());
                    if (!enrichGroup.equals(NO_GROUP)) {
                        if (enrichGroup.equals(sourceGroup)) {
                            ifaceEnrichs = depthFirstList.get(edgeDef.getEnrichmentField()).get(sourceSubGroup);
                        } else if (enrichGroup.equals(sinkGroup)) {
                            ifaceEnrichs = depthFirstList.get(edgeDef.getEnrichmentField()).get(sinkSubGroup);
                        }
                    }
                    if (null != ifaceEnrichs && !ifaceEnrichs.isEmpty()) {
                        if (ifaceEnrichs.size() > 1) {
                            log.warn("The enrichment field for " + edgeDef.getEnrichmentField() + " contains multiple values...choosing first valid entry");

                        }
                        for (NormalizedContentInterface ifaceEnrich : ifaceEnrichs) {
                            // the value of the enrichment field is a edge type lookup
                            String enrichedIndex = ifaceEnrich.getIndexedFieldValue();
                            // if we know this enrichment mode, then use it
                            if (edgeTypeLookup.get(typeName).containsKey(enrichedIndex)) {
                                edgeDataBundle.setEnrichedIndex(enrichedIndex); // required for eventMetadataRegistry
                                edgeDataBundle.setEnrichedValue(edgeTypeLookup.get(typeName).get(enrichedIndex));
                                break;
                            }
                        }
                    }

                }
            } else {
                log.error("Cannot enrich edge because Enrichment Edge Types are not defined for data type: " + typeName);

            }
        }

        if (event.isRequiresMasking()) {
            maskEdge(edgeDataBundle, event);
        }
        // Is this an edge to delete?
        edgeDataBundle.setIsDeleting(this.getHelper(event.getDataType()).getDeleteMode());

        // Validate the edge value
        if (!edgeDataBundle.isValid()) {
            return null;
        }

        // Set edge type if this is an enrichment edge
        if (edgeDef.isEnrichmentEdge()) {
            if (null != edgeDataBundle.getEnrichedValue()) {
                edgeDataBundle.setEdgeType(edgeDataBundle.getEnrichedValue());
            }
        }

        // check value disallowlist
        if (this.enableDisallowlist && isdisallowlistValue(typeName, edgeDataBundle.getSource().getValue(ValueType.INDEXED))
                        || isdisallowlistValue(typeName, edgeDataBundle.getSink().getValue(ValueType.INDEXED))) {
            return null;
        }

        return edgeDataBundle;
    }

    protected void maskEdge(EdgeDataBundle edgeDataBundle, RawRecordContainer record) {}

    protected void registerEventMetadata(Map<Key,Set<Metadata>> eventMetadataRegistry, String enrichmentFieldName, EdgeDataBundle edgeValue) {
        registerEventMetadata(eventMetadataRegistry, enrichmentFieldName, edgeValue, null);
    }

    protected void registerEventMetadata(Map<Key,Set<Metadata>> eventMetadataRegistry, String enrichmentFieldName, EdgeDataBundle edgeValue,
                    String jexlPrecondition) {
        // add to the eventMetadataRegistry map
        Key baseKey = createMetadataEdgeKey(edgeValue, edgeValue.getSource(), edgeValue.getSource().getIndexedFieldValue(), edgeValue.getSink(),
                        edgeValue.getSink().getIndexedFieldValue(), this.getVisibility(edgeValue));
        Key fwdMetaKey = EdgeKey.getMetadataKey(baseKey);
        Key revMetaKey = EdgeKey.getMetadataKey(EdgeKey.swapSourceSink(EdgeKey.decode(baseKey)).encode());

        Set<Metadata> fwdMetaSet = eventMetadataRegistry.get(fwdMetaKey);
        if (null == fwdMetaSet) {
            fwdMetaSet = new HashSet<>();
            eventMetadataRegistry.put(fwdMetaKey, fwdMetaSet);
        }
        Set<Metadata> revMetaSet = eventMetadataRegistry.get(revMetaKey);
        if (null == revMetaSet) {
            revMetaSet = new HashSet<>();
            eventMetadataRegistry.put(revMetaKey, revMetaSet);
        }

        // Build the Protobuf for the value
        Metadata.Builder forwardBuilder = Metadata.newBuilder().setSource(edgeValue.getSource().getFieldName()).setSink(edgeValue.getSink().getFieldName())
                        .setDate(DateHelper.format(new Date(edgeValue.getEventDate())));
        Metadata.Builder reverseBuilder = Metadata.newBuilder().setDate(DateHelper.format(new Date(edgeValue.getEventDate())))
                        .setSource(edgeValue.getSink().getFieldName()).setSink(edgeValue.getSource().getFieldName());
        if (enrichmentFieldName != null) {
            forwardBuilder.setEnrichment(enrichmentFieldName).setEnrichmentIndex(edgeValue.getEnrichedIndex());
            reverseBuilder.setEnrichment(enrichmentFieldName).setEnrichmentIndex(edgeValue.getEnrichedIndex());
        }

        if (jexlPrecondition != null) {
            forwardBuilder.setJexlPrecondition(jexlPrecondition);
            reverseBuilder.setJexlPrecondition(jexlPrecondition);
        }

        fwdMetaSet.add(forwardBuilder.build());
        revMetaSet.add(reverseBuilder.build());
    }

    protected String getEnrichmentFieldName(EdgeDefinition edgeDef) {
        return (edgeDef.isEnrichmentEdge() ? edgeDef.getEnrichmentField() : null);
    }

    protected void writeMetadataMap(TaskInputOutputContext<KEYIN,? extends RawRecordContainer,KEYOUT,VALUEOUT> context,
                    ContextWriter<KEYOUT,VALUEOUT> contextWriter, Map<Key,Set<Metadata>> metadataRegistry) throws IOException, InterruptedException {
        writeMetadataMap(context, contextWriter, metadataRegistry, this.metadataTableName);
    }

    protected void writeMetadataMap(TaskInputOutputContext<KEYIN,? extends RawRecordContainer,KEYOUT,VALUEOUT> context,
                    ContextWriter<KEYOUT,VALUEOUT> contextWriter, Map<Key,Set<Metadata>> metadataRegistry, String tableName)
                    throws IOException, InterruptedException {
        for (Entry<Key,Set<Metadata>> entry : metadataRegistry.entrySet()) {
            Key metaKey = entry.getKey();
            MetadataValue.Builder valueBuilder = MetadataValue.newBuilder();
            valueBuilder.addAllMetadata(entry.getValue());
            MetadataValue value = valueBuilder.build();

            // write out a metadataRegistry key
            BulkIngestKey bulk = new BulkIngestKey(new Text(tableName), metaKey);
            contextWriter.write(bulk, new Value(value.toByteArray()), context);
        }
    }

    // this just avoids ugly copy pasta
    private NormalizedContentInterface getNullKeyedNCI(String fieldValue, Multimap<String,NormalizedContentInterface> fields) {
        Iterator<NormalizedContentInterface> nciIter = fields.get(fieldValue).iterator();
        if (nciIter.hasNext()) {
            return nciIter.next();
        }
        return null;
    }

    protected String getGroupedFieldName(NormalizedContentInterface value) {
        String fieldName = value.getIndexedFieldName();
        if (value instanceof GroupedNormalizedContentInterface) {
            GroupedNormalizedContentInterface grouped = (GroupedNormalizedContentInterface) value;
            if (grouped.isGrouped() && grouped.getGroup() != null) {
                if (!grouped.getGroup().isEmpty()) {
                    String group;
                    if (trimFieldGroup) {
                        group = fieldParser.getTrimmedGroup(grouped.getGroup());
                    } else {
                        group = grouped.getGroup();
                    }
                    fieldName = fieldName + '.' + group;

                }
            }
        }
        return fieldName;
    }

    private static final String NO_GROUP = "";

    protected String getEdgeDefGroup(String groupedFieldName) {
        int index = groupedFieldName.indexOf('.');
        if (index >= 0) {
            return groupedFieldName.substring(index + 1);
        } else {
            return NO_GROUP;
        }
    }

    /*
     * This part makes the determination as to what type of edge key to build
     */
    protected long writeEdges(EdgeDataBundle value, TaskInputOutputContext<KEYIN,? extends RawRecordContainer,KEYOUT,VALUEOUT> context,
                    ContextWriter<KEYOUT,VALUEOUT> contextWriter, boolean validActivtyDate, boolean sameActivityDate, long eventDate)
                    throws IOException, InterruptedException {
        long edgesCreated = 0;
        if (eventDate < newFormatStartDate) {
            edgesCreated += writeEdges(value, context, contextWriter, EdgeKey.DATE_TYPE.OLD_EVENT);
        } else {
            if (validActivtyDate) {
                if (sameActivityDate) {
                    edgesCreated += writeEdges(value, context, contextWriter, EdgeKey.DATE_TYPE.ACTIVITY_AND_EVENT);
                } else {
                    edgesCreated += writeEdges(value, context, contextWriter, EdgeKey.DATE_TYPE.ACTIVITY_ONLY);
                    edgesCreated += writeEdges(value, context, contextWriter, EdgeKey.DATE_TYPE.EVENT_ONLY);
                }
            } else {
                edgesCreated += writeEdges(value, context, contextWriter, EdgeKey.DATE_TYPE.EVENT_ONLY);
            }
        }

        return edgesCreated;
    }

    protected long writeEdges(EdgeDataBundle value, TaskInputOutputContext<KEYIN,? extends RawRecordContainer,KEYOUT,VALUEOUT> context,
                    ContextWriter<KEYOUT,VALUEOUT> contextWriter, EdgeKey.DATE_TYPE date_type) throws IOException, InterruptedException {

        // the number of key values that have been written
        long counter = 0;

        // writing an edge requires writing the edge, the hour activity stat, and the duration stat, and optionally the link stat

        /*
         * Regular Edges
         */
        Key edgeKey = createEdgeKey(value, value.getSource(), value.getSource().getValue(ValueType.INDEXED), value.getSink(),
                        value.getSink().getValue(ValueType.INDEXED), this.getVisibility(value), date_type);
        writeKey(edgeKey, value.getEdgeValue(true, date_type), context, contextWriter);
        counter++;

        // source STATS/ACTIVITY row
        Key sourceActivityKey = createStatsKey(STATS_TYPE.ACTIVITY, value, value.getSource(), value.getSource().getValue(ValueType.INDEXED),
                        this.getVisibility(value), date_type);
        counter += writeKey(sourceActivityKey, value.getStatsActivityValue(true, date_type), context, contextWriter);

        // source STATS/DURATION row

        if (value.hasDuration()) {
            Key sourceDurationKey = createStatsKey(STATS_TYPE.DURATION, value, value.getSource(), value.getSource().getValue(ValueType.INDEXED),
                            this.getDurationVisibility(value), date_type);
            counter += writeKey(sourceDurationKey, value.getDurationAsValue(true), context, contextWriter);
        }

        /*
         * Regular Bidirectional Edge
         */

        if (value.getEdgeDirection() == EdgeDirection.BIDIRECTIONAL) {
            Key biKey = createEdgeKey(value, value.getSink(), value.getSink().getValue(ValueType.INDEXED), value.getSource(),
                            value.getSource().getValue(ValueType.INDEXED), this.getVisibility(value), date_type);

            counter += writeKey(biKey, value.getEdgeValue(false, date_type), context, contextWriter);

            // sink STATS/ACTIVITY row
            Key sinkActivityKey = createStatsKey(STATS_TYPE.ACTIVITY, value, value.getSink(), value.getSink().getValue(ValueType.INDEXED),
                            this.getVisibility(value), date_type);
            counter += writeKey(sinkActivityKey, value.getStatsActivityValue(false, date_type), context, contextWriter);

            // sink STATS/DURATION row
            if (value.hasDuration()) {
                Key sinkDurationKey = createStatsKey(STATS_TYPE.DURATION, value, value.getSink(), value.getSink().getValue(ValueType.INDEXED),
                                this.getDurationVisibility(value), date_type);
                counter += writeKey(sinkDurationKey, value.getDurationAsValue(false), context, contextWriter);
            }
        }

        // currently requiresMasking is only set if both vertices have masked values

        if (value.requiresMasking()) {
            Text maskedVisibility = new Text(flatten(value.getMaskedVisibility()));

            Key maskedKey = createEdgeKey(value, value.getSource(), value.getSource().getMaskedValue(ValueType.INDEXED), value.getSink(),
                            value.getSink().getMaskedValue(ValueType.INDEXED), maskedVisibility, date_type);
            counter += writeKey(maskedKey, value.getEdgeValue(true, date_type), context, contextWriter);

            if (value.getSource().hasMaskedValue()) {
                // source STATS/ACTIVITY row
                Key maskedSourceActivityKey = createStatsKey(STATS_TYPE.ACTIVITY, value, value.getSource(), value.getSource().getMaskedValue(ValueType.INDEXED),
                                maskedVisibility, date_type);
                counter += writeKey(maskedSourceActivityKey, value.getStatsActivityValue(true, date_type), context, contextWriter);

                // source STATS/DURATION row
                if (value.hasDuration()) {
                    Key maskedSourceDurationKey = createStatsKey(STATS_TYPE.DURATION, value, value.getSource(),
                                    value.getSource().getMaskedValue(ValueType.INDEXED), maskedVisibility, date_type);
                    counter += writeKey(maskedSourceDurationKey, value.getDurationAsValue(true), context, contextWriter);
                }
            }

            /*
             * Masked Bidirectional Edge
             */
            if (value.getEdgeDirection() == EdgeDirection.BIDIRECTIONAL) {
                Key maskedBiKey = createEdgeKey(value, value.getSink(), value.getSink().getMaskedValue(ValueType.INDEXED), value.getSource(),
                                value.getSource().getMaskedValue(ValueType.INDEXED), maskedVisibility, date_type);
                counter += writeKey(maskedBiKey, value.getEdgeValue(false, date_type), context, contextWriter);

                if (value.getSink().hasMaskedValue()) {
                    // sink STATS/ACTIVITY row
                    Key maskedSinkActivityKey = createStatsKey(STATS_TYPE.ACTIVITY, value, value.getSink(), value.getSink().getMaskedValue(ValueType.INDEXED),
                                    maskedVisibility, date_type);
                    counter += writeKey(maskedSinkActivityKey, value.getStatsActivityValue(false, date_type), context, contextWriter);

                    // sink STATS/DURATION row
                    if (value.hasDuration()) {
                        Key maskedSinkDurationKey = createStatsKey(STATS_TYPE.DURATION, value, value.getSink(),
                                        value.getSink().getMaskedValue(ValueType.INDEXED), maskedVisibility, date_type);
                        counter += writeKey(maskedSinkDurationKey, value.getDurationAsValue(false), context, contextWriter);
                    }
                }
            }
        }

        return counter;
    }

    private Key createMetadataEdgeKey(EdgeDataBundle edgeValue, VertexValue source, String sourceValue, VertexValue sink, String sinkValue, Text visibility) {
        long truncatedEventDate = edgeValue.getEventDate() / ONE_DAY * ONE_DAY;
        return createEdgeKey(edgeValue, source, sourceValue, sink, sinkValue, visibility, truncatedEventDate, EdgeKey.DATE_TYPE.OLD_EVENT);
    }

    protected Key createEdgeKey(EdgeDataBundle edgeValue, VertexValue source, String sourceValue, VertexValue sink, String sinkValue, Text visibility,
                    EdgeKey.DATE_TYPE date_type) {
        return createEdgeKey(edgeValue, source, sourceValue, sink, sinkValue, visibility, edgeValue.getEventDate(), date_type);
    }

    private Key createEdgeKey(EdgeDataBundle edgeValue, VertexValue source, String sourceValue, VertexValue sink, String sinkValue, Text visibility,
                    long timestamp, EdgeKey.DATE_TYPE date_type) {
        String typeName = edgeValue.getDataTypeName();
        datawave.edge.util.EdgeKey.EdgeKeyBuilder builder = datawave.edge.util.EdgeKey.newBuilder(EDGE_FORMAT.STANDARD).escape();
        builder.setSourceData(sourceValue).setSinkData(sinkValue).setType(edgeValue.getEdgeType()).setYyyymmdd(edgeValue.getYyyyMMdd(date_type))
                        .setSourceRelationship(source.getRelationshipType()).setSinkRelationship(sink.getRelationshipType())
                        .setSourceAttribute1(source.getCollectionType()).setSinkAttribute1(sink.getCollectionType())
                        .setAttribute3(edgeValue.getEdgeAttribute3()).setAttribute2(edgeValue.getEdgeAttribute2()).setColvis(visibility).setTimestamp(timestamp)
                        .setDateType(date_type);
        builder.setDeleted(edgeValue.isDeleting());

        return builder.build().encode();
    }

    protected Key createStatsKey(STATS_TYPE statsType, EdgeDataBundle edgeValue, VertexValue vertex, String value, Text visibility,
                    EdgeKey.DATE_TYPE date_type) {
        String typeName = edgeValue.getDataTypeName();
        datawave.edge.util.EdgeKey.EdgeKeyBuilder builder = datawave.edge.util.EdgeKey.newBuilder(EDGE_FORMAT.STATS).escape();
        builder.setSourceData(value).setStatsType(statsType).setType(edgeValue.getEdgeType()).setYyyymmdd(edgeValue.getYyyyMMdd(date_type))
                        .setSourceRelationship(vertex.getRelationshipType()).setSourceAttribute1(vertex.getCollectionType())
                        .setAttribute3(edgeValue.getEdgeAttribute3()).setAttribute2(edgeValue.getEdgeAttribute2()).setColvis(visibility)
                        .setTimestamp(edgeValue.getEventDate()).setDateType(date_type);
        builder.setDeleted(edgeValue.isDeleting());
        Key key = builder.build().encode();
        boolean isNewKey = false;

        /**
         * compute 128bit hashcode for edge instead of storing the raw key value we store a 128bit hash value of the edge.
         */
        HashFunction hf = Hashing.murmur3_128();

        /**
         * of note. The google HashCode is a well defined object (equals and hashcode) and is immutable. it's safe for use in hashsets.
         *
         */
        HashCode hcode = hf.newHasher().putString(key.toString() + key.hashCode(), Charsets.UTF_8).hash();

        switch (statsType) {
            case ACTIVITY:
                if (null != activityLog) {
                    if (!activityLog.contains(hcode)) {
                        activityLog.add(hcode);
                        isNewKey = true;
                    }
                } else if (useStatsLogBloomFilter) {
                    if (!activityLogBloom.mightContain(key)) {
                        activityLogBloom.put(key);
                        isNewKey = true;
                    }
                } else {
                    throw new RuntimeException("ActivityLog is null yet we are not configured to use the bloom filters.");
                }
                break;
            case DURATION:
                if (null != durationLog) {
                    if (!durationLog.contains(hcode)) {
                        durationLog.add(hcode);
                        isNewKey = true;
                    }
                } else if (useStatsLogBloomFilter) {
                    if (!durationLogBloom.mightContain(key)) {
                        durationLogBloom.put(key);
                        isNewKey = true;
                    }
                } else {
                    throw new RuntimeException("DurationLogLog is null yet we are not configured to use the bloom filters.");
                }
                break;
        }
        if (isNewKey) {
            return key;
        } else {
            return null;
        }
    }

    protected int writeKey(Key key, Value val, TaskInputOutputContext<KEYIN,? extends RawRecordContainer,KEYOUT,VALUEOUT> context,
                    ContextWriter<KEYOUT,VALUEOUT> contextWriter) throws IOException, InterruptedException {
        if (key == null)
            return 0;
        BulkIngestKey bk = new BulkIngestKey(new Text(this.edgeTableName), key);
        contextWriter.write(bk, val, context);
        return 1;
    }

    private Map<String,String> findLookupMap(Map<String,Map<String,String>> lookup, String typeName) {
        if (lookup.containsKey(typeName)) {
            return lookup.get(typeName);
        } else {
            return lookup.get(EDGE_DEFAULT_DATA_TYPE);
        }
    }

    @Override
    public IngestHelperInterface getHelper(Type datatype) {
        return datatype.getIngestHelper(taskAttemptContext.getConfiguration());
    }

    /**
     * Given a {@link Configuration}, this method returns the required locality groups used by the edge table.
     *
     * @param conf
     *            the configuration
     * @return locality groups used by edge table
     */
    public static Map<String,Set<Text>> getLocalityGroups(Configuration conf) {

        // Edge table definition properties define either individual edges or groups of edges.
        // We create in each data-conf.xml file the ability to specify locality groups
        // These are specified by dataType.edge.table.locality.groups.
        // This does allow the condition for a colfam in one datatype to be specified
        // as a locality group, but not be specified as one in the other datatype
        // At this point that does not seem to be a problem because we are using
        // colfam to denote a logical mapping of the data elements. So if two datatypes
        // have the same colfam for an edge and one is used for a locality group
        // then the other one should most likely be as well.
        //
        // groupname:colfam
        //
        // NOTE: I can't find this being called. It is commented out in the EdgeIngestConfigHelper
        //
        Map<String,Set<Text>> locs = new HashMap<>();

        Pattern p = Pattern.compile("^.+\\.edge\\.table\\.locality\\.groups");

        for (Entry<String,String> confEntry : conf) {
            Matcher m = p.matcher(confEntry.getKey());
            if (m.matches()) {
                String parts[] = StringUtils.split(confEntry.getValue(), ',', false);
                for (String s : parts) {
                    String colfams[] = StringUtils.split(s, ':', false);
                    if (colfams.length > 1) {
                        if (!locs.containsKey(colfams[0])) {
                            locs.put(colfams[0], new HashSet<>());
                        }
                        for (int i = 1; i < colfams.length; i++) {
                            locs.get(colfams[0]).add(new Text(colfams[i]));
                        }
                    }
                }
            }
        }
        return locs;

    }

    /**
     * A helper routine to determine the visibility for a field.
     *
     * @param markings
     *            a map of the markings
     * @param event
     *            the event container
     * @return the visibility as Text object
     */
    protected Text getVisibility(Map<String,String> markings, RawRecordContainer event) {
        try {
            if (null == markings || markings.isEmpty()) {
                return new Text(flatten(event.getVisibility()));
            } else {
                return new Text(flatten(markingFunctions.translateToColumnVisibility(markings)));
            }
        } catch (datawave.marking.MarkingFunctions.Exception e) {
            throw new RuntimeException("Cannot convert markings into column visibility", e);
        }
    }

    protected Text getVisibility(EdgeDataBundle value) {
        if (value.getForceMaskedVisibility()) {
            return new Text(flatten(value.getMaskedVisibility()));
        } else {
            return getVisibility(value.getMarkings(), value.getEvent());
        }
    }

    protected Text getDurationVisibility(EdgeDataBundle value) {
        if (value.getForceMaskedVisibility()) {
            return new Text(flatten(value.getMaskedVisibility()));
        } else {
            return getVisibility(value.getDurationValue().getMarkings(), value.getEvent());
        }
    }

    /**
     * Create a flattened visibility
     *
     * @param vis
     *            a visibility
     * @return the flattened visibility
     */
    protected byte[] flatten(ColumnVisibility vis) {
        return markingFunctions.flatten(vis);
    }

    @Override
    public void close(TaskAttemptContext context) {}

    // has chance to blow up memory depending on what is defined, so this isn't supported.
    @Override
    public Multimap<BulkIngestKey,Value> processBulk(KEYIN key, RawRecordContainer event, Multimap<String,NormalizedContentInterface> fields,
                    StatusReporter reporter) {
        throw new UnsupportedOperationException("processBulk is not supported, please use process");
    }

    @Override
    public String[] getTableNames(Configuration conf) {
        List<String> tableNames = new ArrayList<>();
        tableNames.add(ConfigurationHelper.isNull(conf, EDGE_TABLE_NAME, String.class));
        if (conf.getBoolean(EDGE_TABLE_METADATA_ENABLE, this.enableMetadata)) {
            tableNames.add(ConfigurationHelper.isNull(conf, METADATA_TABLE_NAME, String.class));
            if (LoadDateTableConfigHelper.isLoadDatesEnabled(conf)) {
                tableNames.add(LoadDateTableConfigHelper.getLoadDatesTableName(conf));
            }
        }
        return tableNames.toArray(new String[tableNames.size()]);
    }

    @Override
    public int[] getTableLoaderPriorities(Configuration conf) {
        int[] priorities = new int[3];
        int index = 0;
        priorities[index++] = ConfigurationHelper.isNull(conf, EDGE_TABLE_LOADER_PRIORITY, Integer.class);
        if (conf.getBoolean(EDGE_TABLE_METADATA_ENABLE, this.enableMetadata)) {
            priorities[index++] = ConfigurationHelper.isNull(conf, METADATA_TABLE_LOADER_PRIORITY, Integer.class);
            if (LoadDateTableConfigHelper.isLoadDatesEnabled(conf)) {
                priorities[index++] = LoadDateTableConfigHelper.getLoadDatesTableLoaderPriority(conf);
            }
        }
        if (index != priorities.length) {
            return Arrays.copyOf(priorities, index);
        } else {
            return priorities;
        }
    }

    /*
     * validates the activity date using the past and future delta configured variables both past and future deltas are expected to be positive numbers (in
     * milliseconds)
     */
    protected boolean validateActivityDate(long activityTime, long eventTime) {

        if (eventTime - activityTime > pastDelta) {
            // if activity > event then number is negative and will be checked in the next else if statement
            return false;
        } else if (activityTime - eventTime > futureDelta) {
            // if activity < event then number is negative and would have been checked by the previous if statement
            return false;
        } else {
            return true;
        }
    }

    /*
     * Compares activity and event time. Returns true if they are both on the same day. Eg. both result in the same yyyyMMdd string
     */
    protected boolean compareActivityAndEvent(long activityDate, long eventDate) {
        // The date toString() returns dates in the format yyyy-mm-dd
        if (DateHelper.format(activityDate).equals(DateHelper.format(eventDate))) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public RawRecordMetadata getMetadata() {
        // TODO Auto-generated method stub
        return null;
    }

    public void setVersioningCache(EdgeKeyVersioningCache versioningCache) {
        this.versioningCache = versioningCache;
    }
}
