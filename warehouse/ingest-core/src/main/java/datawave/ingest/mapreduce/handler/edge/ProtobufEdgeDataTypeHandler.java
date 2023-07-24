package datawave.ingest.mapreduce.handler.edge;

import static datawave.ingest.mapreduce.handler.edge.EdgeIngestConfiguration.EDGE_DEFAULT_DATA_TYPE;
import static datawave.ingest.mapreduce.handler.edge.EdgeIngestConfiguration.EDGE_TABLE_METADATA_ENABLE;
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
import org.apache.commons.jexl2.Script;
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
import com.google.common.collect.Multimap;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.hash.PrimitiveSink;

import datawave.edge.util.EdgeKey;
import datawave.edge.util.EdgeKey.EDGE_FORMAT;
import datawave.edge.util.EdgeKey.STATS_TYPE;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.ConfigurationHelper;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.ingest.IngestHelperInterface;
import datawave.ingest.mapreduce.handler.ExtendedDataTypeHandler;
import datawave.ingest.mapreduce.handler.edge.define.EdgeDataBundle;
import datawave.ingest.mapreduce.handler.edge.define.EdgeDefinition;
import datawave.ingest.mapreduce.handler.edge.define.EdgeDefinitionConfigurationHelper;
import datawave.ingest.mapreduce.handler.edge.define.EdgeDirection;
import datawave.ingest.mapreduce.handler.edge.define.VertexValue;
import datawave.ingest.mapreduce.handler.edge.define.VertexValue.ValueType;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.writer.ContextWriter;
import datawave.ingest.metadata.RawRecordMetadata;
import datawave.ingest.table.config.LoadDateTableConfigHelper;
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

    public static final String EDGE_SETUP_FAILURE_POLICY = "protobufedge.setup.default.failurepolicy";
    public static final String EDGE_PROCESS_FAILURE_POLICY = "protobufedge.process.default.failurepolicy";
    public static final String EDGE_STATS_LOG_USE_BLOOM = "protobufedge.stats.use.bloom";

    protected static final long ONE_DAY = 1000 * 60 * 60 * 24;

    protected String edgeTableName = null;
    protected String metadataTableName = null;
    protected boolean enableMetadata = false;
    protected MarkingFunctions markingFunctions;

    protected TaskAttemptContext taskAttemptContext = null;
    protected boolean useStatsLogBloomFilter = false;
    protected FailurePolicy setUpFailurePolicy = FailurePolicy.FAIL_JOB;
    protected FailurePolicy processFailurePolicy = FailurePolicy.FAIL_JOB;

    long newFormatStartDate;

    EdgeIngestConfiguration edgeConfig;

    // used so we don't write duplicate stats entries for events with multiple field values;
    protected Set<HashCode> activityLog = null;
    protected Set<HashCode> durationLog = null;

    protected BloomFilter<Key> activityLogBloom = null;
    protected BloomFilter<Key> durationLogBloom = null;

    Map<Key,Set<Metadata>> eventMetadataRegistry;
    EdgeDefinitionConfigurationHelper edgeDefConfigs = null;

    private static final String NO_GROUP = "";

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
        setUpFailurePolicy = ProtobufEdgeDataTypeHandler.FailurePolicy.valueOf(conf.get(EDGE_SETUP_FAILURE_POLICY));
        processFailurePolicy = ProtobufEdgeDataTypeHandler.FailurePolicy.valueOf(conf.get(EDGE_PROCESS_FAILURE_POLICY));

        try {
            edgeConfig = new EdgeIngestConfiguration(conf);
            newFormatStartDate = edgeConfig.getStartDateIfValid();
        } catch (Exception e) {
            if (setUpFailurePolicy == ProtobufEdgeDataTypeHandler.FailurePolicy.FAIL_JOB) {
                throw e;
            } else {
                return; // no edges will be created but the ingest job will continue
            }
        }
        log.info("ProtobufEdgeDataTypeHandler configured.");

    }

    protected static final class KeyFunnel implements Funnel<Key> {

        private static final long serialVersionUID = 3536725437637012624L;

        public KeyFunnel() {}

        @Override
        public void funnel(Key from, PrimitiveSink into) {
            into.putInt(from.hashCode());
        }

    }

    @Override
    public long process(KEYIN key, RawRecordContainer event, Multimap<String,NormalizedContentInterface> fields,
                    TaskInputOutputContext<KEYIN,? extends RawRecordContainer,KEYOUT,VALUEOUT> context, ContextWriter<KEYOUT,VALUEOUT> contextWriter)
                    throws IOException, InterruptedException {

        long edgesCreated = 0;
        String typeName = getTypeName(event.getDataType());

        // get edge definitions for this event type
        edgeDefConfigs = edgeConfig.getEdges().get(typeName);
        List<EdgeDefinition> edgeDefs = edgeDefConfigs.getEdges();

        if (shortCircuit(event, typeName, edgeConfig.getEdges())) {
            return edgesCreated;
        }

        // Track metadata for this event
        eventMetadataRegistry = new HashMap<>();

        /**
         * If enabled, set the filtered context from the NormalizedContentInterface and create the script cache
         */
        if (edgeConfig.evaluatePreconditions()) {
            setupPreconditionEvaluation(fields);
        }

        boolean trimGroups = context.getConfiguration().getBoolean(typeName + EdgeIngestConfiguration.TRIM_FIELD_GROUP, false);
        EdgeEventFieldUtil edgeEventFieldUtil = new EdgeEventFieldUtil(trimGroups);
        /*
         * normalize field names with groups
         */
        edgeEventFieldUtil.normalizeAndGroupFields(fields);
        EdgeDataBundle baseEdgeBundle = new EdgeDataBundle(event);
        edgeEventFieldUtil.setEdgeInfoFromEventFields(baseEdgeBundle, edgeDefConfigs, event, edgeConfig, newFormatStartDate, context.getConfiguration(),
                        typeName);

        // Is this an edge to delete?
        baseEdgeBundle.setIsDeleting(this.getHelper(event.getDataType()).getDeleteMode());

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

        for (EdgeDefinition edgeDef : edgeDefs) {
            edgeConfig.clearArithmeticMatchingGroups();
            Map<String,Set<String>> matchingGroups = new HashMap<>();
            String jexlPreconditions = null;
            Multimap<String,NormalizedContentInterface> mSource = edgeEventFieldUtil.getDepthFirstList().get(edgeDef.getSourceFieldName());
            Multimap<String,NormalizedContentInterface> mSink = edgeEventFieldUtil.getDepthFirstList().get(edgeDef.getSinkFieldName());

            // don't bother checking the preconditions if we don't even have the necessary fields in the event
            if (null == mSource || null == mSink || mSource.isEmpty() || mSink.isEmpty()) {
                continue;
            }

            /**
             * Fail fast for setting up precondition evaluations
             */
            if (edgeConfig.evaluatePreconditions()) {

                if (edgeDef.hasJexlPrecondition()) {
                    jexlPreconditions = edgeDef.getJexlPrecondition();

                    long start = System.currentTimeMillis();
                    if (!edgeConfig.getEdgePreconditionEvaluation().apply(edgeConfig.getScriptCache().get(jexlPreconditions))) {

                        if (log.isTraceEnabled()) {
                            log.trace("Time to evaluate event(-): " + (System.currentTimeMillis() - start) + "ms.");
                        }
                        continue;

                    } else {
                        matchingGroups = edgeConfig.getArithmeticMatchingGroups();
                        if (log.isTraceEnabled()) {
                            log.trace("Time to evaluate event(+): " + (System.currentTimeMillis() - start) + "ms.");
                        }

                    }
                }
            }

            // setup duration
            if (edgeDef.hasDuration()) {
                edgeEventFieldUtil.setEdgeDuration(edgeDef, baseEdgeBundle);
            }

            if (null != edgeDef.getEnrichmentField()) {
                if (edgeEventFieldUtil.getNormalizedFields().containsKey(edgeDef.getEnrichmentField())
                                && !(edgeEventFieldUtil.getNormalizedFields().get(edgeDef.getEnrichmentField()).isEmpty())) {
                    edgeDef.setEnrichmentEdge(true);
                } else {
                    edgeDef.setEnrichmentEdge(false);
                }
            }

            String sourceGroup = getEdgeDefGroup(edgeDef.getSourceFieldName());
            String sinkGroup = getEdgeDefGroup(edgeDef.getSinkFieldName());

            EdgeSourceSinkInfo edgeInfo = new EdgeSourceSinkInfo(mSource, mSink, sourceGroup, sinkGroup);

            boolean sameGroup = sourceGroup.equals(sinkGroup) && (sourceGroup != NO_GROUP);

            boolean ignorePreconditionMatchedGroups = !edgeDef.hasJexlPrecondition() || !edgeDef.isGroupAware()
                            || !(matchingGroups.containsKey(sourceGroup) || matchingGroups.containsKey(sinkGroup));

            baseEdgeBundle.setEdgeDefinition(edgeDef);

            // If within the same group, then pair each subgroup in common for both the sink and source
            if (sameGroup) {
                Set<String> commonKeys;

                if (ignorePreconditionMatchedGroups) {
                    // use all subgroups
                    commonKeys = mSource.keySet();
                    commonKeys.retainAll(mSink.keySet());
                } else {
                    // use only subgroups that matched the precondition
                    commonKeys = matchingGroups.get(sourceGroup);
                }
                edgeInfo.setCommonKeys(commonKeys);
            } else {
                // otherwise multiplex across subgroups
                Set<String> sourceSubGroups;
                Set<String> sinkSubGroups;

                if (ignorePreconditionMatchedGroups) {
                    // use all subgroups
                    sourceSubGroups = mSource.keySet();
                    sinkSubGroups = mSink.keySet();
                } else {
                    // use only subgroups that matched the precondition
                    sourceSubGroups = matchingGroups.get(sourceGroup) != null ? matchingGroups.get(sourceGroup) : mSource.keySet();
                    sinkSubGroups = matchingGroups.get(sinkGroup) != null ? matchingGroups.get(sinkGroup) : mSink.keySet();
                }
                edgeInfo.setSourceSubGroups(sourceSubGroups);
                edgeInfo.setSinkSubGroups(sinkSubGroups);
            }

            edgesCreated += generateEdges(baseEdgeBundle, edgeInfo, edgeEventFieldUtil, event, context, contextWriter);

        } // end edge defs

        if (this.enableMetadata) {
            writeMetadataMap(context, contextWriter, eventMetadataRegistry);
        }
        postProcessEdges(event, context, contextWriter, edgesCreated, baseEdgeBundle);

        return edgesCreated;
    }

    private long generateEdges(EdgeDataBundle baseEdgeBundle, EdgeSourceSinkInfo edgeInfo, EdgeEventFieldUtil edgeEventFieldUtil, RawRecordContainer event,
                    TaskInputOutputContext<KEYIN,? extends RawRecordContainer,KEYOUT,VALUEOUT> context, ContextWriter<KEYOUT,VALUEOUT> contextWriter)
                    throws IOException, InterruptedException {
        long edgesCreated = 0L;
        if (null != edgeInfo.getCommonKeys()) {
            edgesCreated += pairSubgroups(baseEdgeBundle, edgeInfo, event, edgeEventFieldUtil, context, contextWriter);
        } else {
            edgesCreated += multiplexSubgroups(baseEdgeBundle, edgeInfo, event, edgeEventFieldUtil, context, contextWriter);
        }

        return edgesCreated;
    }

    private boolean shortCircuit(RawRecordContainer event, String typeName, Map<String,EdgeDefinitionConfigurationHelper> edges) {
        if (event.fatalError()) {
            return true;
        }
        if (!edges.containsKey(typeName)) {
            return true;
        }
        return false;
    }

    /**
     *
     * We are using the intersection of 2 sets here to make sure we only loop over edges that we will create. Previously we would loop over all the possible
     * edges even if they were at different levels of nesting.
     *
     */
    private long pairSubgroups(EdgeDataBundle edgeDataBundle, EdgeSourceSinkInfo edgeInfo, RawRecordContainer event, EdgeEventFieldUtil fieldUtil,
                    TaskInputOutputContext<KEYIN,? extends RawRecordContainer,KEYOUT,VALUEOUT> context, ContextWriter<KEYOUT,VALUEOUT> contextWriter)
                    throws IOException, InterruptedException {

        long edgesCreated = 0L;

        Multimap<String,NormalizedContentInterface> mSource = edgeInfo.getSourceNCI();
        Multimap<String,NormalizedContentInterface> mSink = edgeInfo.getSinkNCI();
        Set<String> commonKeys = edgeInfo.getCommonKeys();

        for (String subGroup : commonKeys) {
            for (NormalizedContentInterface ifaceSource : mSource.get(subGroup)) {
                for (NormalizedContentInterface ifaceSink : mSink.get(subGroup)) {
                    if (ifaceSink == ifaceSource) {
                        continue;
                    }
                    EdgeDataBundle edgeValue = createEdge(edgeDataBundle, event, fieldUtil, ifaceSource, edgeInfo.getSourceGroup(), subGroup, ifaceSink,
                                    edgeInfo.getSinkGroup(), subGroup);
                    if (edgeValue != null) {

                        // have to write out the keys as the edge values are generated, so counters get updated
                        // and the system doesn't timeout.
                        edgesCreated += writeEdgesForDateType(edgeValue, context, contextWriter);

                        if (this.enableMetadata) {
                            registerEventMetadata(edgeValue, edgeDataBundle.getEdgeDefinition());

                        }
                    }

                }
            }
        }
        return edgesCreated;

    }

    private long multiplexSubgroups(EdgeDataBundle edgeDataBundle, EdgeSourceSinkInfo edgeInfo, RawRecordContainer event, EdgeEventFieldUtil fieldUtil,
                    TaskInputOutputContext<KEYIN,? extends RawRecordContainer,KEYOUT,VALUEOUT> context, ContextWriter<KEYOUT,VALUEOUT> contextWriter)
                    throws IOException, InterruptedException {

        long edgesCreated = 0L;

        Multimap<String,NormalizedContentInterface> mSource = edgeInfo.getSourceNCI();
        Multimap<String,NormalizedContentInterface> mSink = edgeInfo.getSinkNCI();
        Set<String> sourceSubGroups = edgeInfo.getSourceSubGroups();
        Set<String> sinkSubGroups = edgeInfo.getSinkSubGroups();

        for (String sourceSubGroup : sourceSubGroups) {
            for (NormalizedContentInterface ifaceSource : mSource.get(sourceSubGroup)) {
                for (String sinkSubGroup : sinkSubGroups) {
                    for (NormalizedContentInterface ifaceSink : mSink.get(sinkSubGroup)) {
                        if (ifaceSink == ifaceSource) {
                            continue;
                        }
                        EdgeDataBundle edgeValue = createEdge(edgeDataBundle, event, fieldUtil, ifaceSource, edgeInfo.getSourceGroup(), sourceSubGroup,
                                        ifaceSink, edgeInfo.getSinkGroup(), sinkSubGroup);
                        if (edgeValue != null) {

                            // have to write out the keys as the edge values are generated, so counters get updated
                            // and the system doesn't timeout.
                            edgesCreated += writeEdgesForDateType(edgeValue, context, contextWriter);

                            if (this.enableMetadata) {
                                registerEventMetadata(edgeValue, edgeDataBundle.getEdgeDefinition());

                            }
                        }

                    }
                }
            }

        }
        return edgesCreated;
    }

    private String getTypeName(Type dataType) {
        String typeName = dataType.typeName();
        String outputName = dataType.outputName();

        typeName = edgeConfig.getEdges().containsKey(outputName) ? outputName : typeName;

        return typeName;
    }

    private void setupPreconditionEvaluation(Multimap<String,NormalizedContentInterface> fields) {
        long start = System.currentTimeMillis();
        edgeConfig.getPreconditionContext().setFilteredContextForNormalizedContentInterface(fields);
        edgeConfig.getEdgePreconditionEvaluation().setJexlContext(edgeConfig.getPreconditionContext());
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

    protected void postProcessEdges(RawRecordContainer event, TaskInputOutputContext<KEYIN,? extends RawRecordContainer,KEYOUT,VALUEOUT> context,
                    ContextWriter<KEYOUT,VALUEOUT> contextWriter, long edgesCreated, EdgeDataBundle edgeDataBundle) throws IOException, InterruptedException {}

    protected EdgeDataBundle createEdge(EdgeDataBundle edgeDataBundle, RawRecordContainer event, EdgeEventFieldUtil fieldUtil,
                    NormalizedContentInterface ifaceSource, String sourceGroup, String sourceSubGroup, NormalizedContentInterface ifaceSink, String sinkGroup,
                    String sinkSubGroup) {

        EdgeDefinition edgeDef = edgeDataBundle.getEdgeDefinition();
        // Get the edge value

        VertexValue source = new VertexValue(edgeDef.isUseRealm(), edgeDef.getSourceIndexedFieldRealm(), edgeDef.getSourceEventFieldRealm(),
                        edgeDef.getSourceRelationship(), edgeDef.getSourceCollection(), ifaceSource);
        VertexValue sink = new VertexValue(edgeDef.isUseRealm(), edgeDef.getSinkIndexedFieldRealm(), edgeDef.getSourceEventFieldRealm(),
                        edgeDef.getSinkRelationship(), edgeDef.getSinkCollection(), ifaceSink);
        edgeDataBundle.setEdgeDefinition(edgeDef);
        edgeDataBundle.setSource(source);
        edgeDataBundle.setSink(sink);
        edgeDataBundle.initMarkings(ifaceSource.getMarkings(), ifaceSink.getMarkings());

        // if the edgeDef is an enrichment definition, fill in the enrichedValue
        if (edgeDef.isEnrichmentEdge()) {
            setEnrichmentInfo(edgeDef, edgeDataBundle, fieldUtil, sourceGroup, sourceSubGroup, sinkGroup, sinkSubGroup);
        }

        if (edgeDataBundle.requiresMasking()) {
            if (source.hasMaskedValue() || sink.hasMaskedValue()) {
                maskEdge(edgeDataBundle, event);
            }
        }

        // Validate the edge value
        if (!edgeDataBundle.isValid()) {
            return null;
        }

        // check value denylist
        if (edgeConfig.enableDenylist() && edgeConfig.isDenylistValue(edgeDataBundle.getDataTypeName(), edgeDataBundle.getSource().getValue(ValueType.INDEXED))
                        || edgeConfig.isDenylistValue(edgeDataBundle.getDataTypeName(), edgeDataBundle.getSink().getValue(ValueType.INDEXED))) {
            return null;
        }

        return edgeDataBundle;
    }

    private void setEnrichmentInfo(EdgeDefinition edgeDef, EdgeDataBundle edgeDataBundle, EdgeEventFieldUtil fieldUtil, String sourceGroup,
                    String sourceSubGroup, String sinkGroup, String sinkSubGroup) {
        Map<String,Map<String,String>> edgeTypeLookup = edgeConfig.getEdgeEnrichmentTypeLookup();
        if (edgeTypeLookup.containsKey(edgeDataBundle.getDataTypeName())) {
            // if the group is the same as the sink or source,
            // then ensure we use the correct subgroup, otherwise we will enrich with the first value found
            Collection<NormalizedContentInterface> ifaceEnrichs = fieldUtil.normalizedFields.get(edgeDef.getEnrichmentField());
            if (null != ifaceEnrichs && !ifaceEnrichs.isEmpty()) {
                String enrichGroup = getEdgeDefGroup(edgeDef.getEnrichmentField());
                if (enrichGroup != NO_GROUP) {
                    if (enrichGroup.equals(sourceGroup)) {
                        ifaceEnrichs = fieldUtil.depthFirstList.get(edgeDef.getEnrichmentField()).get(sourceSubGroup);
                    } else if (enrichGroup.equals(sinkGroup)) {
                        ifaceEnrichs = fieldUtil.depthFirstList.get(edgeDef.getEnrichmentField()).get(sinkSubGroup);
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
                        if (edgeTypeLookup.get(edgeDataBundle.getDataTypeName()).containsKey(enrichedIndex)) {
                            edgeDataBundle.setEnrichedIndex(enrichedIndex); // required for eventMetadataRegistry
                            edgeDataBundle.setEnrichedValue(edgeTypeLookup.get(edgeDataBundle.getDataTypeName()).get(enrichedIndex));
                            break;
                        }
                    }
                }

            }
        } else {
            log.error("Cannot enrich edge because Enrichment Edge Types are not defined for data type: " + edgeDataBundle.getDataTypeName());

        }
        if (null != edgeDataBundle.getEnrichedValue()) {
            edgeDataBundle.setEdgeType(edgeDataBundle.getEnrichedValue());
        }
    }

    protected void maskEdge(EdgeDataBundle edgeDataBundle, RawRecordContainer record) {}

    protected void registerEventMetadata(EdgeDataBundle edgeValue, EdgeDefinition edgeDef) {
        String enrichmentFieldName = getEnrichmentFieldName(edgeDef);
        String jexlPrecondition = edgeDef.getJexlPrecondition();

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
    protected long writeEdgesForDateType(EdgeDataBundle value, TaskInputOutputContext<KEYIN,? extends RawRecordContainer,KEYOUT,VALUEOUT> context,
                    ContextWriter<KEYOUT,VALUEOUT> contextWriter) throws IOException, InterruptedException {

        long edgesCreated = 0;
        EdgeKey.DATE_TYPE dateType = value.getDateType();
        // write edges for this specific date type
        edgesCreated += writeEdges(value, context, contextWriter, dateType);
        // activity only also historically generated event only edges, so maintain that here
        if (dateType.equals(EdgeKey.DATE_TYPE.ACTIVITY_ONLY)) {
            edgesCreated += writeEdges(value, context, contextWriter, EdgeKey.DATE_TYPE.EVENT_ONLY);
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

    @Override
    public RawRecordMetadata getMetadata() {
        // TODO Auto-generated method stub
        return null;
    }

    private class EdgeSourceSinkInfo {
        Multimap<String,NormalizedContentInterface> mSource;
        Multimap<String,NormalizedContentInterface> mSink;
        String sourceGroup;
        String sinkGroup;
        Set<String> commonKeys;
        Set<String> sourceSubGroups;
        Set<String> sinkSubGroups;

        private EdgeSourceSinkInfo(Multimap<String,NormalizedContentInterface> mSource, Multimap<String,NormalizedContentInterface> mSink, String sourceGroup,
                        String sinkGroup) {
            this.mSource = mSource;
            this.mSink = mSink;
            this.sourceGroup = sourceGroup;
            this.sinkGroup = sinkGroup;
        }

        private Multimap<String,NormalizedContentInterface> getSourceNCI() {
            return this.mSource;
        }

        private Multimap<String,NormalizedContentInterface> getSinkNCI() {
            return this.mSink;
        }

        private String getSourceGroup() {
            return this.sourceGroup;
        }

        private String getSinkGroup() {
            return this.sinkGroup;
        }

        public void setCommonKeys(Set<String> commonKeys) {
            this.commonKeys = commonKeys;
        }

        public Set<String> getCommonKeys() {
            return this.commonKeys;
        }

        public void setSourceSubGroups(Set<String> sourceSubGroups) {
            this.sourceSubGroups = sourceSubGroups;
        }

        public Set<String> getSourceSubGroups() {
            return this.sourceSubGroups;
        }

        public void setSinkSubGroups(Set<String> sinkSubGroups) {
            this.sinkSubGroups = sinkSubGroups;
        }

        public Set<String> getSinkSubGroups() {
            return this.sinkSubGroups;
        }

    }

}
