package datawave.ingest.mapreduce.handler.edge;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import datawave.data.hash.UID;
import datawave.data.normalizer.DateNormalizer;
import datawave.ingest.config.RawRecordContainerImpl;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.config.BaseNormalizedContent;
import datawave.ingest.data.config.GroupedNormalizedContentInterface;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.ingest.BaseIngestHelper;
import datawave.ingest.data.config.ingest.FakeIngestHelper;
import datawave.ingest.mapreduce.SimpleDataTypeHandler;
import datawave.ingest.mapreduce.handler.ExtendedDataTypeHandler;
import datawave.ingest.mapreduce.handler.edge.define.EdgeDataBundle;
import datawave.ingest.mapreduce.handler.edge.define.EdgeDefinition;
import datawave.ingest.mapreduce.handler.edge.define.EdgeDefinitionConfigurationHelper;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.writer.AbstractContextWriter;
import datawave.ingest.mapreduce.job.writer.ContextWriter;
import datawave.ingest.test.StandaloneStatusReporter;
import datawave.ingest.test.StandaloneTaskAttemptContext;
import datawave.ingest.time.Now;
import datawave.metadata.protobuf.EdgeMetadata;
import datawave.util.TypeRegistryTestSetup;
import datawave.util.TableName;
import datawave.util.time.DateHelper;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.log4j.Appender;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeSet;

public class ProtobufEdgeDeleteModeTest {
    
    private Configuration conf;
    private static Path edgeKeyVersionCachePath = Paths.get(System.getProperty("user.dir"), "edge-key-version.txt");
    private static Logger log = Logger.getLogger(ProtobufEdgeDeleteModeTest.class);
    private static Enumeration rootAppenders = Logger.getRootLogger().getAllAppenders();
    private static Multimap<String,NormalizedContentInterface> fields = HashMultimap.create();
    private static Type type = new Type("mycsv", FakeIngestHelper.class, null, new String[] {SimpleDataTypeHandler.class.getName()}, 10, null);
    private static final Now now = Now.getInstance();
    
    @BeforeClass
    public static void setupSystemSettings() throws Exception {
        
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        System.setProperty("file.encoding", "UTF8");
        // Set edge version cache file...
        try {
            Files.write(edgeKeyVersionCachePath, "1\t1970-01-01T00:00:00.000Z".getBytes());
        } catch (IOException io) {
            log.fatal("Could not create " + edgeKeyVersionCachePath);
        }
        
        fields.put("fileExtension", new BaseNormalizedContent("fileExtension", "gz"));
        fields.put("lastModified", new BaseNormalizedContent("lastModified", "2016-01-01"));
        fields.put("LANGUAGE", new BaseNormalizedContent("LANGUAGE", "NONE"));
        fields.put("EVENT_DATE", new BaseNormalizedContent("EVENT_DATE", "2016-04-26T01:31:53Z"));
        fields.put("SHA1", new BaseNormalizedContent("SHA1", "b7472ce04163d18089f05b6a0dc5dffe65f2c9a6"));
        fields.put("FIELDNAME1_WE_DONT_WANT_INDEXED", new BaseNormalizedContent("FIELDNAME1_WE_DONT_WANT_INDEXED", "VALUE1_OF_FIELDNAME1_WE_DONT_WANT_INDEXED"));
        fields.put("BAR_FIELD", new BaseNormalizedContent("BAR_FIELD", "MYBAR"));
        fields.put("SECURITY_MARKING", new BaseNormalizedContent("SECURITY_MARKING", "PUBLIC"));
        fields.put("EDGE_VERTEX_TO", new BaseNormalizedContent("EDGE_VERTEX_TO", "VERTEX3"));
        fields.put("SHA256", new BaseNormalizedContent("SHA256", "873e26c91c968525c86690772bdb044d398a51847846944ac8c5109fe732d690"));
        fields.put("PROCESSING_DATE", new BaseNormalizedContent("PROCESSING_DATE", "2016-04-26 03:00:00"));
        fields.put("R1_FIELD2", new BaseNormalizedContent("R1_FIELD2", "R1_FIELD2_VALUE"));
        fields.put("PROCESSED_SIZE", new BaseNormalizedContent("PROCESSED_SIZE", "268"));
        fields.put("EDGE_VERTEX_FROM", new BaseNormalizedContent("EDGE_VERTEX_FROM", "VERTEX1"));
        fields.put("MY_DATE", new BaseNormalizedContent("MY_DATE", "R1_FIELD2_VALU"));
        fields.put("FOO_FIELD", new BaseNormalizedContent("FOO_FIELD", "MYFOO"));
        fields.put("Summary", new BaseNormalizedContent("Summary", "\\~n~THIS IS THE SUMMARY TEXT"));
        fields.put("EVENT_ID", new BaseNormalizedContent("EVENT_ID", "trrn.n2016117aamy.0000000179"));
        fields.put("R1_FIELD1", new BaseNormalizedContent("R1_FIELD1", "R1_FIELD1_VALUE"));
        fields.put("FILE_TYPE", new BaseNormalizedContent("FILE_TYPE", "MIME"));
        fields.put("UUID", new BaseNormalizedContent("UUID", "0016dd72-0000-827d-dd4d-001b2163ba09"));
        fields.put("ORIGINAL_SIZE", new BaseNormalizedContent("ORIGINAL_SIZE", "3173"));
        fields.put("MD5", new BaseNormalizedContent("MD5", "715555845289dd6ba0f4cbb8a02e5052"));
    }
    
    @AfterClass
    public static void tearDown() {
        Logger.getRootLogger().removeAllAppenders();
        while (rootAppenders.hasMoreElements()) {
            Appender appender = (Appender) rootAppenders.nextElement();
            Logger.getRootLogger().addAppender(appender);
        }
        try {
            Files.deleteIfExists(edgeKeyVersionCachePath);
        } catch (IOException io) {
            log.error("Could not delete " + edgeKeyVersionCachePath);
        }
    }
    
    @Before
    public void setup() {
        conf = new Configuration();
        conf.addResource(ClassLoader.getSystemResource("config/all-config.xml"));
        conf.addResource(ClassLoader.getSystemResource("config/edge-ingest-config.xml"));
        conf.addResource(ClassLoader.getSystemResource("config/metadata-config.xml"));
        TypeRegistryTestSetup.resetTypeRegistryWithTypes(conf, type);
    }
    
    private RawRecordContainer getEvent(Configuration conf) {
        
        RawRecordContainerImpl myEvent = new RawRecordContainerImpl();
        myEvent.addSecurityMarking("columnVisibility", "PRIVATE");
        myEvent.setDataType(type);
        myEvent.setId(UID.builder().newId());
        myEvent.setConf(conf);
        
        Instant i = Instant.from(DateTimeFormatter.ISO_INSTANT.parse("2016-04-26T01:31:53Z"));
        myEvent.setDate(i.getEpochSecond());
        return myEvent;
    }
    
    @Test
    public void testEdgeKeyDeleteModeSetViaHelper() throws Exception {
        log.debug("---testHelperDeleteMode---");
        
        //
        // check that the default value for edge delete mode is FALSE
        //
        
        ProtobufEdgeDataTypeHandler<Text,BulkIngestKey,Value> edgeHandler = new ProtobufEdgeDataTypeHandler<>();
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        edgeHandler.setup(context);
        
        RawRecordContainer myEvent = getEvent(conf);
        
        HandlerTestUtil.processEvent(fields, edgeHandler, myEvent, 4, true, false);
        
        //
        // Have the data type helper handle our delete configuration...
        //
        
        // clear out the cached ingest helper
        type.clearIngestHelper();
        
        conf.set(BaseIngestHelper.INGEST_MODE_DELETE, "true");
        
        edgeHandler = new ProtobufEdgeDataTypeHandler<>();
        context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        edgeHandler.setup(context);
        
        myEvent = getEvent(conf);
        
        HandlerTestUtil.processEvent(fields, edgeHandler, myEvent, 4, true, true);
    }
    
    @Test
    public void testIngestJobDeleteProperty() throws Exception {
        log.debug("---testIngestJobDeleteModeProperty---");
        
        //
        // Build a derived handler that picks up the delete config directly
        // Check that the default value for edge delete mode is FALSE
        //
        
        // Set up the edge
        MyDerivedProtobufEdgeDataTypeHandler<Text,BulkIngestKey,Value> edgeHandler = new MyDerivedProtobufEdgeDataTypeHandler<>();
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        edgeHandler.setup(context);
        
        RawRecordContainer myEvent = getEvent(conf);
        
        HandlerTestUtil.processEvent(fields, edgeHandler, myEvent, 4, true, false);
        
        //
        // Order delete mode via conf property
        //
        
        conf.set("command.line.injected.delete.mode.property", "true");
        context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        
        edgeHandler = new MyDerivedProtobufEdgeDataTypeHandler<>();
        edgeHandler.setup(context);
        
        myEvent = getEvent(conf);
        
        HandlerTestUtil.processEvent(fields, edgeHandler, myEvent, 4, true, true);
        
        //
        // set conf injected value to false
        //
        
        conf.set("command.line.injected.delete.mode.property", "false");
        context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        
        edgeHandler = new MyDerivedProtobufEdgeDataTypeHandler<>();
        edgeHandler.setup(context);
        
        myEvent = getEvent(conf);
        
        HandlerTestUtil.processEvent(fields, edgeHandler, myEvent, 4, true, false);
        
    }
    
    /**
     * A class derived from ProtobufEdgeDataTypeHandler to demonstrate integrating delete mode from a parameter passed via job command line. ß
     */
    class MyDerivedProtobufEdgeDataTypeHandler<KEYIN,KEYOUT,VALUEOUT> extends ProtobufEdgeDataTypeHandler<KEYIN,KEYOUT,VALUEOUT> {
        
        private Boolean deleteMode = false;
        
        // capture the property during setup
        @Override
        public void setup(TaskAttemptContext context) {
            super.setup(context);
            this.deleteMode = context.getConfiguration().get("command.line.injected.delete.mode.property", "false").equals("true");
        }
        
        // a stripped-down handler retaining a minimum capability to demonstrate setting
        // delete mode on each EdgeDataBundle, referencing a command line property (see 'NOTE:' comments below)
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
            Type dataType = event.getDataType();
            String typeName = dataType.typeName();
            List<EdgeDefinition> edgeDefs = null;
            EdgeDefinitionConfigurationHelper edgeDefConfigs = null;
            if (!edges.containsKey(typeName)) {
                return edgesCreated; // short circuit, no edges defined for this type
            }
            edgeDefConfigs = (EdgeDefinitionConfigurationHelper) edges.get(typeName);
            edgeDefs = edgeDefConfigs.getEdges();
            
            loadDateStr = DateHelper.format(new Date(now.get()));
            
            /*
             * normalize field names with groups
             */
            Multimap<String,NormalizedContentInterface> normalizedFields = HashMultimap.create();
            Map<String,Multimap<String,NormalizedContentInterface>> depthFirstList = new HashMap<>();
            Multimap<String,NormalizedContentInterface> tmp = null;
            for (Map.Entry<String,NormalizedContentInterface> e : fields.entries()) {
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
            Map<Key,Set<EdgeMetadata.MetadataValue.Metadata>> eventMetadataRegistry = new HashMap<>();
            
            activityLog = new HashSet<>();
            durationLog = new HashSet<>();
            
            /*
             * Create Edge Values from Edge Definitions
             */
            for (EdgeDefinition edgeDef : edgeDefs) {
                
                String enrichmentFieldName = getEnrichmentFieldName(edgeDef);
                String jexlPreconditions = null;
                
                if (null != edgeDef.getEnrichmentField()) {
                    if (normalizedFields.containsKey(edgeDef.getEnrichmentField()) && !(normalizedFields.get(edgeDef.getEnrichmentField()).isEmpty())) {
                        edgeDef.setEnrichmentEdge(true);
                    } else {
                        edgeDef.setEnrichmentEdge(false);
                    }
                }
                
                Multimap<String,NormalizedContentInterface> mSource = null;
                Multimap<String,NormalizedContentInterface> mSink = null;
                
                String sourceGroup = getGroup(edgeDef.getSourceFieldName());
                String sinkGroup = getGroup(edgeDef.getSinkFieldName());
                
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
                
                // NOTE: A simple test case that supports only the test edge configuration...
                for (String sourceSubGroup : mSource.keySet()) {
                    for (NormalizedContentInterface ifaceSource : mSource.get(sourceSubGroup)) {
                        for (String sinkSubGroup : mSink.keySet()) {
                            for (NormalizedContentInterface ifaceSink : mSink.get(sinkSubGroup)) {
                                EdgeDataBundle edgeValue = createEdge(edgeDef, event, ifaceSource, sourceGroup, sourceSubGroup, ifaceSink, sinkGroup,
                                                sinkSubGroup, edgeAttribute2, edgeAttribute3, normalizedFields, depthFirstList, loadDateStr, activityDate,
                                                validActivityDate);
                                if (edgeValue != null) {
                                    
                                    // NOTE: The only difference in this test class
                                    edgeValue.setIsDeleting(this.deleteMode);
                                    
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
            } // end edge defs
            
            return edgesCreated;
        }
        
    }
    
    public static class HandlerTestUtil {
        
        public static final Text edgeTableName = new Text(TableName.EDGE);
        public static final String NB = "\u0000";
        
        private static Logger log = Logger.getLogger(HandlerTestUtil.class);
        
        public static boolean isDocumentKey(Key k) {
            return isShardKey(k) && k.getColumnFamily().toString().equals(ExtendedDataTypeHandler.FULL_CONTENT_COLUMN_FAMILY);
        }
        
        public static boolean isShardKey(Key k) {
            return k.getRow().toString().matches("\\d{8}_\\d+");
        }
        
        public static void processEvent(Multimap<String,NormalizedContentInterface> eventFields, ExtendedDataTypeHandler<Text,BulkIngestKey,Value> edgeHandler,
                        RawRecordContainer event, int expectedEdgeKeys, boolean printKeysOnlyOnFail, boolean edgeDeleteMode) {
            
            Assert.assertNotNull("Event was null.", event);
            Set<Key> edgeKeys = new HashSet<>();
            Map<Text,Integer> countMap = Maps.newHashMap();
            
            // Process edges
            countMap.put(edgeTableName, 0);
            if (null != edgeHandler) {
                HandlerTestUtil.MyCachingContextWriter contextWriter = new HandlerTestUtil.MyCachingContextWriter();
                StandaloneTaskAttemptContext<Text,RawRecordContainerImpl,BulkIngestKey,Value> ctx = new StandaloneTaskAttemptContext<>(
                                ((RawRecordContainerImpl) event).getConf(), new StandaloneStatusReporter());
                
                try {
                    contextWriter.setup(ctx.getConfiguration(), false);
                    edgeHandler.process(null, event, eventFields, ctx, contextWriter);
                    contextWriter.commit(ctx);
                    for (Map.Entry<BulkIngestKey,Value> entry : contextWriter.getCache().entries()) {
                        if (entry.getKey().getTableName().equals(edgeTableName)) {
                            edgeKeys.add(entry.getKey().getKey());
                        }
                        if (!entry.getKey().getTableName().equals(edgeTableName) || entry.getKey().getKey().isDeleted() == edgeDeleteMode) {
                            if (countMap.containsKey(entry.getKey().getTableName())) {
                                countMap.put(entry.getKey().getTableName(), countMap.get(entry.getKey().getTableName()) + 1);
                            } else {
                                countMap.put(entry.getKey().getTableName(), 1);
                            }
                        }
                    }
                } catch (Throwable t) {
                    log.error("Error during edge processing", t);
                    throw new RuntimeException(t);
                }
            }
            
            Set<String> keyPrint = new TreeSet<>();
            
            // check edge keys
            for (Key k : edgeKeys) {
                keyPrint.add("edge key: " + k.getRow().toString().replaceAll(NB, "%00;") + " ::: " + k.getColumnFamily().toString().replaceAll(NB, "%00;")
                                + " ::: " + k.getColumnQualifier().toString().replaceAll(NB, "%00;") + " ::: " + k.getColumnVisibility() + " ::: "
                                + k.getTimestamp() + " ::: " + k.isDeleted() + "\n");
            }
            
            try {
                if (!printKeysOnlyOnFail) {
                    for (String keyString : keyPrint) {
                        log.info(keyString.trim());
                    }
                }
                Assert.assertEquals((int) countMap.get(edgeTableName), expectedEdgeKeys);
            } catch (AssertionError ae) {
                if (printKeysOnlyOnFail) {
                    for (String keyString : keyPrint) {
                        log.info(keyString.trim());
                    }
                }
                final Text shardTableName = new Text(TableName.SHARD);
                Assert.fail(String.format("Expected: %s edge keys.\nFound: %s", expectedEdgeKeys, countMap.get(shardTableName), countMap.get(edgeTableName)));
            }
        }
        
        private static class MyCachingContextWriter extends AbstractContextWriter<BulkIngestKey,Value> {
            private Multimap<BulkIngestKey,Value> cache = HashMultimap.create();
            
            @Override
            protected void flush(Multimap<BulkIngestKey,Value> entries, TaskInputOutputContext<?,?,BulkIngestKey,Value> context) throws IOException,
                            InterruptedException {
                for (Map.Entry<BulkIngestKey,Value> entry : entries.entries()) {
                    cache.put(entry.getKey(), entry.getValue());
                }
            }
            
            public Multimap<BulkIngestKey,Value> getCache() {
                return cache;
            }
        }
    }
    
}
