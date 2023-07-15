package datawave.ingest.mapreduce.handler.edge;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.data.hash.UID;
import datawave.data.normalizer.DateNormalizer;
import datawave.ingest.config.RawRecordContainerImpl;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.BaseNormalizedContent;
import datawave.ingest.data.config.GroupedNormalizedContentInterface;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.ingest.BaseIngestHelper;
import datawave.ingest.data.config.ingest.FakeIngestHelper;
import datawave.ingest.mapreduce.SimpleDataTypeHandler;
import datawave.ingest.mapreduce.handler.edge.define.EdgeDataBundle;
import datawave.ingest.mapreduce.handler.edge.define.EdgeDefinition;
import datawave.ingest.mapreduce.handler.edge.define.EdgeDefinitionConfigurationHelper;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.writer.ContextWriter;
import datawave.ingest.time.Now;
import datawave.metadata.protobuf.EdgeMetadata;
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
        TypeRegistry.reset();
        conf = new Configuration();
        conf.addResource(ClassLoader.getSystemResource("config/all-config.xml"));
        conf.addResource(ClassLoader.getSystemResource("config/edge-ingest-config.xml"));
        conf.addResource(ClassLoader.getSystemResource("config/metadata-config.xml"));
        TypeRegistry registry = TypeRegistry.getInstance(conf);
        registry.put(type.typeName(), type);
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
        
        EdgeHandlerTestUtil.processEvent(fields, edgeHandler, myEvent, 4, true, false);
        
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
        
        EdgeHandlerTestUtil.processEvent(fields, edgeHandler, myEvent, 4, true, true);
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
        
        EdgeHandlerTestUtil.processEvent(fields, edgeHandler, myEvent, 4, true, false);
        
        //
        // Order delete mode via conf property
        //
        
        conf.set("ingest.mode.delete", "true");
        context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        
        edgeHandler = new MyDerivedProtobufEdgeDataTypeHandler<>();
        edgeHandler.setup(context);
        
        myEvent = getEvent(conf);
        
        EdgeHandlerTestUtil.processEvent(fields, edgeHandler, myEvent, 4, true, true);
        
        //
        // set conf injected value to false
        //
        
        conf.set("ingest.mode.delete", "false");
        context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        
        edgeHandler = new MyDerivedProtobufEdgeDataTypeHandler<>();
        edgeHandler.setup(context);
        
        myEvent = getEvent(conf);
        
        EdgeHandlerTestUtil.processEvent(fields, edgeHandler, myEvent, 4, true, false);
        
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
            // setup the helper again to force the current delete mode
            this.getHelper(event.getDataType()).setup(context.getConfiguration());
            // this method used to be some partial copypasta out of the real ProtobufedgeDTH but that seemed really error prone to me and it made me mad so I
            // removed it
            return super.process(key, event, fields, context, contextWriter);
        }
        
    }
    
}
