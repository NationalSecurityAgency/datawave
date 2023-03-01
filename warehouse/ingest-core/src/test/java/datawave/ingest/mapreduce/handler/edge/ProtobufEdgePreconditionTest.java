package datawave.ingest.mapreduce.handler.edge;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.data.hash.UID;
import datawave.ingest.config.RawRecordContainerImpl;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.BaseNormalizedContent;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;
import datawave.ingest.data.config.ingest.FakeIngestHelper;
import datawave.ingest.mapreduce.SimpleDataTypeHandler;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.time.Now;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

import static datawave.ingest.mapreduce.handler.edge.EdgeKeyVersioningCache.KEY_VERSION_CACHE_DIR;
import static datawave.ingest.mapreduce.handler.edge.EdgeKeyVersioningCache.KEY_VERSION_DIST_CACHE_DIR;

public class ProtobufEdgePreconditionTest {
    
    private static Logger log = Logger.getLogger(ProtobufEdgeDeleteModeTest.class);
    private static Enumeration rootAppenders = Logger.getRootLogger().getAllAppenders();
    private static Multimap<String,NormalizedContentInterface> fields = HashMultimap.create();
    private static Type type = new Type("mycsv", FakeIngestHelper.class, null, new String[] {SimpleDataTypeHandler.class.getName()}, 10, null);
    private static final Now now = Now.getInstance();
    private Configuration conf;
    
    @Before
    public void setup() {
        TypeRegistry.reset();
        conf = new Configuration();
        conf.addResource(ClassLoader.getSystemResource("config/all-config.xml"));
        conf.addResource(ClassLoader.getSystemResource("config/edge-ingest-config.xml"));
        conf.addResource(ClassLoader.getSystemResource("config/metadata-config.xml"));
        conf.setBoolean(ProtobufEdgeDataTypeHandler.EVALUATE_PRECONDITIONS, true);
        conf.set(ProtobufEdgeDataTypeHandler.EDGE_SPRING_CONFIG, "config/EdgeSpringConfigPrecon.xml");
        conf.set(KEY_VERSION_CACHE_DIR, ClassLoader.getSystemResource("config").getPath());
        conf.set(KEY_VERSION_DIST_CACHE_DIR, ClassLoader.getSystemResource("config").getPath());
        
        TypeRegistry registry = TypeRegistry.getInstance(conf);
        registry.put(type.typeName(), type);
        
        fields.clear();
        EdgeHandlerTestUtil.edgeKeyResults.clear();
    }
    
    private RawRecordContainer getEvent(Configuration conf) {
        
        RawRecordContainerImpl myEvent = new RawRecordContainerImpl();
        myEvent.addSecurityMarking("columnVisibility", "PRIVATE");
        myEvent.setDataType(type);
        myEvent.setId(UID.builder().newId());
        myEvent.setConf(conf);
        
        Instant i = Instant.from(DateTimeFormatter.ISO_INSTANT.parse("2022-10-26T01:31:53Z"));
        myEvent.setDate(i.getEpochSecond());
        
        return myEvent;
    }
    
    @Test
    public void testUnawarePrecon() {
        // FELINE == 'tabby'
        
        fields.put("EVENT_DATE", new BaseNormalizedContent("EVENT_DATE", "2022-10-26T01:31:53Z"));
        fields.put("UUID", new BaseNormalizedContent("UUID", "0016dd72-0000-827d-dd4d-001b2163ba09"));
        fields.put("FELINE", new NormalizedFieldAndValue("FELINE", "tabby", "PET", "0"));
        fields.put("FELINE", new NormalizedFieldAndValue("FELINE", "siamese", "PET", "1"));
        fields.put("FISH", new NormalizedFieldAndValue("FISH", "salmon", "PET", "0"));
        fields.put("FISH", new NormalizedFieldAndValue("FISH", "guppy", "PET", "1"));
        fields.put("ACTIVITY", new NormalizedFieldAndValue("ACTIVITY", "fetch", "THING", "0"));
        
        ProtobufEdgeDataTypeHandler<Text,BulkIngestKey,Value> edgeHandler = new ProtobufEdgeDataTypeHandler<>();
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        edgeHandler.setup(context);
        
        Set<String> expectedKeys = new HashSet<>();
        expectedKeys.add("guppy");
        expectedKeys.add("guppy%00;siamese");
        expectedKeys.add("salmon");
        expectedKeys.add("salmon%00;tabby");
        expectedKeys.add("siamese");
        expectedKeys.add("siamese%00;guppy");
        expectedKeys.add("tabby");
        expectedKeys.add("tabby%00;salmon");
        
        RawRecordContainer myEvent = getEvent(conf);
        
        EdgeHandlerTestUtil.processEvent(fields, edgeHandler, myEvent, 8, true, false);
        Assert.assertEquals(expectedKeys, EdgeHandlerTestUtil.edgeKeyResults);
        
    }
    
    @Test
    public void testAwarePreconSameGroup() {
        // CANINE == 'shepherd'
        
        fields.put("EVENT_DATE", new BaseNormalizedContent("EVENT_DATE", "2022-10-26T01:31:53Z"));
        fields.put("UUID", new BaseNormalizedContent("UUID", "0016dd72-0000-827d-dd4d-001b2163ba09"));
        fields.put("CANINE", new NormalizedFieldAndValue("CANINE", "shepherd", "PET", "0"));
        fields.put("CANINE", new NormalizedFieldAndValue("CANINE", "bernese", "PET", "1"));
        fields.put("FISH", new NormalizedFieldAndValue("FISH", "salmon", "PET", "0"));
        fields.put("FISH", new NormalizedFieldAndValue("FISH", "guppy", "PET", "1"));
        
        ProtobufEdgeDataTypeHandler<Text,BulkIngestKey,Value> edgeHandler = new ProtobufEdgeDataTypeHandler<>();
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        edgeHandler.setup(context);
        
        Set<String> expectedKeys = new HashSet<>();
        expectedKeys.add("shepherd");
        expectedKeys.add("shepherd%00;salmon");
        expectedKeys.add("salmon");
        expectedKeys.add("salmon%00;shepherd");
        
        RawRecordContainer myEvent = getEvent(conf);
        
        EdgeHandlerTestUtil.processEvent(fields, edgeHandler, myEvent, 4, true, false);
        Assert.assertEquals(expectedKeys, EdgeHandlerTestUtil.edgeKeyResults);
        
    }
    
    @Test
    public void testAwarePreconDifferentGroup() {
        // CANINE == 'shepherd'
        
        fields.put("EVENT_DATE", new BaseNormalizedContent("EVENT_DATE", "2022-10-26T01:31:53Z"));
        fields.put("UUID", new BaseNormalizedContent("UUID", "0016dd72-0000-827d-dd4d-001b2163ba09"));
        fields.put("CANINE", new NormalizedFieldAndValue("CANINE", "shepherd", "PET", "0"));
        fields.put("CANINE", new NormalizedFieldAndValue("CANINE", "bernese", "PET", "1"));
        fields.put("ACTIVITY", new NormalizedFieldAndValue("ACTIVITY", "fetch", "THING", "0"));
        fields.put("ACTIVITY", new NormalizedFieldAndValue("ACTIVITY", "swim", "THING", "1"));
        
        ProtobufEdgeDataTypeHandler<Text,BulkIngestKey,Value> edgeHandler = new ProtobufEdgeDataTypeHandler<>();
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        edgeHandler.setup(context);
        
        Set<String> expectedKeys = new HashSet<>();
        expectedKeys.add("shepherd");
        expectedKeys.add("shepherd%00;fetch");
        expectedKeys.add("shepherd%00;swim");
        expectedKeys.add("fetch");
        expectedKeys.add("fetch%00;shepherd");
        expectedKeys.add("swim%00;shepherd");
        expectedKeys.add("swim");
        
        RawRecordContainer myEvent = getEvent(conf);
        
        EdgeHandlerTestUtil.processEvent(fields, edgeHandler, myEvent, 7, true, false);
        Assert.assertEquals(expectedKeys, EdgeHandlerTestUtil.edgeKeyResults);
        
    }
    
    @Test
    public void testAwareFieldComparison() {
        // PART == SOUND
        
        fields.put("EVENT_DATE", new BaseNormalizedContent("EVENT_DATE", "2022-10-26T01:31:53Z"));
        fields.put("UUID", new BaseNormalizedContent("UUID", "0016dd72-0000-827d-dd4d-001b2163ba09"));
        fields.put("PART", new NormalizedFieldAndValue("PART", "bark", "TREE", "0"));
        fields.put("SPECIES", new NormalizedFieldAndValue("SPECIES", "spruce", "TREE", "0"));
        fields.put("SOUND", new NormalizedFieldAndValue("SOUND", "meow", "ANIMAL", "0"));
        fields.put("SPECIES", new NormalizedFieldAndValue("SPECIES", "feline", "ANIMAL", "0"));
        fields.put("SOUND", new NormalizedFieldAndValue("SOUND", "bark", "ANIMAL", "1"));
        fields.put("SPECIES", new NormalizedFieldAndValue("SPECIES", "canine", "ANIMAL", "1"));
        
        ProtobufEdgeDataTypeHandler<Text,BulkIngestKey,Value> edgeHandler = new ProtobufEdgeDataTypeHandler<>();
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        edgeHandler.setup(context);
        
        Set<String> expectedKeys = new HashSet<>();
        expectedKeys.add("canine%00;spruce");
        expectedKeys.add("spruce%00;canine");
        expectedKeys.add("canine");
        expectedKeys.add("spruce");
        ;
        
        RawRecordContainer myEvent = getEvent(conf);
        
        EdgeHandlerTestUtil.processEvent(fields, edgeHandler, myEvent, 4, true, false);
        Assert.assertEquals(expectedKeys, EdgeHandlerTestUtil.edgeKeyResults);
        
    }
    
    @Test
    public void testAwareFieldComparisonNullCheck() {
        // PART == SOUND
        
        fields.put("EVENT_DATE", new BaseNormalizedContent("EVENT_DATE", "2022-10-26T01:31:53Z"));
        fields.put("UUID", new BaseNormalizedContent("UUID", "0016dd72-0000-827d-dd4d-001b2163ba09"));
        fields.put("SPECIES", new NormalizedFieldAndValue("SPECIES", "spruce", "TREE", "0"));
        fields.put("SPECIES", new NormalizedFieldAndValue("SPECIES", "feline", "ANIMAL", "0"));
        fields.put("SPECIES", new NormalizedFieldAndValue("SPECIES", "canine", "ANIMAL", "1"));
        
        ProtobufEdgeDataTypeHandler<Text,BulkIngestKey,Value> edgeHandler = new ProtobufEdgeDataTypeHandler<>();
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        edgeHandler.setup(context);
        
        Set<String> expectedKeys = new HashSet<>();
        
        RawRecordContainer myEvent = getEvent(conf);
        
        EdgeHandlerTestUtil.processEvent(fields, edgeHandler, myEvent, 0, true, false);
        Assert.assertEquals(expectedKeys, EdgeHandlerTestUtil.edgeKeyResults);
        
    }
    
    @Test
    public void testAwareOrGroupsNotEqual() {
        // SAE_GRADE == '5W_30' || SAE_GRADE == '5W_40'
        
        fields.put("EVENT_DATE", new BaseNormalizedContent("EVENT_DATE", "2022-10-26T01:31:53Z"));
        fields.put("UUID", new BaseNormalizedContent("UUID", "0016dd72-0000-827d-dd4d-001b2163ba09"));
        fields.put("SAE_GRADE", new NormalizedFieldAndValue("SAE_GRADE", "5W_30", "OIL", "0"));
        fields.put("ENGINE", new NormalizedFieldAndValue("ENGINE", "3.0L Boxer 6", "CAR", "0"));
        fields.put("MAKE", new NormalizedFieldAndValue("MAKE", "Subaru", "CAR", "0"));
        
        fields.put("SAE_GRADE", new NormalizedFieldAndValue("SAE_GRADE", "5W_40", "OIL", "1"));
        fields.put("ENGINE", new NormalizedFieldAndValue("ENGINE", "3.0L V6", "CAR", "1"));
        fields.put("MAKE", new NormalizedFieldAndValue("MAKE", "Audi", "CAR", "1"));
        
        fields.put("SAE_GRADE", new NormalizedFieldAndValue("SAE_GRADE", "10W_30", "OIL", "2"));
        fields.put("ENGINE", new NormalizedFieldAndValue("ENGINE", "993cc EFI", "MOWER", "2"));
        fields.put("MAKE", new NormalizedFieldAndValue("MAKE", "Wright", "MOWER", "2"));
        
        ProtobufEdgeDataTypeHandler<Text,BulkIngestKey,Value> edgeHandler = new ProtobufEdgeDataTypeHandler<>();
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        edgeHandler.setup(context);
        
        Set<String> expectedKeys = new HashSet<>();
        expectedKeys.add("3.0L Boxer 6%00;Subaru");
        expectedKeys.add("3.0L V6%00;Audi");
        expectedKeys.add("Subaru%00;3.0L Boxer 6");
        expectedKeys.add("Audi%00;3.0L V6");
        expectedKeys.add("3.0L Boxer 6");
        expectedKeys.add("3.0L V6");
        expectedKeys.add("Audi");
        expectedKeys.add("Subaru");
        
        RawRecordContainer myEvent = getEvent(conf);
        
        EdgeHandlerTestUtil.processEvent(fields, edgeHandler, myEvent, 8, true, false);
        Assert.assertEquals(expectedKeys, EdgeHandlerTestUtil.edgeKeyResults);
        
    }
    
    @Test
    public void testAwareGreaterThanSameGroup() {
        // CANINE == 'shepherd'
        
        fields.put("EVENT_DATE", new BaseNormalizedContent("EVENT_DATE", "2022-10-26T01:31:53Z"));
        fields.put("UUID", new BaseNormalizedContent("UUID", "0016dd72-0000-827d-dd4d-001b2163ba09"));
        fields.put("CANINE", new NormalizedFieldAndValue("CANINE", "shepherd", "PET", "0"));
        fields.put("WEIGHT", new NormalizedFieldAndValue("WEIGHT", "60", "PET", "0"));
        fields.put("ACTIVITY", new NormalizedFieldAndValue("ACTIVITY", "tug", "PET", "0"));
        
        fields.put("CANINE", new NormalizedFieldAndValue("CANINE", "bernese", "PET", "1"));
        fields.put("WEIGHT", new NormalizedFieldAndValue("WEIGHT", "80", "PET", "1"));
        fields.put("ACTIVITY", new NormalizedFieldAndValue("ACTIVITY", "fetch", "PET", "1"));
        
        fields.put("CANINE", new NormalizedFieldAndValue("CANINE", "chihuahua", "PET", "2"));
        fields.put("WEIGHT", new NormalizedFieldAndValue("WEIGHT", "7", "PET", "2"));
        fields.put("ACTIVITY", new NormalizedFieldAndValue("ACTIVITY", "bite", "PET", "2"));
        
        ProtobufEdgeDataTypeHandler<Text,BulkIngestKey,Value> edgeHandler = new ProtobufEdgeDataTypeHandler<>();
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        edgeHandler.setup(context);
        
        Set<String> expectedKeys = new HashSet<>();
        expectedKeys.add("bernese%00;fetch");
        expectedKeys.add("fetch%00;bernese");
        expectedKeys.add("fetch");
        expectedKeys.add("bernese");
        
        RawRecordContainer myEvent = getEvent(conf);
        
        EdgeHandlerTestUtil.processEvent(fields, edgeHandler, myEvent, 4, true, false);
        Assert.assertEquals(expectedKeys, EdgeHandlerTestUtil.edgeKeyResults);
        
    }
}
