package datawave.ingest.mapreduce.handler.edge;

import static datawave.ingest.mapreduce.handler.edge.EdgeKeyVersioningCache.KEY_VERSION_CACHE_DIR;
import static datawave.ingest.mapreduce.handler.edge.EdgeKeyVersioningCache.KEY_VERSION_DIST_CACHE_DIR;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

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
import datawave.util.time.DateHelper;

public class ProtobufEdgePreconditionTest {

    private static Logger log = Logger.getLogger(ProtobufEdgePreconditionTest.class);
    private static Enumeration rootAppenders = Logger.getRootLogger().getAllAppenders();
    private static Multimap<String,NormalizedContentInterface> fields = HashMultimap.create();
    private static Type type = new Type("mycsv", FakeIngestHelper.class, null, new String[] {SimpleDataTypeHandler.class.getName()}, 10, null);
    private static final Now now = Now.getInstance();
    private Configuration conf;
    private String loadDateStr = DateHelper.format(new Date(now.get()));

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
        EdgeHandlerTestUtil.edgeValueResults.clear();
    }

    private RawRecordContainer getEvent(Configuration conf) {

        RawRecordContainerImpl myEvent = new RawRecordContainerImpl();
        myEvent.addSecurityMarking("columnVisibility", "PRIVATE");
        myEvent.setDataType(type);
        myEvent.setId(UID.builder().newId());
        myEvent.setAltIds(Collections.singleton("0016dd72-0000-827d-dd4d-001b2163ba09"));
        myEvent.setConf(conf);

        Instant i = Instant.from(DateTimeFormatter.ISO_INSTANT.parse("2022-10-26T01:31:53Z"));
        myEvent.setDate(i.toEpochMilli());

        return myEvent;
    }

    @Test
    public void testUnawarePreconSameGroup() {
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
        Assert.assertEquals(expectedKeys, EdgeHandlerTestUtil.edgeKeyResults.keySet());

        // colFam
        Assert.assertEquals("MY_EDGE_TYPE/TO-FROM", EdgeHandlerTestUtil.edgeKeyResults.get("guppy%00;siamese").get(0)[0]);

        // colQual
        Assert.assertEquals("20221026/MY_CSV_DATA-MY_CSV_DATA///B", EdgeHandlerTestUtil.edgeKeyResults.get("guppy%00;siamese").get(0)[1]);

        // values
        Assert.assertEquals(1, EdgeHandlerTestUtil.edgeValueResults.get("guppy%00;siamese").size());
        Assert.assertEquals(
                        "count: 1, bitmask: 2, sourceValue: guppy, sinkValue: siamese, hours: , duration: , loadDate: " + loadDateStr
                                        + ", uuidString: , uuidObj: 0016dd72-0000-827d-dd4d-001b2163ba09, badActivityDate: ",
                        EdgeHandlerTestUtil.edgeValueResults.get("guppy%00;siamese").get(0));
        Assert.assertEquals(1, EdgeHandlerTestUtil.edgeValueResults.get("guppy").size());
        Assert.assertEquals(
                        "count: , bitmask: , sourceValue: guppy, sinkValue: , hours: [0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], duration: , loadDate: "
                                        + loadDateStr + ", uuidString: , uuidObj: 0016dd72-0000-827d-dd4d-001b2163ba09, badActivityDate: ",
                        EdgeHandlerTestUtil.edgeValueResults.get("guppy").get(0));

        // vis and ts
        Assert.assertEquals("PRIVATE", EdgeHandlerTestUtil.edgeKeyResults.get("guppy%00;siamese").get(0)[2]);
        Assert.assertEquals("1666747913000", EdgeHandlerTestUtil.edgeKeyResults.get("guppy%00;siamese").get(0)[3]);

    }

    @Test
    public void testUnawarePreconSameGroupEarlyActivityDate() {
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
        myEvent.setDate(1666737913000L);

        // the count is doubled since activity < event date in this test. In this case, we add 2 edges each.
        EdgeHandlerTestUtil.processEvent(fields, edgeHandler, myEvent, 16, true, false);
        Assert.assertEquals(expectedKeys, EdgeHandlerTestUtil.edgeKeyResults.keySet());

        Assert.assertEquals("MY_EDGE_TYPE/TO-FROM", EdgeHandlerTestUtil.edgeKeyResults.get("guppy%00;siamese").get(0)[0]);

        // the dates
        Assert.assertEquals("20221025/MY_CSV_DATA-MY_CSV_DATA///A", EdgeHandlerTestUtil.edgeKeyResults.get("guppy%00;siamese").get(0)[1]);
        Assert.assertEquals("20221026/MY_CSV_DATA-MY_CSV_DATA///C", EdgeHandlerTestUtil.edgeKeyResults.get("guppy%00;siamese").get(1)[1]);

        // values
        Assert.assertEquals(2, EdgeHandlerTestUtil.edgeValueResults.get("guppy%00;siamese").size());
        Assert.assertEquals(
                        "count: 1, bitmask: 4194304, sourceValue: guppy, sinkValue: siamese, hours: , duration: , loadDate: " + loadDateStr
                                        + ", uuidString: , uuidObj: 0016dd72-0000-827d-dd4d-001b2163ba09, badActivityDate: false",
                        EdgeHandlerTestUtil.edgeValueResults.get("guppy%00;siamese").get(0));
        Assert.assertEquals(
                        "count: 1, bitmask: 2, sourceValue: guppy, sinkValue: siamese, hours: , duration: , loadDate: " + loadDateStr
                                        + ", uuidString: , uuidObj: 0016dd72-0000-827d-dd4d-001b2163ba09, badActivityDate: ",
                        EdgeHandlerTestUtil.edgeValueResults.get("guppy%00;siamese").get(1));
        Assert.assertEquals(2, EdgeHandlerTestUtil.edgeValueResults.get("guppy").size());
        Assert.assertEquals(
                        "count: , bitmask: , sourceValue: guppy, sinkValue: , hours: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0], duration: , loadDate: "
                                        + loadDateStr + ", uuidString: , uuidObj: 0016dd72-0000-827d-dd4d-001b2163ba09, badActivityDate: false",
                        EdgeHandlerTestUtil.edgeValueResults.get("guppy").get(0));
        Assert.assertEquals(
                        "count: , bitmask: , sourceValue: guppy, sinkValue: , hours: [0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], duration: , loadDate: "
                                        + loadDateStr + ", uuidString: , uuidObj: 0016dd72-0000-827d-dd4d-001b2163ba09, badActivityDate: ",
                        EdgeHandlerTestUtil.edgeValueResults.get("guppy").get(1));

        Assert.assertEquals("PRIVATE", EdgeHandlerTestUtil.edgeKeyResults.get("guppy%00;siamese").get(0)[2]);
        Assert.assertEquals("1666737913000", EdgeHandlerTestUtil.edgeKeyResults.get("guppy%00;siamese").get(0)[3]);

    }

    @Test
    public void testUnawarePreconSameGroupVeryOldData() {
        // FELINE == 'tabby'

        fields.put("EVENT_DATE", new BaseNormalizedContent("EVENT_DATE", "1966-09-08"));
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
        myEvent.setDate(0L);

        // the count is doubled since activity < event date in this test. In this case, we add 2 edges each.
        EdgeHandlerTestUtil.processEvent(fields, edgeHandler, myEvent, 16, true, false);
        Assert.assertEquals(expectedKeys, EdgeHandlerTestUtil.edgeKeyResults.keySet());

        Assert.assertEquals("MY_EDGE_TYPE/TO-FROM", EdgeHandlerTestUtil.edgeKeyResults.get("guppy%00;siamese").get(0)[0]);

        // the dates
        Assert.assertEquals("19700101/MY_CSV_DATA-MY_CSV_DATA///A", EdgeHandlerTestUtil.edgeKeyResults.get("guppy%00;siamese").get(0)[1]);
        Assert.assertEquals("19660908/MY_CSV_DATA-MY_CSV_DATA///C", EdgeHandlerTestUtil.edgeKeyResults.get("guppy%00;siamese").get(1)[1]);

        // values
        Assert.assertEquals(2, EdgeHandlerTestUtil.edgeValueResults.get("guppy%00;siamese").size());
        Assert.assertEquals(
                        "count: 1, bitmask: , sourceValue: guppy, sinkValue: siamese, hours: , duration: , loadDate: " + loadDateStr
                                        + ", uuidString: , uuidObj: 0016dd72-0000-827d-dd4d-001b2163ba09, badActivityDate: false",
                        EdgeHandlerTestUtil.edgeValueResults.get("guppy%00;siamese").get(0));
        Assert.assertEquals(
                        "count: 1, bitmask: 1, sourceValue: guppy, sinkValue: siamese, hours: , duration: , loadDate: " + loadDateStr
                                        + ", uuidString: , uuidObj: 0016dd72-0000-827d-dd4d-001b2163ba09, badActivityDate: ",
                        EdgeHandlerTestUtil.edgeValueResults.get("guppy%00;siamese").get(1));
        Assert.assertEquals(2, EdgeHandlerTestUtil.edgeValueResults.get("guppy").size());
        Assert.assertEquals(
                        "count: , bitmask: , sourceValue: guppy, sinkValue: , hours: [1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], duration: , loadDate: "
                                        + loadDateStr + ", uuidString: , uuidObj: 0016dd72-0000-827d-dd4d-001b2163ba09, badActivityDate: ",
                        EdgeHandlerTestUtil.edgeValueResults.get("guppy").get(0));
        Assert.assertEquals(
                        "count: , bitmask: , sourceValue: guppy, sinkValue: , hours: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], duration: , loadDate: "
                                        + loadDateStr + ", uuidString: , uuidObj: 0016dd72-0000-827d-dd4d-001b2163ba09, badActivityDate: false",
                        EdgeHandlerTestUtil.edgeValueResults.get("guppy").get(1));

        Assert.assertEquals("PRIVATE", EdgeHandlerTestUtil.edgeKeyResults.get("guppy%00;siamese").get(0)[2]);
        Assert.assertEquals("0", EdgeHandlerTestUtil.edgeKeyResults.get("guppy%00;siamese").get(0)[3]);

    }

    @Test
    public void testUnawarePreconDifferentGroup() {
        // FELINE == 'tabby'

        fields.put("EVENT_DATE", new BaseNormalizedContent("EVENT_DATE", "2022-10-26T01:31:53Z"));
        fields.put("UUID", new BaseNormalizedContent("UUID", "0016dd72-0000-827d-dd4d-001b2163ba09"));
        fields.put("FELINE", new NormalizedFieldAndValue("FELINE", "tabby", "PET", "0"));
        fields.put("FELINE", new NormalizedFieldAndValue("FELINE", "siamese", "PET", "1"));
        fields.put("FISH", new NormalizedFieldAndValue("FISH", "salmon", "WILD", "0"));
        fields.put("FISH", new NormalizedFieldAndValue("FISH", "guppy", "WILD", "1"));
        fields.put("ACTIVITY", new NormalizedFieldAndValue("ACTIVITY", "fetch", "THING", "0"));

        ProtobufEdgeDataTypeHandler<Text,BulkIngestKey,Value> edgeHandler = new ProtobufEdgeDataTypeHandler<>();
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        edgeHandler.setup(context);

        Set<String> expectedKeys = new HashSet<>();
        expectedKeys.add("guppy");
        expectedKeys.add("guppy%00;siamese");
        expectedKeys.add("guppy%00;tabby");

        expectedKeys.add("salmon");
        expectedKeys.add("salmon%00;tabby");
        expectedKeys.add("salmon%00;siamese");

        expectedKeys.add("siamese");
        expectedKeys.add("siamese%00;guppy");
        expectedKeys.add("siamese%00;salmon");

        expectedKeys.add("tabby");
        expectedKeys.add("tabby%00;salmon");
        expectedKeys.add("tabby%00;guppy");

        RawRecordContainer myEvent = getEvent(conf);

        EdgeHandlerTestUtil.processEvent(fields, edgeHandler, myEvent, 12, true, false);
        Assert.assertEquals(expectedKeys, EdgeHandlerTestUtil.edgeKeyResults.keySet());

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
        Assert.assertEquals(expectedKeys, EdgeHandlerTestUtil.edgeKeyResults.keySet());

    }

    @Test
    public void testAwareTwoNegated() {
        // CHEESE != 'apple' AND WINE != 'chianti'
        // make sure negations don't take the cross products of groups that each contained things that don't match

        fields.put("EVENT_DATE", new BaseNormalizedContent("EVENT_DATE", "2022-10-26T01:31:53Z"));
        fields.put("UUID", new BaseNormalizedContent("UUID", "0016dd72-0000-827d-dd4d-001b2163ba09"));
        fields.put("FRUIT", new NormalizedFieldAndValue("FRUIT", "apple", "FOOD", "0"));
        fields.put("FRUIT", new NormalizedFieldAndValue("FRUIT", "pear", "FOOD", "1"));
        fields.put("FRUIT", new NormalizedFieldAndValue("FRUIT", "orange", "FOOD", "2"));
        fields.put("WINE", new NormalizedFieldAndValue("WINE", "pinot noir", "FOOD", "0"));
        fields.put("WINE", new NormalizedFieldAndValue("WINE", "chianti", "FOOD", "1"));
        fields.put("WINE", new NormalizedFieldAndValue("WINE", "cabernet", "FOOD", "2"));

        ProtobufEdgeDataTypeHandler<Text,BulkIngestKey,Value> edgeHandler = new ProtobufEdgeDataTypeHandler<>();
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        edgeHandler.setup(context);

        Set<String> expectedKeys = new HashSet<>();
        expectedKeys.add("cabernet");
        expectedKeys.add("cabernet%00;orange");
        expectedKeys.add("orange");
        expectedKeys.add("orange%00;cabernet");

        RawRecordContainer myEvent = getEvent(conf);

        EdgeHandlerTestUtil.processEvent(fields, edgeHandler, myEvent, 4, true, false);
        Assert.assertEquals(expectedKeys, EdgeHandlerTestUtil.edgeKeyResults.keySet());

    }

    @Test
    public void testAwareAllNegated() {
        // CHEESE != 'apple' AND WINE != 'chianti'
        // make sure negations don't take the cross products of groups that each contained things that don't match

        fields.put("EVENT_DATE", new BaseNormalizedContent("EVENT_DATE", "2022-10-26T01:31:53Z"));
        fields.put("UUID", new BaseNormalizedContent("UUID", "0016dd72-0000-827d-dd4d-001b2163ba09"));
        fields.put("FRUIT", new NormalizedFieldAndValue("FRUIT", "apple", "FOOD", "0"));
        fields.put("FRUIT", new NormalizedFieldAndValue("FRUIT", "pear", "FOOD", "1"));
        fields.put("WINE", new NormalizedFieldAndValue("WINE", "pinot noir", "FOOD", "0"));
        fields.put("WINE", new NormalizedFieldAndValue("WINE", "chianti", "FOOD", "1"));

        ProtobufEdgeDataTypeHandler<Text,BulkIngestKey,Value> edgeHandler = new ProtobufEdgeDataTypeHandler<>();
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        edgeHandler.setup(context);

        Set<String> expectedKeys = new HashSet<>();

        RawRecordContainer myEvent = getEvent(conf);

        EdgeHandlerTestUtil.processEvent(fields, edgeHandler, myEvent, 0, true, false);
        Assert.assertEquals(expectedKeys, EdgeHandlerTestUtil.edgeKeyResults.keySet());

    }

    @Test
    public void testAwareNegation() {
        // CHEESE != 'cheddar'

        fields.put("EVENT_DATE", new BaseNormalizedContent("EVENT_DATE", "2022-10-26T01:31:53Z"));
        fields.put("UUID", new BaseNormalizedContent("UUID", "0016dd72-0000-827d-dd4d-001b2163ba09"));
        fields.put("CHEESE", new NormalizedFieldAndValue("CHEESE", "cheddar", "FOOD", "0"));
        fields.put("CHEESE", new NormalizedFieldAndValue("CHEESE", "parmesan", "FOOD", "1"));
        fields.put("WINE", new NormalizedFieldAndValue("WINE", "pinot noir", "FOOD", "0"));
        fields.put("WINE", new NormalizedFieldAndValue("WINE", "chianti", "FOOD", "1"));

        ProtobufEdgeDataTypeHandler<Text,BulkIngestKey,Value> edgeHandler = new ProtobufEdgeDataTypeHandler<>();
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        edgeHandler.setup(context);

        Set<String> expectedKeys = new HashSet<>();
        expectedKeys.add("parmesan");
        expectedKeys.add("parmesan%00;chianti");
        expectedKeys.add("chianti");
        expectedKeys.add("chianti%00;parmesan");

        RawRecordContainer myEvent = getEvent(conf);

        EdgeHandlerTestUtil.processEvent(fields, edgeHandler, myEvent, 4, true, false);
        Assert.assertEquals(expectedKeys, EdgeHandlerTestUtil.edgeKeyResults.keySet());

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
        Assert.assertEquals(expectedKeys, EdgeHandlerTestUtil.edgeKeyResults.keySet());

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
        Assert.assertEquals(expectedKeys, EdgeHandlerTestUtil.edgeKeyResults.keySet());

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
        Assert.assertEquals(expectedKeys, EdgeHandlerTestUtil.edgeKeyResults.keySet());

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
        Assert.assertEquals(expectedKeys, EdgeHandlerTestUtil.edgeKeyResults.keySet());

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
        Assert.assertEquals(expectedKeys, EdgeHandlerTestUtil.edgeKeyResults.keySet());

    }
}
