package datawave.ingest.mapreduce.handler.edge;

import static datawave.ingest.mapreduce.handler.edge.EdgeKeyVersioningCache.KEY_VERSION_CACHE_DIR;
import static datawave.ingest.mapreduce.handler.edge.EdgeKeyVersioningCache.KEY_VERSION_DIST_CACHE_DIR;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
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

public class ProtobufEdgeDirectionTest {

    private static final Multimap<String,NormalizedContentInterface> fields = HashMultimap.create();
    // bidirectional edge definition
    private static final Type typeBi = new Type("bidirectional", FakeIngestHelper.class, null, new String[] {SimpleDataTypeHandler.class.getName()}, 10, null);
    // unidirectional edge definition
    private static final Type typeUni = new Type("unidirectional", FakeIngestHelper.class, null, new String[] {SimpleDataTypeHandler.class.getName()}, 10,
                    null);
    // bidirectional edge definition with jexl conditionl
    private static final Type typeBiJexl = new Type("bidirectional_jexlCondition", FakeIngestHelper.class, null,
                    new String[] {SimpleDataTypeHandler.class.getName()}, 10, null);
    // unidirectional edge definition
    private static final Type typeUniJexl = new Type("unidirectional_jexlCondition", FakeIngestHelper.class, null,
                    new String[] {SimpleDataTypeHandler.class.getName()}, 10, null);

    private static final String jexlConditon = "FISH == 'guppy'";

    private Configuration conf;

    private ProtobufEdgeDataTypeHandler<Text,BulkIngestKey,Value> edgeHandler;

    @Before
    public void setup() {
        TypeRegistry.reset();
        conf = new Configuration();
        conf.addResource(ClassLoader.getSystemResource("config/all-config.xml"));
        conf.addResource(ClassLoader.getSystemResource("config/edge-ingest-config.xml"));
        conf.addResource(ClassLoader.getSystemResource("config/metadata-config.xml"));
        conf.setBoolean(ProtobufEdgeDataTypeHandler.EVALUATE_PRECONDITIONS, true);
        conf.set(ProtobufEdgeDataTypeHandler.EDGE_SPRING_CONFIG, "config/EdgeSpringConfigDirection.xml");
        conf.set(KEY_VERSION_CACHE_DIR, ClassLoader.getSystemResource("config").getPath());
        conf.set(KEY_VERSION_DIST_CACHE_DIR, ClassLoader.getSystemResource("config").getPath());

        TypeRegistry registry = TypeRegistry.getInstance(conf);
        registry.put(typeBi.typeName(), typeBi);
        registry.put(typeUni.typeName(), typeUni);
        registry.put(typeBiJexl.typeName(), typeBiJexl);
        registry.put(typeUniJexl.typeName(), typeUniJexl);

        fields.clear();
        EdgeHandlerTestUtil.edgeKeyResults.clear();
        EdgeHandlerTestUtil.edgeValueResults.clear();
        EdgeHandlerTestUtil.metaData.clear();

        fields.put("EVENT_DATE", new BaseNormalizedContent("EVENT_DATE", "2022-10-26T01:31:53Z"));
        fields.put("UUID", new BaseNormalizedContent("UUID", "0016dd72-0000-827d-dd4d-001b2163ba09"));
        fields.put("FELINE", new NormalizedFieldAndValue("FELINE", "tabby", "PET", "0"));
        fields.put("FELINE", new NormalizedFieldAndValue("FELINE", "siamese", "PET", "1"));
        fields.put("FISH", new NormalizedFieldAndValue("FISH", "salmon", "WILD", "0"));
        fields.put("FISH", new NormalizedFieldAndValue("FISH", "guppy", "WILD", "1"));
        fields.put("ACTIVITY", new NormalizedFieldAndValue("ACTIVITY", "fetch", "THING", "0"));

        edgeHandler = new ProtobufEdgeDataTypeHandler<>();
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        edgeHandler.setup(context);
    }

    /**
     * Test Edge Definition with direction set to BIDIRECTIONAL
     */
    @Test
    public void testBiDirection() {
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

        RawRecordContainer myEvent = getEvent(typeBi);

        EdgeHandlerTestUtil.processEvent(fields, edgeHandler, myEvent, expectedKeys.size(), true, false);

        // check edge dictionary
        // forward direction
        Assert.assertEquals(1, EdgeHandlerTestUtil.metaData.keySet().stream().filter(Objects::nonNull).filter(key -> {
            return key.getColumnFamily().toString().equals("edge") && key.getRow().toString().equals("MY_EDGE_TYPE/FROM-TO");
        }).count());

        // reverse direction
        Assert.assertEquals(1, EdgeHandlerTestUtil.metaData.keySet().stream().filter(Objects::nonNull).filter(key -> {
            return key.getColumnFamily().toString().equals("edge") && key.getRow().toString().equals("MY_EDGE_TYPE/TO-FROM");
        }).count());

        // check edge key results
        Assert.assertEquals(expectedKeys, EdgeHandlerTestUtil.edgeKeyResults.keySet());

        // check forward direction keys
        Assert.assertEquals("STATS/ACTIVITY/MY_EDGE_TYPE/FROM", Objects.requireNonNull(EdgeHandlerTestUtil.edgeKeyResults.get("siamese").get(0))[0]);
        Assert.assertEquals("STATS/ACTIVITY/MY_EDGE_TYPE/FROM", Objects.requireNonNull(EdgeHandlerTestUtil.edgeKeyResults.get("tabby").get(0))[0]);

        Assert.assertEquals("MY_EDGE_TYPE/FROM-TO", Objects.requireNonNull(EdgeHandlerTestUtil.edgeKeyResults.get("siamese%00;guppy").get(0))[0]);
        Assert.assertEquals("MY_EDGE_TYPE/FROM-TO", Objects.requireNonNull(EdgeHandlerTestUtil.edgeKeyResults.get("siamese%00;salmon").get(0))[0]);
        Assert.assertEquals("MY_EDGE_TYPE/FROM-TO", Objects.requireNonNull(EdgeHandlerTestUtil.edgeKeyResults.get("tabby%00;guppy").get(0))[0]);
        Assert.assertEquals("MY_EDGE_TYPE/FROM-TO", Objects.requireNonNull(EdgeHandlerTestUtil.edgeKeyResults.get("tabby%00;salmon").get(0))[0]);

        // check reverse direction keys
        Assert.assertEquals("STATS/ACTIVITY/MY_EDGE_TYPE/TO", Objects.requireNonNull(EdgeHandlerTestUtil.edgeKeyResults.get("guppy").get(0))[0]);
        Assert.assertEquals("STATS/ACTIVITY/MY_EDGE_TYPE/TO", Objects.requireNonNull(EdgeHandlerTestUtil.edgeKeyResults.get("salmon").get(0))[0]);

        Assert.assertEquals("MY_EDGE_TYPE/TO-FROM", Objects.requireNonNull(EdgeHandlerTestUtil.edgeKeyResults.get("guppy%00;siamese").get(0))[0]);
        Assert.assertEquals("MY_EDGE_TYPE/TO-FROM", Objects.requireNonNull(EdgeHandlerTestUtil.edgeKeyResults.get("guppy%00;tabby").get(0))[0]);
        Assert.assertEquals("MY_EDGE_TYPE/TO-FROM", Objects.requireNonNull(EdgeHandlerTestUtil.edgeKeyResults.get("salmon%00;siamese").get(0))[0]);
        Assert.assertEquals("MY_EDGE_TYPE/TO-FROM", Objects.requireNonNull(EdgeHandlerTestUtil.edgeKeyResults.get("salmon%00;tabby").get(0))[0]);
    }

    /**
     * Test Edge Definition with direction set to UNIDIRECTIONAL
     */
    @Test
    public void testUniDirection() {
        Set<String> expectedKeys = new HashSet<>();
        expectedKeys.add("siamese");
        expectedKeys.add("siamese%00;guppy");
        expectedKeys.add("siamese%00;salmon");

        expectedKeys.add("tabby");
        expectedKeys.add("tabby%00;salmon");
        expectedKeys.add("tabby%00;guppy");

        RawRecordContainer myEvent = getEvent(typeUni);

        EdgeHandlerTestUtil.processEvent(fields, edgeHandler, myEvent, expectedKeys.size(), true, false);
        Assert.assertEquals(expectedKeys, EdgeHandlerTestUtil.edgeKeyResults.keySet());

        // check edge dictionary
        // forward direction
        Assert.assertEquals(1, EdgeHandlerTestUtil.metaData.keySet().stream().filter(Objects::nonNull).filter(key -> {
            return key.getColumnFamily().toString().equals("edge") && key.getRow().toString().equals("MY_EDGE_TYPE/FROM-TO");
        }).count());

        // NO reverse direction
        Assert.assertEquals(0, EdgeHandlerTestUtil.metaData.keySet().stream().filter(Objects::nonNull).filter(key -> {
            return key.getColumnFamily().toString().equals("edge") && key.getRow().toString().equals("MY_EDGE_TYPE/TO-FROM");
        }).count());

        // check forward direction keys
        Assert.assertEquals("STATS/ACTIVITY/MY_EDGE_TYPE/FROM", Objects.requireNonNull(EdgeHandlerTestUtil.edgeKeyResults.get("siamese").get(0))[0]);
        Assert.assertEquals("STATS/ACTIVITY/MY_EDGE_TYPE/FROM", Objects.requireNonNull(EdgeHandlerTestUtil.edgeKeyResults.get("tabby").get(0))[0]);

        Assert.assertEquals("MY_EDGE_TYPE/FROM-TO", Objects.requireNonNull(EdgeHandlerTestUtil.edgeKeyResults.get("siamese%00;guppy").get(0))[0]);
        Assert.assertEquals("MY_EDGE_TYPE/FROM-TO", Objects.requireNonNull(EdgeHandlerTestUtil.edgeKeyResults.get("siamese%00;salmon").get(0))[0]);
        Assert.assertEquals("MY_EDGE_TYPE/FROM-TO", Objects.requireNonNull(EdgeHandlerTestUtil.edgeKeyResults.get("tabby%00;guppy").get(0))[0]);
        Assert.assertEquals("MY_EDGE_TYPE/FROM-TO", Objects.requireNonNull(EdgeHandlerTestUtil.edgeKeyResults.get("tabby%00;salmon").get(0))[0]);

        // NO reverse direction keys
        for (String reverseKey : new String[] {"guppy", "salmon", "guppy%00;siamese", "guppy%00;tabby", "salmon%00;siamese", "salmon%00;tabby"}) {
            Assert.assertFalse(EdgeHandlerTestUtil.edgeKeyResults.containsKey(reverseKey));
        }
    }

    /**
     * Test Edge Definition with direction set to BIDIRECTIONAL and Jexl Condition => FISH==guppy
     */
    @Test
    public void testBiDirectionWithJexlCondition() {
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

        RawRecordContainer myEvent = getEvent(typeBiJexl);

        EdgeHandlerTestUtil.processEvent(fields, edgeHandler, myEvent, expectedKeys.size(), true, false);

        // check edge dictionary
        // forward direction
        Assert.assertEquals(1, EdgeHandlerTestUtil.metaData.keySet().stream().filter(Objects::nonNull).filter(key -> {
            return key.getColumnFamily().toString().equals("edge") && key.getRow().toString().equals("MY_EDGE_TYPE/FROM-TO");
        }).count());

        // reverse direction
        Assert.assertEquals(1, EdgeHandlerTestUtil.metaData.keySet().stream().filter(Objects::nonNull).filter(key -> {
            return key.getColumnFamily().toString().equals("edge") && key.getRow().toString().equals("MY_EDGE_TYPE/TO-FROM");
        }).count());

        // jexl condition
        Assert.assertEquals(2, EdgeHandlerTestUtil.metaData.values().stream().filter(Objects::nonNull).filter(value -> {
            return value.toString().contains(jexlConditon);
        }).count());

        // check edge key results
        Assert.assertEquals(expectedKeys, EdgeHandlerTestUtil.edgeKeyResults.keySet());

        // check forward direction keys
        Assert.assertEquals("STATS/ACTIVITY/MY_EDGE_TYPE/FROM", Objects.requireNonNull(EdgeHandlerTestUtil.edgeKeyResults.get("siamese").get(0))[0]);
        Assert.assertEquals("STATS/ACTIVITY/MY_EDGE_TYPE/FROM", Objects.requireNonNull(EdgeHandlerTestUtil.edgeKeyResults.get("tabby").get(0))[0]);

        Assert.assertEquals("MY_EDGE_TYPE/FROM-TO", Objects.requireNonNull(EdgeHandlerTestUtil.edgeKeyResults.get("siamese%00;guppy").get(0))[0]);
        Assert.assertEquals("MY_EDGE_TYPE/FROM-TO", Objects.requireNonNull(EdgeHandlerTestUtil.edgeKeyResults.get("siamese%00;salmon").get(0))[0]);
        Assert.assertEquals("MY_EDGE_TYPE/FROM-TO", Objects.requireNonNull(EdgeHandlerTestUtil.edgeKeyResults.get("tabby%00;guppy").get(0))[0]);
        Assert.assertEquals("MY_EDGE_TYPE/FROM-TO", Objects.requireNonNull(EdgeHandlerTestUtil.edgeKeyResults.get("tabby%00;salmon").get(0))[0]);

        // check reverse direction keys
        Assert.assertEquals("STATS/ACTIVITY/MY_EDGE_TYPE/TO", Objects.requireNonNull(EdgeHandlerTestUtil.edgeKeyResults.get("guppy").get(0))[0]);
        Assert.assertEquals("STATS/ACTIVITY/MY_EDGE_TYPE/TO", Objects.requireNonNull(EdgeHandlerTestUtil.edgeKeyResults.get("salmon").get(0))[0]);

        Assert.assertEquals("MY_EDGE_TYPE/TO-FROM", Objects.requireNonNull(EdgeHandlerTestUtil.edgeKeyResults.get("guppy%00;siamese").get(0))[0]);
        Assert.assertEquals("MY_EDGE_TYPE/TO-FROM", Objects.requireNonNull(EdgeHandlerTestUtil.edgeKeyResults.get("guppy%00;tabby").get(0))[0]);
        Assert.assertEquals("MY_EDGE_TYPE/TO-FROM", Objects.requireNonNull(EdgeHandlerTestUtil.edgeKeyResults.get("salmon%00;siamese").get(0))[0]);
        Assert.assertEquals("MY_EDGE_TYPE/TO-FROM", Objects.requireNonNull(EdgeHandlerTestUtil.edgeKeyResults.get("salmon%00;tabby").get(0))[0]);
    }

    /**
     * Test Edge Definition with direction set to UNIDIRECTIONAL and Jexl Condition => FISH==guppy
     */
    @Test
    public void testUniDirectionWithJexlCondition() {
        Set<String> expectedKeys = new HashSet<>();
        expectedKeys.add("siamese");
        expectedKeys.add("siamese%00;guppy");
        expectedKeys.add("siamese%00;salmon");

        expectedKeys.add("tabby");
        expectedKeys.add("tabby%00;salmon");
        expectedKeys.add("tabby%00;guppy");

        RawRecordContainer myEvent = getEvent(typeUniJexl);

        EdgeHandlerTestUtil.processEvent(fields, edgeHandler, myEvent, expectedKeys.size(), true, false);
        Assert.assertEquals(expectedKeys, EdgeHandlerTestUtil.edgeKeyResults.keySet());

        // check edge dictionary
        // forward direction
        Assert.assertEquals(1, EdgeHandlerTestUtil.metaData.keySet().stream().filter(Objects::nonNull).filter(key -> {
            return key.getColumnFamily().toString().equals("edge") && key.getRow().toString().equals("MY_EDGE_TYPE/FROM-TO");
        }).count());

        // NO reverse direction
        Assert.assertEquals(0, EdgeHandlerTestUtil.metaData.keySet().stream().filter(Objects::nonNull).filter(key -> {
            return key.getColumnFamily().toString().equals("edge") && key.getRow().toString().equals("MY_EDGE_TYPE/TO-FROM");
        }).count());
        // jexl condition
        Assert.assertEquals(1, EdgeHandlerTestUtil.metaData.values().stream().filter(Objects::nonNull).filter(value -> {
            return value.toString().contains(jexlConditon);
        }).count());

        // check forward direction keys
        Assert.assertEquals("STATS/ACTIVITY/MY_EDGE_TYPE/FROM", Objects.requireNonNull(EdgeHandlerTestUtil.edgeKeyResults.get("siamese").get(0))[0]);
        Assert.assertEquals("STATS/ACTIVITY/MY_EDGE_TYPE/FROM", Objects.requireNonNull(EdgeHandlerTestUtil.edgeKeyResults.get("tabby").get(0))[0]);

        Assert.assertEquals("MY_EDGE_TYPE/FROM-TO", Objects.requireNonNull(EdgeHandlerTestUtil.edgeKeyResults.get("siamese%00;guppy").get(0))[0]);
        Assert.assertEquals("MY_EDGE_TYPE/FROM-TO", Objects.requireNonNull(EdgeHandlerTestUtil.edgeKeyResults.get("siamese%00;salmon").get(0))[0]);
        Assert.assertEquals("MY_EDGE_TYPE/FROM-TO", Objects.requireNonNull(EdgeHandlerTestUtil.edgeKeyResults.get("tabby%00;guppy").get(0))[0]);
        Assert.assertEquals("MY_EDGE_TYPE/FROM-TO", Objects.requireNonNull(EdgeHandlerTestUtil.edgeKeyResults.get("tabby%00;salmon").get(0))[0]);

        // NO reverse direction keys
        for (String reverseKey : new String[] {"guppy", "salmon", "guppy%00;siamese", "guppy%00;tabby", "salmon%00;siamese", "salmon%00;tabby"}) {
            Assert.assertFalse(EdgeHandlerTestUtil.edgeKeyResults.containsKey(reverseKey));
        }
    }

    private RawRecordContainer getEvent(Type type) {
        RawRecordContainerImpl myEvent = new RawRecordContainerImpl();
        myEvent.addSecurityMarking("columnVisibility", "PRIVATE");
        myEvent.setDataType(type);
        myEvent.setId(UID.builder().newId());
        myEvent.setAltIds(Collections.singleton("0016dd72-0000-827d-dd4d-001b2163ba09"));
        myEvent.setConf(conf);

        Instant i = Instant.from(DateTimeFormatter.ISO_INSTANT.parse("2022-10-26T01:31:53Z"));
        myEvent.setTimestamp(i.toEpochMilli());

        return myEvent;
    }
}
