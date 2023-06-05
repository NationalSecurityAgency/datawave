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
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.Set;

import static datawave.ingest.mapreduce.handler.edge.EdgeKeyVersioningCache.KEY_VERSION_CACHE_DIR;
import static datawave.ingest.mapreduce.handler.edge.EdgeKeyVersioningCache.KEY_VERSION_DIST_CACHE_DIR;

public class ProtobufEdgeDeletePreconditionTest {


    private static Multimap<String, NormalizedContentInterface> fields = HashMultimap.create();
    private static Type type = new Type("mycsv", FakeIngestHelper.class, null, new String[]{SimpleDataTypeHandler.class.getName()}, 10, null);
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
        conf.setBoolean("ingest.mode.delete", true);

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
        myEvent.setDate(i.toEpochMilli());

        return myEvent;
    }

    @Test
    public void testDeleteUnawarePreconSameGroup() {
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

        EdgeHandlerTestUtil.processEvent(fields, edgeHandler, myEvent, 8, true, true);
        Assert.assertEquals(expectedKeys, EdgeHandlerTestUtil.edgeKeyResults);

    }

    @Test
    public void testDeleteUnawarePreconDifferentGroup() {
        // FELINE == 'tabby'


        fields.put("EVENT_DATE", new BaseNormalizedContent("EVENT_DATE", "2022-10-26T01:31:53Z"));
        fields.put("UUID", new BaseNormalizedContent("UUID", "0016dd72-0000-827d-dd4d-001b2163ba09"));
        fields.put("FELINE", new NormalizedFieldAndValue("FELINE", "tabby", "PET", "0"));
        fields.put("FELINE", new NormalizedFieldAndValue("FELINE", "siamese", "PET", "1"));
        fields.put("FISH", new NormalizedFieldAndValue("FISH", "salmon", "WILD", "0"));
        fields.put("FISH", new NormalizedFieldAndValue("FISH", "guppy", "WILD", "1"));
        fields.put("ACTIVITY", new NormalizedFieldAndValue("ACTIVITY", "fetch", "THING", "0"));

        ProtobufEdgeDataTypeHandler<Text, BulkIngestKey, Value> edgeHandler = new ProtobufEdgeDataTypeHandler<>();
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

        EdgeHandlerTestUtil.processEvent(fields, edgeHandler, myEvent, 12, true, true);

        Assert.assertEquals(expectedKeys, EdgeHandlerTestUtil.edgeKeyResults);

    }

    @Test
    public void testDeleteUnawarePreconAndedDifferentGroup() {
        // INGREDIENT == 'banana' && COLOR == 'blue'

        fields.put("EVENT_DATE", new BaseNormalizedContent("EVENT_DATE", "2022-10-26T01:31:53Z"));
        fields.put("UUID", new BaseNormalizedContent("UUID", "0016dd72-0000-827d-dd4d-001b2163ba09"));
        fields.put("INGREDIENT", new NormalizedFieldAndValue("INGREDIENT", "banana", "FOOD", "0"));
        fields.put("INGREDIENT", new NormalizedFieldAndValue("INGREDIENT", "apple", "FOOD", "1"));
        fields.put("DISH", new NormalizedFieldAndValue("DISH", "banoffee", "FOOD", "0"));
        fields.put("DISH", new NormalizedFieldAndValue("DISH", "applesauce", "FOOD", "1"));
        fields.put("COLOR", new NormalizedFieldAndValue("COLOR", "green", "ATTR", "0"));
        fields.put("COLOR", new NormalizedFieldAndValue("COLOR", "blue", "ATTR", "1"));
        fields.put("CAR", new NormalizedFieldAndValue("CAR", "zonda", "ATTR", "0"));
        fields.put("CAR", new NormalizedFieldAndValue("CAR", "lotus", "ATTR", "1"));

        ProtobufEdgeDataTypeHandler<Text, BulkIngestKey, Value> edgeHandler = new ProtobufEdgeDataTypeHandler<>();
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        edgeHandler.setup(context);

        Set<String> expectedKeys = new HashSet<>();
        //please do not eat and drive.  this dataset is for example only and not meant to condone culinarily distracted driving
        expectedKeys.add("zonda");
        expectedKeys.add("zonda%00;banoffee");
        expectedKeys.add("zonda%00;applesauce");

        expectedKeys.add("lotus");
        expectedKeys.add("lotus%00;banoffee");
        expectedKeys.add("lotus%00;applesauce");

        expectedKeys.add("applesauce");
        expectedKeys.add("applesauce%00;zonda");
        expectedKeys.add("applesauce%00;lotus");

        expectedKeys.add("banoffee");
        expectedKeys.add("banoffee%00;zonda");
        expectedKeys.add("banoffee%00;lotus");


        RawRecordContainer myEvent = getEvent(conf);

        EdgeHandlerTestUtil.processEvent(fields, edgeHandler, myEvent, 12, true, true);

        Assert.assertEquals(expectedKeys, EdgeHandlerTestUtil.edgeKeyResults);

    }

}
