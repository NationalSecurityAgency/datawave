package datawave.ingest.annotation.mapreduce.handler;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.google.protobuf.util.JsonFormat;

import datawave.annotation.data.AnnotationWriteException;
import datawave.annotation.data.v1.AccumuloAnnotationSerializer;
import datawave.annotation.data.v1.AccumuloAnnotationSourceSerializer;
import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.AnnotationSource;
import datawave.annotation.protobuf.v1.Segment;
import datawave.annotation.util.v1.AnnotationJsonUtils;
import datawave.data.hash.UID;
import datawave.ingest.config.RawRecordContainerImpl;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.writer.AbstractContextWriter;
import datawave.ingest.test.StandaloneTaskAttemptContext;
import net.sf.saxon.s9api.SaxonApiException;

public class AnnotationHelperTest {
    private AnnotationHelper annotationHelper;
    private Configuration conf;
    private StandaloneTaskAttemptContext<Text,RawRecordContainer,BulkIngestKey,Value> ctx = null;
    private CachingContextWriter contextWriter;
    private MockStatusReporter statusReporter;

    @BeforeEach
    public void setupAnnotationHelper() {
        conf = new Configuration();
        conf.addResource(ClassLoader.getSystemResource("config/test-annotation-ingest-config.xml"));
        conf.addResource(ClassLoader.getSystemResource("config/test-annotation-transform-config.xml"));

        TypeRegistry.reset();
        TypeRegistry.getInstance(conf);

        annotationHelper = new AnnotationHelper(conf);
        statusReporter = new MockStatusReporter();
        ctx = new StandaloneTaskAttemptContext<>(conf, statusReporter);

        contextWriter = new CachingContextWriter();
        try {
            contextWriter.setup(ctx.getConfiguration(), false);
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("Error setting up context writer", e);
        }

    }

    @Test
    public void testGetTableNamesAndPriorities() {

        //@formatter:off
        String[] expectedTableNames = new String[] {
                conf.get(AnnotationHelper.ANNOTATION_TNAME),
                conf.get(AnnotationHelper.ANNOTATION_SOURCE_TNAME),
                conf.get(AnnotationHelper.TRUTHMARK_TNAME),
                conf.get(AnnotationHelper.TRUTHMARK_SOURCE_TNAME)
        };
        //@formatter:on

        assertArrayEquals(expectedTableNames, annotationHelper.getAnnotationTableNames(null));
        assertArrayEquals(expectedTableNames, annotationHelper.getAnnotationTableNames(new String[] {}));

        //@formatter:off
        String[] expectedTableNamesWithTest = new String[] {
                "test",
                expectedTableNames[0],
                expectedTableNames[1],
                expectedTableNames[2],
                expectedTableNames[3]
        };
        //@formatter:on

        assertArrayEquals(expectedTableNamesWithTest, annotationHelper.getAnnotationTableNames(new String[] {"test"}));

        //@formatter:off
        int[] expectedTablePriorities = new int[] {
                conf.getInt(AnnotationHelper.ANNOTATION_TABLE_LOAD_PRIORITY, -10),
                conf.getInt(AnnotationHelper.ANNOTATION_SOURCE_TABLE_LOAD_PRIORITY, -10),
                conf.getInt(AnnotationHelper.TRUTHMARK_TABLE_LOAD_PRIORITY, -10),
                conf.getInt(AnnotationHelper.TRUTHMARK_SOURCE_TABLE_LOAD_PRIORITY, -10)
        };
        //@formatter:on

        assertArrayEquals(expectedTablePriorities, annotationHelper.getAnnotationTableLoaderPriorities(null));
        assertArrayEquals(expectedTablePriorities, annotationHelper.getAnnotationTableLoaderPriorities(new int[] {}));

        //@formatter:off
        int[] expectedTablePrioritiesWithTest = new int[] {
                1,
                expectedTablePriorities[0],
                expectedTablePriorities[1],
                expectedTablePriorities[2],
                expectedTablePriorities[3]
        };
        //@formatter:on

        assertArrayEquals(expectedTablePrioritiesWithTest, annotationHelper.getAnnotationTableLoaderPriorities(new int[] {1}));
    }

    @Test
    public void testProcess() throws IOException, InterruptedException, ParseException {
        // create test event record wrapper with minimum set of fields required for creating annotation mutations
        RawRecordContainer event = new EventWithShardId("20251107_1");

        long time = new SimpleDateFormat("yyyyMMdd").parse("20251107").getTime();
        event.setDataType(TypeRegistry.getType("annotation"));
        event.setTimestamp(time);
        event.setVisibility(new ColumnVisibility("TEST_VISIBILITY"));
        event.setId(UID.parse("a.b.c"));
        event.setRawData(new byte[0]);

        // no fields are required
        HashMultimap<String,NormalizedContentInterface> fields = HashMultimap.create();

        annotationHelper.process(event, contextWriter, ctx, statusReporter, event.getId(), event.getVisibility().flatten(), event.getShardId().getBytes(),
                        ClassLoader.getSystemResource("input/singleAnnotation.json").openStream().readAllBytes());
        contextWriter.commit(ctx);

        BulkIngestKey expectedKey = new BulkIngestKey(new Text("datawave.annotation"),
                        new Key("20251107_1", "myannotation\0a.b.c\0testAnnotationType", "BAB72CC7\0seg\0testSegmentId1", "TEST_VISIBILITY", time));

        BulkIngestKey expectedSourceKey = new BulkIngestKey(new Text("datawave.annotationSource"),
                        new Key("F1A0463C207B3778B472B506F3F8351A", "d", "testEngine\u00006.7\u0000testAnalyticHash", new ColumnVisibility("PRIVATE"), time));

        assertFalse(contextWriter.getCache().get(expectedSourceKey).isEmpty());

        // the first segment value should be protobuf that can be parsed by Segment class
        Collection<Value> result = contextWriter.getCache().get(expectedKey);
        Optional<Value> firstValue = result.stream().findFirst();
        if (firstValue.isPresent()) {
            Value firstNestedValue = firstValue.get();
            byte[] segmentBytes = firstNestedValue.get();
            Segment segment = Segment.parseFrom(segmentBytes);
            assertEquals("testSegmentId1", segment.getSegmentHash(),
                            "BulkIngestKey structure could potentially change as annotation-core library gets updated.");
        } else {
            fail("BulkIngestKey does not have the expected value");
        }

    }

    @Test
    public void testProcessSingleAnnotationWithoutSource() throws IOException, InterruptedException, ParseException {
        // create test event record wrapper with minimum set of fields required for creating annotation mutations
        RawRecordContainer event = new EventWithShardId("20251107_1");

        long time = new SimpleDateFormat("yyyyMMdd").parse("20251107").getTime();
        event.setDataType(TypeRegistry.getType("annotation"));
        event.setTimestamp(time);
        event.setVisibility(new ColumnVisibility("TEST_VISIBILITY"));
        event.setId(UID.parse("a.b.c"));

        assertThrows(AnnotationWriteException.class, () -> {
            annotationHelper.process(event, contextWriter, ctx, statusReporter, event.getId(), event.getVisibility().flatten(), event.getShardId().getBytes(),
                            ClassLoader.getSystemResource("input/singleAnnotationWithoutSource.json").openStream().readAllBytes());
        }, "annotation without source is no longer allowed");
    }

    @Test
    public void testProcessBulk() throws IOException, ParseException {
        // create test event record wrapper with minimum set of fields required for creating annotation mutations
        RawRecordContainer event = new EventWithShardId("20251107_1");

        long time = new SimpleDateFormat("yyyyMMdd").parse("20251107").getTime();
        event.setDataType(TypeRegistry.getType("annotation"));
        event.setTimestamp(time);
        event.setVisibility(new ColumnVisibility("TEST_VISIBILITY"));
        event.setId(UID.parse("a.b.c"));

        // no fields are required
        HashMultimap<String,NormalizedContentInterface> fields = HashMultimap.create();

        // read the singleAnnotation.json and overlay the properties from the event to create BulkIngestKeys for datawave.annotation table
        Multimap<BulkIngestKey,Value> bulkKeys = annotationHelper
                        .processBulk(ClassLoader.getSystemResource("input/singleAnnotation.json").openStream().readAllBytes(), event, fields);

        BulkIngestKey expectedKey = new BulkIngestKey(new Text("datawave.annotation"),
                        new Key("20251107_1", "myannotation\0a.b.c\0testAnnotationType", "BAB72CC7\0seg\0testSegmentId1", "TEST_VISIBILITY", time));
        // checking to make sure the first segment BulkIngestKey is created
        assertTrue(bulkKeys.containsKey(expectedKey), "BulkIngestKey structure could potentially change as annotation-core library gets updated.");

        // the first segment value should be protobuf that can be parsed by Segment class
        Segment segment = Segment.parseFrom(bulkKeys.get(expectedKey).stream().findFirst().get().get());
        assertEquals("testSegmentId1", segment.getSegmentHash(), "BulkIngestKey structure could potentially change as annotation-core library gets updated.");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testInlineVectorInBulkAndStreamingIngest(boolean ignoreUnknownFields) throws Exception {
        conf.setBoolean("annotation.ignore.unknown.fields", ignoreUnknownFields);
        annotationHelper = new AnnotationHelper(conf);
        Annotation input = readUpstreamEmbedding();
        byte[] json = readUpstreamEmbeddingJson();
        RawRecordContainer event = embeddingEvent();

        Multimap<BulkIngestKey,Value> bulk = annotationHelper.processBulk(json, event, HashMultimap.create());
        annotationHelper.process(event, contextWriter, ctx, statusReporter, event.getId(), event.getVisibility().flatten(),
                        event.getShardId().getBytes(StandardCharsets.UTF_8), json);
        contextWriter.commit(ctx);
        assertEquals(new HashSet<>(bulk.entries()), new HashSet<>(contextWriter.getCache().entries()));
        assertTrue(bulk.keySet().stream().anyMatch(k -> k.getTableName().equals(new Text("datawave.annotationSource"))));

        Annotation restored = new AccumuloAnnotationSerializer().deserialize(
                        bulk.entries().stream().filter(e -> e.getKey().getTableName().equals(new Text("datawave.annotation"))).<Map.Entry<Key,Value>> map(
                                        e -> new AbstractMap.SimpleImmutableEntry<>(e.getKey().getKey(), e.getValue())).iterator());
        AnnotationSource restoredSource = new AccumuloAnnotationSourceSerializer().deserialize(
                        bulk.entries().stream().filter(e -> e.getKey().getTableName().equals(new Text("datawave.annotationSource"))).<Map.Entry<Key,Value>> map(
                                        e -> new AbstractMap.SimpleImmutableEntry<>(e.getKey().getKey(), e.getValue())).iterator());
        assertEquals(input.getSegmentsList(), restored.getSegmentsList());
        assertEquals(input.getSource().getAnalyticSourceHash(), restored.getAnalyticSourceHash());
        assertEquals(input.getSource().getAnalyticSourceHash(), restoredSource.getAnalyticSourceHash());
        assertEquals(input.getSource().getAnalyticHash(), restoredSource.getAnalyticHash());
        assertEquals("myannotation", restored.getDataType());
        assertEquals("a.b.c", restored.getUid());
        assertEquals("20251107_1", restored.getShard());
        assertEquals("TEST_VISIBILITY", restored.getMetadataOrThrow("visibility"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"sha256", "modality", "embeddingSpaceId"})
    public void testInvalidVectorFailsBeforeStreamingWrites(String field) throws Exception {
        conf.setBoolean("annotation.ignore.unknown.fields", false);
        annotationHelper = new AnnotationHelper(conf);
        Annotation.Builder invalid = readUpstreamEmbedding().toBuilder();
        if (field.equals("sha256")) {
            invalid.getSegmentsBuilder(0).getValuesBuilder(0).getVectorBuilder().setSha256("0".repeat(64));
        } else {
            invalid.getSegmentsBuilder(0).getValuesBuilder(0).getVectorBuilder()
                            .clearField(invalid.getSegments(0).getValues(0).getVector().getDescriptorForType().findFieldByName(field));
        }
        byte[] json = AnnotationJsonUtils.getPrinter().print(invalid.build()).getBytes(StandardCharsets.UTF_8);
        RawRecordContainer event = embeddingEvent();

        assertThrows(AnnotationWriteException.class, () -> annotationHelper.processBulk(json, event, HashMultimap.create()));
        assertThrows(AnnotationWriteException.class, () -> annotationHelper.process(event, contextWriter, ctx, statusReporter, event.getId(),
                        event.getVisibility().flatten(), event.getShardId().getBytes(StandardCharsets.UTF_8), json));
        contextWriter.commit(ctx);
        assertTrue(contextWriter.getCache().isEmpty());
    }

    private static Annotation readUpstreamEmbedding() throws IOException {
        Annotation.Builder builder = Annotation.newBuilder();
        JsonFormat.parser().merge(new String(readUpstreamEmbeddingJson(), StandardCharsets.UTF_8), builder);
        return builder.build();
    }

    private static byte[] readUpstreamEmbeddingJson() throws IOException {
        try (InputStream stream = AnnotationHelperTest.class.getResourceAsStream("/input/upstreamEmbeddingAnnotation.json")) {
            return stream.readAllBytes();
        }
    }

    private static RawRecordContainer embeddingEvent() throws ParseException {
        RawRecordContainer event = new EventWithShardId("20251107_1");
        event.setDataType(TypeRegistry.getType("annotation"));
        event.setTimestamp(new SimpleDateFormat("yyyyMMdd").parse("20251107").getTime());
        event.setVisibility(new ColumnVisibility("TEST_VISIBILITY"));
        event.setId(UID.parse("a.b.c"));
        event.setRawData(new byte[0]);
        return event;
    }

    @Test
    public void testBuildAnnotation() throws IOException, SaxonApiException {
        RawRecordContainer event = new EventWithShardId("20251107_1");

        event.setDataType(TypeRegistry.getType("annotation"));
        event.setTimestamp(System.currentTimeMillis());

        Annotation annotation = annotationHelper.buildAnnotation(ClassLoader.getSystemResource("input/singleAnnotation.json").openStream().readAllBytes(),
                        event.getShardId().getBytes(), UID.parse("1.2.3"), "viz".getBytes(), event);

        assertEquals("myannotation", annotation.getDataType());
        assertEquals("1.2.3", annotation.getUid());
    }

    @Test
    public void testTransformJson() throws SaxonApiException, IOException {
        // transformation remaps values out segmentValue from json
        assertTrue(annotationHelper.transformJson(ClassLoader.getSystemResource("input/singleAnnotation.json").openStream().readAllBytes())
                        .contains("\"boundary\""));
    }

    @Test
    public void testDisabledTransformJson() throws SaxonApiException, IOException {
        conf = new Configuration();
        conf.addResource(ClassLoader.getSystemResource("config/test-annotation-ingest-config-notransform.xml"));

        TypeRegistry.reset();
        TypeRegistry.getInstance(conf);

        annotationHelper = new AnnotationHelper(conf);

        assertFalse(annotationHelper.transformJson(ClassLoader.getSystemResource("input/singleAnnotation.json").openStream().readAllBytes())
                        .contains("\"boundary\""));
    }

    public static class EventWithShardId extends RawRecordContainerImpl {
        private String shardId;

        public EventWithShardId(String shardId) {
            this.shardId = shardId;
        }

        @Override
        public String getShardId() {
            return shardId;
        }
    }

    private static class CachingContextWriter extends AbstractContextWriter<BulkIngestKey,Value> {
        private final Multimap<BulkIngestKey,Value> cache = LinkedListMultimap.create();

        @Override
        protected void flush(Multimap<BulkIngestKey,Value> entries, TaskInputOutputContext<?,?,BulkIngestKey,Value> context) {
            for (Map.Entry<BulkIngestKey,Value> entry : entries.entries()) {
                cache.put(entry.getKey(), entry.getValue());
            }
        }

        public Multimap<BulkIngestKey,Value> getCache() {
            return cache;
        }
    }
}
