package datawave.ingest.annotation.mapreduce.handler;

import static java.nio.charset.StandardCharsets.UTF_8;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.Segment;
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
    private Counters counters;
    private CachingContextWriter contextWriter;
    private MockStatusReporter statusReporter;

    @Before
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
        assertEquals(annotationHelper.getAnnotationTableNames(null)[0], conf.get(AnnotationHelper.ANNOTATION_TNAME));
        assertEquals(annotationHelper.getAnnotationTableNames(new String[] {})[0], conf.get(AnnotationHelper.ANNOTATION_TNAME));
        assertArrayEquals(annotationHelper.getAnnotationTableNames(new String[] {"test"}), new String[] {"test", conf.get(AnnotationHelper.ANNOTATION_TNAME)});

        assertEquals(annotationHelper.getAnnotationTableLoaderPriorities(null)[0], conf.getInt(AnnotationHelper.ANNOTATION_TABLE_LOAD_PRIORITY, -10));
        assertEquals(annotationHelper.getAnnotationTableLoaderPriorities(new int[] {})[0], conf.getInt(AnnotationHelper.ANNOTATION_TABLE_LOAD_PRIORITY, -10));
        assertArrayEquals(annotationHelper.getAnnotationTableLoaderPriorities(new int[] {1}),
                        new int[] {1, conf.getInt(AnnotationHelper.ANNOTATION_TABLE_LOAD_PRIORITY, -10)});
    }

    @Test
    public void testProcess() throws IOException, InterruptedException {
        // create test event record wrapper with minimum set of fields required for creating annotation mutations
        RawRecordContainer event = new EventWithShardId("20251107_1");

        long time = System.currentTimeMillis();
        event.setDataType(TypeRegistry.getType("annotation"));
        event.setTimestamp(time);
        event.setVisibility(new ColumnVisibility("TEST_VISIBILITY"));
        event.setId(UID.parse("a.b.c"));

        // no fields are required
        HashMultimap<String,NormalizedContentInterface> fields = HashMultimap.create();

        annotationHelper.process(event, contextWriter, ctx, statusReporter, event.getId(), event.getVisibility().flatten(), event.getShardId().getBytes(),
                        ClassLoader.getSystemResource("input/singleAnnotation.json").openStream().readAllBytes());
        contextWriter.commit(ctx);

        BulkIngestKey expectedKey = new BulkIngestKey(new Text("datawave.annotation"),
                        new Key("20251107_1", "myannotation\0a.b.c\0testAnnotationType", "testAnnotationId\0testSegmentId1", "TEST_VISIBILITY", time));

        // the first segment value should be protobuf that can be parsed by Segment class
        Segment segment = Segment.parseFrom(contextWriter.getCache().get(expectedKey).stream().findFirst().get().get());
        assertEquals("BulkIngestKey structure could potentially change as annotation-core library gets updated.", "testSegmentId1", segment.getSegmentId());
    }

    @Test
    public void testProcessBulk() throws IOException {
        // create test event record wrapper with minimum set of fields required for creating annotation mutations
        RawRecordContainer event = new EventWithShardId("20251107_1");

        long time = System.currentTimeMillis();
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
                        new Key("20251107_1", "myannotation\0a.b.c\0testAnnotationType", "testAnnotationId\0testSegmentId1", "TEST_VISIBILITY", time));
        // checking to make sure the first segment BulkIngestKey is created
        assertTrue("BulkIngestKey structure could potentially change as annotation-core library gets updated.", bulkKeys.containsKey(expectedKey));

        // the first segment value should be protobuf that can be parsed by Segment class
        Segment segment = Segment.parseFrom(bulkKeys.get(expectedKey).stream().findFirst().get().get());
        assertEquals("BulkIngestKey structure could potentially change as annotation-core library gets updated.", "testSegmentId1", segment.getSegmentId());
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
    public void testTransformJson() throws SaxonApiException {
        byte[] json = ("{\n" + "  \"annotationType\": \"testAnnotationType\",\n" + "  \"annotationId\": \"testAnnotationId\",\n"
                        + "  \"dataType\": \"testDataType\",\n" + "  \"documentId\": \"testDocumentId\",\n" + "  \"metadata\": {\n" + "  }\n" + "}\n")
                        .getBytes(UTF_8);

        // transformation strips out documentId from json
        assertFalse(annotationHelper.transformJson(json).contains("documentId"));
    }

    @Test
    public void testDisabledTransformJson() throws SaxonApiException {
        conf = new Configuration();
        conf.addResource(ClassLoader.getSystemResource("config/test-annotation-ingest-config-notransform.xml"));

        TypeRegistry.reset();
        TypeRegistry.getInstance(conf);

        annotationHelper = new AnnotationHelper(conf);

        byte[] json = ("{\n" + "  \"annotationType\": \"testAnnotationType\",\n" + "  \"annotationId\": \"testAnnotationId\",\n"
                        + "  \"dataType\": \"testDataType\",\n" + "  \"documentId\": \"testDocumentId\",\n" + "  \"metadata\": {\n" + "  }\n" + "}\n")
                        .getBytes(UTF_8);

        // transformation strips out documentId from json
        assertTrue(annotationHelper.transformJson(json).contains("documentId"));
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
