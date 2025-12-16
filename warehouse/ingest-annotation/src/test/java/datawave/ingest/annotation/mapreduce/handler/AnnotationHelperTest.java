package datawave.ingest.annotation.mapreduce.handler;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;

import datawave.annotation.data.AnnotationWriteException;
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
        String[] expectedTableNames = new String[] {conf.get(AnnotationHelper.ANNOTATION_TNAME), conf.get(AnnotationHelper.ANNOTATION_SOURCE_TNAME)};

        assertArrayEquals(expectedTableNames, annotationHelper.getAnnotationTableNames(null));
        assertArrayEquals(expectedTableNames, annotationHelper.getAnnotationTableNames(new String[] {}));
        assertArrayEquals(new String[] {"test", expectedTableNames[0], expectedTableNames[1]}, annotationHelper.getAnnotationTableNames(new String[] {"test"}));

        int[] expectedTablePriorities = new int[] {conf.getInt(AnnotationHelper.ANNOTATION_TABLE_LOAD_PRIORITY, -10),
                conf.getInt(AnnotationHelper.ANNOTATION_SOURCE_TABLE_LOAD_PRIORITY, -10)};

        assertArrayEquals(expectedTablePriorities, annotationHelper.getAnnotationTableLoaderPriorities(null));
        assertArrayEquals(expectedTablePriorities, annotationHelper.getAnnotationTableLoaderPriorities(new int[] {}));
        assertArrayEquals(new int[] {1, expectedTablePriorities[0], expectedTablePriorities[1]},
                        annotationHelper.getAnnotationTableLoaderPriorities(new int[] {1}));
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

        // no fields are required
        HashMultimap<String,NormalizedContentInterface> fields = HashMultimap.create();

        annotationHelper.process(event, contextWriter, ctx, statusReporter, event.getId(), event.getVisibility().flatten(), event.getShardId().getBytes(),
                        ClassLoader.getSystemResource("input/singleAnnotation.json").openStream().readAllBytes());
        contextWriter.commit(ctx);

        BulkIngestKey expectedKey = new BulkIngestKey(new Text("datawave.annotation"),
                        new Key("20251107_1", "myannotation\0a.b.c\0testAnnotationType", "87C7ABE2\0seg\0testSegmentId1", "TEST_VISIBILITY", time));

        BulkIngestKey expectedSourceKey = new BulkIngestKey(new Text("datawave.annotationSource"),
                        new Key("F1A0463C207B3778B472B506F3F8351A", "d", "testEngine\u00006.7\u0000testAnalyticHash", new ColumnVisibility("PRIVATE"), time));

        assertFalse(contextWriter.getCache().get(expectedSourceKey).isEmpty());

        // the first segment value should be protobuf that can be parsed by Segment class
        Segment segment = Segment.parseFrom(contextWriter.getCache().get(expectedKey).stream().findFirst().get().get());
        assertEquals("testSegmentId1", segment.getSegmentHash(), "BulkIngestKey structure could potentially change as annotation-core library gets updated.");
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
                        new Key("20251107_1", "myannotation\0a.b.c\0testAnnotationType", "87C7ABE2\0seg\0testSegmentId1", "TEST_VISIBILITY", time));
        // checking to make sure the first segment BulkIngestKey is created
        assertTrue(bulkKeys.containsKey(expectedKey), "BulkIngestKey structure could potentially change as annotation-core library gets updated.");

        // the first segment value should be protobuf that can be parsed by Segment class
        Segment segment = Segment.parseFrom(bulkKeys.get(expectedKey).stream().findFirst().get().get());
        assertEquals("testSegmentId1", segment.getSegmentHash(), "BulkIngestKey structure could potentially change as annotation-core library gets updated.");
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
