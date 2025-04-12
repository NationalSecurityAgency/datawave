package datawave.annotation.data.v0;

import static datawave.annotation.util.v0.AnnotationTestUtil.assertAnnotationsEqual;
import static datawave.annotation.util.v0.AnnotationTestUtil.assertMetadataEqual;
import static datawave.annotation.util.v0.AnnotationTestUtil.assertSegmentsEqual;
import static datawave.annotation.util.v0.AnnotationTestUtil.generateTestAnnotation;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.annotation.data.AnnotationSerializationException;
import datawave.annotation.data.AnnotationSerializer;
import datawave.annotation.model.v0.Annotation;
import datawave.annotation.model.v0.Segment;
import datawave.annotation.protobuf.v0.SegmentData;

public class AccumuloAnnotationSerializerTest {

    private static final Logger log = LoggerFactory.getLogger(AccumuloAnnotationSerializerTest.class);

    public static final String TABLE_NAME = "testAnnotations";
    protected AccumuloClient client;
    protected TableOperations tableOperations;

    @Before
    public void setup() throws AccumuloSecurityException, AccumuloException, TableNotFoundException, TableExistsException {
        InMemoryInstance i = new InMemoryInstance(this.getClass().toString());
        client = new InMemoryAccumuloClient("root", i);
        if (client.tableOperations().exists(TABLE_NAME))
            client.tableOperations().delete(TABLE_NAME);
        client.tableOperations().create(TABLE_NAME);
        tableOperations = client.tableOperations();
    }

    @Test
    public void testAnnotationSerializerDeserialize() throws AnnotationSerializationException, InvalidProtocolBufferException {
        Annotation testAnnotation = generateTestAnnotation();
        AnnotationSerializer<Iterator<Map.Entry<Key,Value>>,Annotation> serializer = new AccumuloAnnotationSerializer();
        Iterator<Map.Entry<Key,Value>> results = serializer.serialize(testAnnotation);
        assertNotNull(results);

        // persist the results from the iterator so we can inspect them later.
        final List<Map.Entry<Key,Value>> savedResults = new ArrayList<>();
        while (results.hasNext()) {
            savedResults.add(results.next());
        }
        assertFalse(savedResults.isEmpty());

        // deserialize the results back into an annotation.
        Annotation observedAnnotation = serializer.deserialize(savedResults.iterator());

        // Compare the serialized results with what's expected
        assertSerialization(testAnnotation, savedResults.iterator());

        // Compare the deserialized results with the original annotation.
        assertAnnotationsEqual(testAnnotation, observedAnnotation);
    }

    private void assertSerialization(Annotation expected, Iterator<Map.Entry<Key,Value>> results) throws InvalidProtocolBufferException {
        final List<Map.Entry<String,String>> observedMetadata = new ArrayList<>();
        final List<Segment> observedSegments = new ArrayList<>();

        while (results.hasNext()) {
            Map.Entry<Key,Value> e = results.next();
            Key key = e.getKey();

            log.debug("Iterated key: '{}'", e.getKey());

            Value value = e.getValue();

            assertEquals("Row id mismatch", "20250704_249", key.getRow().toString());
            assertEquals("Column family mismatch", "testDataType\0abcde.fghij.klmno\0testAnnotationType", key.getColumnFamily().toString());
            String cq = key.getColumnQualifier().toString();
            String[] parts = cq.split("\0");
            assertTrue("Column qualifier incorrect length", parts.length >= 2);
            String annotationId = parts[0];
            assertEquals("Annotation id mismatch", "kir5i4.tf9ozi.-ji6i29", annotationId);
            if (parts.length == 2) {
                String segmentId = parts[1];
                assertEquals("comhxz.qyfmph.dpbt8m", segmentId);

                // the value must be decode-able into SegmentData.
                SegmentData segmentData = SegmentData.parseFrom(value.get());
                observedSegments.add(Segment.newBuilder().setSegmentData(segmentData).build());

            }
            if (parts.length == 3) {
                observedMetadata.add(Map.entry(parts[1], parts[2]));
            }
        }

        assertSegmentsEqual(expected.getSegments(), observedSegments);
        assertMetadataEqual(expected.getMetadata(), observedMetadata);
    }
}
