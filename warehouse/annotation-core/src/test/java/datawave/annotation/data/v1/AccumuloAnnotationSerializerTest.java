package datawave.annotation.data.v1;

import static datawave.annotation.test.v1.AnnotationAssertions.assertAnnotationsEqual;
import static datawave.annotation.test.v1.AnnotationAssertions.assertMetadataEqual;
import static datawave.annotation.test.v1.AnnotationAssertions.assertSegmentsEqual;
import static datawave.annotation.test.v1.AnnotationTestDataUtil.generateTestAnnotation;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;

import datawave.annotation.data.AnnotationSerializationException;
import datawave.annotation.data.AnnotationSerializer;
import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.Segment;
import datawave.annotation.util.v1.AnnotationUtils;

public class AccumuloAnnotationSerializerTest {

    private static final Logger log = LoggerFactory.getLogger(AccumuloAnnotationSerializerTest.class);

    @Test
    public void testAnnotationSerializerDeserialize() throws AnnotationSerializationException, InvalidProtocolBufferException {
        Annotation testAnnotation = generateTestAnnotation();
        // an id must be assigned to serialize/deserialize an annotation - typically this is handled by the data
        // access object.
        Annotation expectedAnnotation = AnnotationUtils.injectAnnotationAndSegmentIds(testAnnotation);

        AnnotationSerializer<Iterator<Map.Entry<Key,Value>>,Annotation> serializer = new AccumuloAnnotationSerializer();
        Iterator<Map.Entry<Key,Value>> results = serializer.serialize(expectedAnnotation);
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
        assertSerialization(expectedAnnotation, savedResults.iterator());

        // Compare the deserialized results with the original annotation.
        assertAnnotationsEqual(expectedAnnotation, observedAnnotation);
    }

    private void assertSerialization(Annotation expected, Iterator<Map.Entry<Key,Value>> results) throws InvalidProtocolBufferException {
        final List<Map.Entry<String,String>> observedMetadata = new ArrayList<>();
        final List<Segment> observedSegments = new ArrayList<>();

        while (results.hasNext()) {
            Map.Entry<Key,Value> e = results.next();
            Key key = e.getKey();

            log.debug("Iterated key: '{}'", e.getKey());

            Value value = e.getValue();

            assertEquals("20250704_249", key.getRow().toString(), "Row id mismatch");
            assertEquals("testDataType\0abcde.fghij.klmno\0testAnnotationType", key.getColumnFamily().toString(), "Column family mismatch");
            String cq = key.getColumnQualifier().toString();
            String[] parts = cq.split("\0");
            assertTrue(parts.length >= 2, "Column qualifier incorrect length");
            String annotationId = parts[0];
            assertEquals("a75beb9e", annotationId, "Annotation id mismatch");
            if (parts.length == 2) {
                String segmentId = parts[1];
                assertEquals("5a7bcdd9", segmentId);

                // the value must be decode-able into SegmentData.
                Segment segment = Segment.parseFrom(value.get());
                observedSegments.add(segment);

            }
            if (parts.length == 3) {
                observedMetadata.add(Map.entry(parts[1], parts[2]));
            }
        }

        assertSegmentsEqual(expected.getSegmentsList(), observedSegments);
        assertMetadataEqual(expected.getMetadataMap(), observedMetadata);
    }
}
