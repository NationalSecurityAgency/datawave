package datawave.annotation.data.v1;

import static datawave.annotation.data.v1.AccumuloAnnotationSerializer.DOCUMENT_CQ_FRAGMENT;
import static datawave.annotation.data.v1.AccumuloAnnotationSerializer.METADATA_CQ_FRAGMENT;
import static datawave.annotation.data.v1.AccumuloAnnotationSerializer.NULL;
import static datawave.annotation.data.v1.AccumuloAnnotationSerializer.SEGMENT_CQ_FRAGMENT;
import static datawave.annotation.data.v1.AccumuloAnnotationSerializer.SOURCE_CQ_FRAGMENT;
import static datawave.annotation.data.v1.AccumuloAnnotationSerializer.VALID_CQ_FRAGMENTS;
import static datawave.annotation.test.v1.AnnotationAssertions.assertAnnotationsEqual;
import static datawave.annotation.test.v1.AnnotationAssertions.assertMetadataEqual;
import static datawave.annotation.test.v1.AnnotationAssertions.assertSegmentsEqual;
import static datawave.annotation.test.v1.AnnotationTestDataUtil.generateTestAnnotation;
import static datawave.annotation.test.v1.AnnotationTestDataUtil.generateTestAnnotationSource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

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
import datawave.annotation.protobuf.v1.AnnotationSource;
import datawave.annotation.protobuf.v1.Segment;
import datawave.annotation.util.v1.AnnotationUtils;

public class AccumuloAnnotationSerializerTest {

    private static final Logger log = LoggerFactory.getLogger(AccumuloAnnotationSerializerTest.class);

    @Test
    public void testAnnotationSerializerDeserialize() throws AnnotationSerializationException, InvalidProtocolBufferException {
        Annotation baseAnnotation = generateTestAnnotation();
        AnnotationSource baseSource = generateTestAnnotationSource();

        AnnotationSource testSource = AnnotationUtils.injectAnnotationSourceHashes(baseSource);
        Annotation testAnnotation = AnnotationUtils.injectAnnotationSource(baseAnnotation, testSource);

        // an id must be assigned to serialize/deserialize an annotation - typically this is handled by the data
        // access object.
        Annotation expectedAnnotation = AnnotationUtils.injectAllHashes(testAnnotation);

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

    @Test
    public void testAnnotationSerializerDeserializeWithSourceAndDocument() throws AnnotationSerializationException, InvalidProtocolBufferException {
        Annotation baseAnnotation = generateTestAnnotation();

        // enrich the base annotation with a document id and a source id.
        Annotation testAnnotation = baseAnnotation.toBuilder().setDocumentId("a-good-document").setAnalyticSourceHash("a-good-source").build();

        // an id must be assigned to serialize/deserialize an annotation - typically this is handled by the data
        // access object.
        Annotation expectedAnnotation = AnnotationUtils.injectAllHashes(testAnnotation);

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
        final Segment expectedSegment = expected.getSegments(0);
        final String expectedCf = expected.getDataType() + NULL + expected.getUid() + NULL + expected.getAnnotationType();

        final List<Map.Entry<String,String>> observedMetadata = new ArrayList<>();
        final List<Segment> observedSegments = new ArrayList<>();

        while (results.hasNext()) {
            Map.Entry<Key,Value> e = results.next();
            Key key = e.getKey();

            log.debug("Iterated key: '{}'", e.getKey());

            Value value = e.getValue();
            assertEquals(expected.getShard(), key.getRow().toString(), "Row id mismatch");
            assertEquals(expectedCf, key.getColumnFamily().toString(), "Column family mismatch");

            String cq = key.getColumnQualifier().toString();
            String[] cqParts = cq.split("\0");
            assertTrue(cqParts.length >= 3, "Column qualifier too short");
            assertTrue(cqParts.length <= 4, "Column qualifier too long");

            String annotationHash = cqParts[0];
            assertEquals(expected.getAnnotationId(), annotationHash, "Annotation hash mismatch");
            assertTrue(VALID_CQ_FRAGMENTS.contains(cqParts[1]), "Observed invalid column qualifier fragment: " + cqParts[1]);

            switch (cqParts[1]) {
                case SOURCE_CQ_FRAGMENT:
                    assertEquals(expected.getAnalyticSourceHash(), cqParts[2]);
                    break;
                case DOCUMENT_CQ_FRAGMENT:
                    assertEquals(expected.getDocumentId(), cqParts[2]);
                    break;
                case METADATA_CQ_FRAGMENT:
                    observedMetadata.add(Map.entry(cqParts[2], cqParts[3]));
                    break;
                case SEGMENT_CQ_FRAGMENT:
                    assertEquals(expectedSegment.getSegmentHash(), cqParts[2]);
                    Segment segment = Segment.parseFrom(value.get());
                    observedSegments.add(segment);
                    break;
                default:
                    fail("Unexpected column qualifier fragment in key " + key);
            }

        }

        assertSegmentsEqual(expected.getSegmentsList(), observedSegments);
        assertMetadataEqual(expected.getMetadataMap(), observedMetadata);
    }
}
