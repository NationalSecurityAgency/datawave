package datawave.annotation.data.transform;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.jupiter.api.Test;

import datawave.annotation.data.AnnotationSerializationException;
import datawave.annotation.data.v1.AccumuloAnnotationSerializer;
import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.test.v1.AnnotationTestDataUtil;
import datawave.annotation.util.v1.AnnotationUtils;

public class TimestampTransformerTest {

    /**
     * assert that the default transformer will take the Timestamp metadata attribute and use it to populate the ColumnTimestamp on every key the serializer
     * generates
     *
     * @throws AnnotationSerializationException
     *             if there is a serialization problem.
     */
    @Test
    public void testDefaultTimestampTransformer() throws AnnotationSerializationException {
        AccumuloAnnotationSerializer serializer = new AccumuloAnnotationSerializer();
        Annotation baseAnnotation = AnnotationTestDataUtil.generateTestAnnotation();
        Annotation partialAnnotation = AnnotationUtils.injectAnnotationHash(baseAnnotation);
        Map<String,String> metadata = new HashMap<>();
        metadata.put("created_date", "2025-10-14T20:17:46.384Z");
        metadata.put("updated_date", "2025-10-15T09:32:19.483Z");
        Annotation annotationWithTimestamp = partialAnnotation.toBuilder().putAllMetadata(metadata).build();

        Iterator<Map.Entry<Key,Value>> results = serializer.serialize(annotationWithTimestamp);
        while (results.hasNext()) {
            Map.Entry<Key,Value> e = results.next();
            Key k = e.getKey();
            assertEquals(1760473066384L, k.getTimestamp());
        }
    }

    /**
     * assert that a custom transformer will take the Timestamp metadata attribute and use it to populate the ColumnTimestamp on every key the serializer
     * generates
     *
     * @throws AnnotationSerializationException
     *             if there is a serialization problem.
     */
    @Test
    public void testCustomTimestampTransformer() throws AnnotationSerializationException {
        VisibilityTransformer visibilityTransformer = new DefaultVisibilityTransformer();
        TimestampTransformer timestampTransformer = new TestTimestampTransformer();
        AccumuloAnnotationSerializer serializer = new AccumuloAnnotationSerializer(visibilityTransformer, timestampTransformer);
        Annotation baseAnnotation = AnnotationTestDataUtil.generateTestAnnotation();
        Annotation partialAnnotation = AnnotationUtils.injectAnnotationHash(baseAnnotation);
        Map<String,String> metadata = new HashMap<>();
        metadata.put("created_date", "2025-10-14T20:17:46.384Z");
        metadata.put("updated_date", "2025-10-15T09:32:19.483Z");

        Annotation annotationWithTimestamp = partialAnnotation.toBuilder().putAllMetadata(metadata).build();
        Iterator<Map.Entry<Key,Value>> results = serializer.serialize(annotationWithTimestamp);
        while (results.hasNext()) {
            Map.Entry<Key,Value> e = results.next();
            Key k = e.getKey();
            assertNotEquals(1760473066384L, k.getTimestamp());
            assertEquals(1760520739483L, k.getTimestamp());
        }
    }

    /** Non-trivial test transformer with no type/error checking, could be used as a template for more complex implementations */
    static class TestTimestampTransformer implements TimestampTransformer {
        @Override
        public Long fromMetadataMap(Map<String,String> metadataMap) throws AnnotationTransformException {
            String createdDateString = metadataMap.get("updated_date");
            try {
                return Instant.parse(createdDateString).toEpochMilli();
            } catch (DateTimeParseException | NullPointerException e) {
                throw new AnnotationTransformException("Exception parsing date for timestamp: '" + createdDateString + "'", e);
            }
        }

        @Override
        public Map<String,String> toMetadataMap(Long timestamp) {
            String iso8601 = Instant.ofEpochMilli(timestamp).toString();
            return Map.of("updated_date", iso8601);
        }

        public Set<String> getMetadataFields() {
            return Set.of("updated_date");
        }

        @Override
        public Map<String,String> mergeMetadataMaps(Map<String,String> first, Map<String,String> second) {
            return Map.of("updated_date", first.get("updated_date"));
        }
    }
}
