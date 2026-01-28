package datawave.annotation.data.transform;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.junit.jupiter.api.Test;

import datawave.annotation.data.AnnotationSerializationException;
import datawave.annotation.data.v1.AccumuloAnnotationSerializer;
import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.test.v1.AnnotationTestDataUtil;
import datawave.annotation.util.v1.AnnotationUtils;

public class VisibilityTransformerTest {

    /**
     * assert that the default transformer will take the visibility metadata attribute and use it to populate the ColumnVisibility on every key the serializer
     * generates
     *
     * @throws AnnotationSerializationException
     *             if there is a serialization problem.
     */
    @Test
    public void testDefaultVisibilityTransformer() throws AnnotationSerializationException {
        AccumuloAnnotationSerializer serializer = new AccumuloAnnotationSerializer();
        Annotation baseAnnotation = AnnotationTestDataUtil.generateTestAnnotation();
        Annotation partialAnnotation = AnnotationUtils.injectAnnotationHash(baseAnnotation);
        Map<String,String> metadata = new HashMap<>();
        metadata.put("visibility", "PUBLIC");
        metadata.put("owner", "dad");
        metadata.put("dist", "family");
        metadata.put("archive", "forever");
        Annotation annotationWithVisibility = partialAnnotation.toBuilder().putAllMetadata(metadata).build();

        Iterator<Map.Entry<Key,Value>> results = serializer.serialize(annotationWithVisibility);
        while (results.hasNext()) {
            Map.Entry<Key,Value> e = results.next();
            Key k = e.getKey();
            assertEquals("PUBLIC", k.getColumnVisibility().toString());
        }
    }

    /**
     * assert that a custom transformer will take the visibility metadata attribute and use it to populate the ColumnVisibility on every key the serializer
     * generates
     *
     * @throws AnnotationSerializationException
     *             if there is a serialization problem.
     */
    @Test
    public void testCustomVisibilityTransformer() throws AnnotationSerializationException {
        VisibilityTransformer visibilityTransformer = new TestAnnotationVisibilityTransformer();
        TimestampTransformer timestampTransformer = new DefaultTimestampTransformer();
        AccumuloAnnotationSerializer serializer = new AccumuloAnnotationSerializer(visibilityTransformer, timestampTransformer);
        Annotation baseAnnotation = AnnotationTestDataUtil.generateTestAnnotation();
        Annotation partialAnnotation = AnnotationUtils.injectAnnotationHash(baseAnnotation);
        Map<String,String> metadata = new HashMap<>();
        metadata.put("visibility", "PUBLIC");
        metadata.put("owner", "dad");
        metadata.put("dist", "family");
        metadata.put("archive", "forever");
        Annotation annotationWithVisibility = partialAnnotation.toBuilder().putAllMetadata(metadata).build();
        Iterator<Map.Entry<Key,Value>> results = serializer.serialize(annotationWithVisibility);
        while (results.hasNext()) {
            Map.Entry<Key,Value> e = results.next();
            Key k = e.getKey();
            assertNotEquals("PUBLIC", k.getColumnVisibility().toString());
            assertEquals("A:forever|D:family|O:dad", k.getColumnVisibility().toString());
        }
    }

    /** Non-trivial test transformer with no type/error checking, could be used as a template for more complex implementations */
    static class TestAnnotationVisibilityTransformer implements VisibilityTransformer {

        // a list of metadata keys we'll use to determine visibility.
        private static final Map<String,String> visMapToColumnVis = Map.of("owner", "O", "dist", "D", "archive", "A");
        private static final Map<String,String> columnVisToVisMap = Map.of("O", "owner", "D", "dist", "A", "archive");

        @Override
        public Map<String,String> toMetadataMap(ColumnVisibility columnVisibility) throws AnnotationTransformException {
            Map<String,String> visibilityMap = new HashMap<>();
            String visibilityString = columnVisibility.toString();
            String[] parts = visibilityString.split("\\|");
            for (String part : parts) {
                int pos = part.indexOf(":");
                String key = part.substring(0, pos);
                String value = part.substring(pos + 1);
                String fieldName = transformKeyForVisibilityMap(key);
                String mapValue = transformValueForVisibilityMap(value);
                if (fieldName == null || fieldName.isEmpty()) {
                    throw new AnnotationTransformException("Unexpected prefix '" + key + "' found in visibility expression '" + visibilityString);
                }
                visibilityMap.put(fieldName, mapValue);
            }
            return visibilityMap;
        }

        @Override
        public ColumnVisibility fromMetadataMap(Map<String,String> visibilityMap) throws AnnotationTransformException {
            StringBuilder b = new StringBuilder();

            SortedMap<String,String> sortedVisibilityParts = new TreeMap<>();

            // transform and insert into a sorted tree to guarantee order by key.
            for (Map.Entry<String,String> e : visibilityMap.entrySet()) {
                String fieldName = e.getKey();
                String key = transformKeyForColumnVisibility(fieldName);
                if (key != null && !key.isEmpty()) {
                    String value = transformValueForColumnVisibility(e.getValue());
                    if (value != null && !value.isEmpty()) {
                        sortedVisibilityParts.put(key, value);
                    }
                }
            }

            // now that we're sorted, build the visibility string
            for (Map.Entry<String,String> e : sortedVisibilityParts.entrySet()) {
                b.append(e.getKey()).append(":").append(e.getValue()).append("|");

            }
            // remove trailing pipe symbol.
            if (b.length() > 0) {
                b.setLength(b.length() - 1);
            } else {
                throw new AnnotationTransformException("Did not see any visibility fields in visibility map: '" + visibilityMap + "'");
            }

            return new ColumnVisibility(b.toString());
        }

        @Override
        public Set<String> getMetadataFields() {
            return Set.of("owner", "dist", "archive");
        }

        @Override
        public Map<String,String> mergeMetadataMaps(Map<String,String> first, Map<String,String> second) {
            // not really a meaningful implementation, would be better if we compared the various value from
            // first and second and returned the 'best' ones.
            return Map.of("owner", first.get("owner"), "dist", first.get("dist"), "archive", second.get("archive"));
        }

        private static String transformKeyForVisibilityMap(String key) {
            return columnVisToVisMap.get(key);
        }

        private static String transformValueForVisibilityMap(String value) {
            return value;
        }

        private static String transformKeyForColumnVisibility(String key) {
            return visMapToColumnVis.get(key);
        }

        private static String transformValueForColumnVisibility(String value) {
            return value;
        }

    }
}
