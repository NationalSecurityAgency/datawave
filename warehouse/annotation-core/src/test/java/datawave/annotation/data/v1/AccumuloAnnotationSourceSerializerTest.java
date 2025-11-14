package datawave.annotation.data.v1;

import static datawave.annotation.data.v1.AccumuloAnnotationSourceSerializer.COLUMN_FAMILIES;
import static datawave.annotation.data.v1.AccumuloAnnotationSourceSerializer.CONFIG_COLUMN_FAMILY;
import static datawave.annotation.data.v1.AccumuloAnnotationSourceSerializer.ENGINE_COLUMN_FAMILY;
import static datawave.annotation.data.v1.AccumuloAnnotationSourceSerializer.MODEL_COLUMN_FAMILY;
import static datawave.annotation.data.v1.AccumuloAnnotationSourceSerializer.SOURCE_LABEL_COLUMN_FAMILY;
import static datawave.annotation.test.v1.AnnotationAssertions.assertAnnotationSourcesEqual;
import static datawave.annotation.test.v1.AnnotationAssertions.assertMetadataEqual;
import static datawave.annotation.test.v1.AnnotationTestDataUtil.generateTestAnnotationSource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.HashMap;
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
import datawave.annotation.data.transform.DefaultTimestampTransformer;
import datawave.annotation.data.transform.DefaultVisibilityTransformer;
import datawave.annotation.protobuf.v1.AnnotationSource;
import datawave.annotation.util.v1.AnnotationUtils;

public class AccumuloAnnotationSourceSerializerTest {

    private static final Logger log = LoggerFactory.getLogger(AccumuloAnnotationSourceSerializerTest.class);

    @Test
    public void testAnnotationSourceSerializerDeserialize() throws AnnotationSerializationException, InvalidProtocolBufferException {
        AnnotationSource testAnnotationSource = generateTestAnnotationSource();
        // an id must be assigned to serialize/deserialize an annotation - typically this is handled by the data
        // access object.
        AnnotationSource expectedAnnotationSource = AnnotationUtils.injectAnnotationSourceId(testAnnotationSource);

        DefaultVisibilityTransformer visibilityTransformer = new DefaultVisibilityTransformer();
        DefaultTimestampTransformer timestampTransformer = new DefaultTimestampTransformer();
        AnnotationSerializer<Iterator<Map.Entry<Key,Value>>,AnnotationSource> serializer = new AccumuloAnnotationSourceSerializer(visibilityTransformer,
                        timestampTransformer);
        Iterator<Map.Entry<Key,Value>> results = serializer.serialize(expectedAnnotationSource);
        assertNotNull(results);

        // persist the results from the iterator so we can inspect them later.
        final List<Map.Entry<Key,Value>> savedResults = new ArrayList<>();
        while (results.hasNext()) {
            savedResults.add(results.next());
        }
        assertFalse(savedResults.isEmpty());

        // deserialize the results back into an annotation.
        AnnotationSource observedAnnotationSource = serializer.deserialize(savedResults.iterator());

        // Compare the serialized results with what's in Accumulo
        // - we need to remove the created_date config here b/c that's written to the timestamp.
        Map<String,String> iteratedConfiguration = new HashMap<>(expectedAnnotationSource.getConfigurationMap());
        for (String removedField : timestampTransformer.getTimestampFields()) {
            iteratedConfiguration.remove(removedField);
        }
        AnnotationSource expectedAnnotationIterated = expectedAnnotationSource.toBuilder().clearConfiguration().putAllConfiguration(iteratedConfiguration)
                        .build();
        assertSerialization(expectedAnnotationIterated, savedResults.iterator());

        // Compare the deserialized results with the original annotation.
        assertAnnotationSourcesEqual(expectedAnnotationSource, observedAnnotationSource);
    }

    private void assertSerialization(AnnotationSource expected, Iterator<Map.Entry<Key,Value>> results) throws InvalidProtocolBufferException {
        final List<Map.Entry<String,String>> observedConfiguration = new ArrayList<>();
        final Value EMPTY = new Value();

        boolean seenEngine = false;
        boolean seenModel = false;
        boolean seenSourceLabel = false;
        boolean seenConfig = false;

        while (results.hasNext()) {
            Map.Entry<Key,Value> e = results.next();
            Key key = e.getKey();

            log.debug("Iterated key: '{}'", e.getKey());

            Value value = e.getValue();
            assertEquals(EMPTY, value);

            assertEquals(expected.getAnalyticHash(), key.getRow().toString(), "Row id mismatch");
            String columnFamily = key.getColumnFamily().toString();
            String columnQualifier = key.getColumnQualifier().toString();

            switch (columnFamily) {
                case ENGINE_COLUMN_FAMILY:
                    if (seenEngine) {
                        fail("Multiple 'engine' entries seen in columnFamily: '" + key + "'");
                    }
                    assertEquals(expected.getEngine(), columnQualifier);
                    seenEngine = true;
                    break;
                case MODEL_COLUMN_FAMILY:
                    if (seenModel) {
                        fail("Multiple 'model' entries seen in columnFamily: " + key + "'");
                    }
                    assertEquals(expected.getModel(), columnQualifier);
                    seenModel = true;
                    break;
                case SOURCE_LABEL_COLUMN_FAMILY:
                    if (seenSourceLabel) {
                        fail("Multiple 'sourceLabel' entries seen in columnFamily: " + key + "'");
                    }
                    assertEquals(expected.getSourceLabel(), columnQualifier);
                    seenSourceLabel = true;
                    break;
                case CONFIG_COLUMN_FAMILY:
                    String[] cqParts = key.getColumnQualifier().toString().split("\0");
                    assertEquals(2, cqParts.length);
                    observedConfiguration.add(Map.entry(cqParts[0], cqParts[1]));
                    seenConfig = true;
                    break;
                default:
                    fail("Column family '" + columnFamily + "' was not in list of expected column families: '" + COLUMN_FAMILIES + "', '" + key + "'");
            }
        }

        assertTrue(seenEngine);
        assertTrue(seenModel);
        assertTrue(seenModel);
        assertTrue(seenConfig);

        assertMetadataEqual(expected.getConfigurationMap(), observedConfiguration);
    }
}
