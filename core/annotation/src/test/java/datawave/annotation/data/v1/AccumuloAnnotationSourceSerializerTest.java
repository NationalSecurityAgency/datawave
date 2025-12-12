package datawave.annotation.data.v1;

import static datawave.annotation.test.v1.AnnotationAssertions.assertAnnotationSourcesEqual;
import static datawave.annotation.test.v1.AnnotationTestDataUtil.CREATED_DATE;
import static datawave.annotation.test.v1.AnnotationTestDataUtil.VISIBILITY;
import static datawave.annotation.test.v1.AnnotationTestDataUtil.generateTestAnnotationSource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
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
        AnnotationSource expectedAnnotationSource = AnnotationUtils.injectAnnotationSourceHashes(testAnnotationSource);

        // set up the object to test
        DefaultVisibilityTransformer visibilityTransformer = new DefaultVisibilityTransformer();
        DefaultTimestampTransformer timestampTransformer = new DefaultTimestampTransformer();
        AnnotationSerializer<Iterator<Map.Entry<Key,Value>>,AnnotationSource> serializer = new AccumuloAnnotationSourceSerializer(visibilityTransformer,
                        timestampTransformer);

        // serialize the annotation
        Iterator<Map.Entry<Key,Value>> results = serializer.serialize(expectedAnnotationSource);
        assertNotNull(results);

        // persist the results from the iterator so we can inspect then.
        final List<Map.Entry<Key,Value>> savedResults = new ArrayList<>();
        while (results.hasNext()) {
            savedResults.add(results.next());
        }
        assertFalse(savedResults.isEmpty());

        // Compare the serialized results with the input annotation.
        assertSerialization(expectedAnnotationSource, savedResults.iterator());

        // Deserialize the serialized results back into an annotation and compare.
        AnnotationSource observedAnnotationSource = serializer.deserialize(savedResults.iterator());
        assertAnnotationSourcesEqual(expectedAnnotationSource, observedAnnotationSource);
    }

    @Test
    public void testAnnotationSourceMultipleDeserialize() throws AnnotationSerializationException {

        // create two sources with only a different visibility and timestamp
        AnnotationSource partialAnnotationSourceOne = generateTestAnnotationSource();
        String expectedDateOne = "2025-10-01T08:00:00Z";
        String expectedVisibilityOne = "APPLES";
        //@formatter:off
        AnnotationSource testAnnotationSourceOne = partialAnnotationSourceOne.toBuilder()
                .clearMetadata()
                .putMetadata("visibility", expectedVisibilityOne)
                .putMetadata("created_date",expectedDateOne)
                .build();
        //@formatter:on
        AnnotationSource expectedAnnotationSourceOne = AnnotationUtils.injectAnnotationSourceHashes(testAnnotationSourceOne);

        AnnotationSource partialAnnotationSourceTwo = generateTestAnnotationSource();
        String expectedDateTwo = "2025-10-01T16:00:00Z";
        String expectedVisibilityTwo = "ORANGES";
        //@formatter:off
        AnnotationSource testAnnotationSourceTwo = partialAnnotationSourceTwo.toBuilder()
                .clearMetadata()
                .putMetadata("visibility", expectedVisibilityTwo)
                .putMetadata("created_date",expectedDateTwo)
                .build();
        //@formatter:on
        AnnotationSource expectedAnnotationSourceTwo = AnnotationUtils.injectAnnotationSourceHashes(testAnnotationSourceTwo);

        String expectedCombinedVisibility = "(APPLES|ORANGES)";

        // validate that the hashes are the same, but the metadata isn't so that we
        // have the correct conditions for the test.
        assertEquals(expectedAnnotationSourceOne.getAnalyticSourceHash(), expectedAnnotationSourceTwo.getAnalyticSourceHash());
        assertEquals(expectedAnnotationSourceOne.getAnalyticHash(), expectedAnnotationSourceTwo.getAnalyticHash());
        assertNotEquals(expectedAnnotationSourceOne.getMetadataMap(), expectedAnnotationSourceTwo.getMetadataMap());

        // set up the serializers that will be doing the work.
        DefaultVisibilityTransformer visibilityTransformer = new DefaultVisibilityTransformer();
        DefaultTimestampTransformer timestampTransformer = new DefaultTimestampTransformer();
        AccumuloAnnotationSourceSerializer serializer = new AccumuloAnnotationSourceSerializer(visibilityTransformer, timestampTransformer);

        // merge the serialization results into a single map to simulate an accumulo return.
        Iterator<Map.Entry<Key,Value>> resultsOne = serializer.serialize(expectedAnnotationSourceOne);
        Iterator<Map.Entry<Key,Value>> resultsTwo = serializer.serialize(expectedAnnotationSourceTwo);

        List<Map.Entry<Key,Value>> merged = new ArrayList<>();
        resultsOne.forEachRemaining(merged::add);
        resultsTwo.forEachRemaining(merged::add);

        // deserialize and check the results.
        AnnotationSource observedSource = serializer.deserialize(merged.iterator());
        assertEquals(expectedAnnotationSourceOne.getAnalyticSourceHash(), observedSource.getAnalyticSourceHash());
        assertEquals(expectedAnnotationSourceOne.getAnalyticHash(), observedSource.getAnalyticHash());
        assertNotEquals(expectedAnnotationSourceOne.getMetadataMap(), expectedAnnotationSourceTwo.getMetadataMap());
        Map<String,String> observedMetadata = observedSource.getMetadataMap();
        String observedVisibility = observedMetadata.get(DefaultVisibilityTransformer.VISIBILITY_METADATA_KEY);
        assertEquals(expectedCombinedVisibility, observedVisibility, "Did not properly combine the visibilities");
        String observedCreatedDate = observedMetadata.get(DefaultTimestampTransformer.CREATED_DATE_METADATA_KEY);
        assertEquals(expectedDateTwo, observedCreatedDate, "Did not choose the most recent of the two created dates");
    }

    private void assertSerialization(AnnotationSource expected, Iterator<Map.Entry<Key,Value>> results) throws InvalidProtocolBufferException {
        final List<Map.Entry<String,String>> observedConfiguration = new ArrayList<>();

        int resultCount = 0;

        while (results.hasNext()) {
            Map.Entry<Key,Value> e = results.next();

            // compare the components of the key..
            Key key = e.getKey();
            log.debug("Iterated key: '{}'", e.getKey());
            resultCount++;

            // rowId and column family
            assertEquals(expected.getAnalyticSourceHash(), key.getRow().toString(), "Row id mismatch");
            assertEquals(AccumuloAnnotationSourceSerializer.DATA_COLUMN_FAMILY, key.getColumnFamily().toString(), "Column family mismatch");

            // validate the structure of the column qualifier
            String columnQualifier = key.getColumnQualifier().toString();
            String[] parts = columnQualifier.split("\0");
            assertEquals(3, parts.length);

            String engine = parts[0];
            String model = parts[1];
            String analyticHash = parts[2];

            assertEquals(expected.getEngine(), engine, "Engine mismatch");
            assertEquals(expected.getModel(), model, "Model mismatch");
            assertEquals(expected.getAnalyticHash(), analyticHash, "Analytic hash mismatch");

            // visibility
            String columnVisibility = key.getColumnVisibility().toString();
            assertEquals(VISIBILITY, columnVisibility, "Visibility mismatch");

            // timestamp
            long timestamp = key.getTimestamp();
            String createdDate = Instant.ofEpochMilli(timestamp).toString();
            assertEquals(CREATED_DATE, createdDate, "Created date mismatch");

            // compare the expected value
            byte[] data = e.getValue().get();
            assertNotNull(data);
            assertTrue(data.length > 0);

            // will compare the source is more detail elsewhere.
            AnnotationSource.parseFrom(data);
        }

        // there should be only one.
        assertEquals(1, resultCount);
    }
}
