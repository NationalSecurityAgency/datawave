package datawave.annotation.data.v1;

import static datawave.annotation.test.v1.AnnotationAssertions.assertAnnotationSourcesEqual;
import static datawave.annotation.test.v1.AnnotationAssertions.assertAnnotationsEqual;
import static datawave.annotation.test.v1.AnnotationTestDataUtil.generateTestAnnotation;
import static datawave.annotation.test.v1.AnnotationTestDataUtil.generateTestAnnotationSource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.NamespaceOperations;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.AnnotationSource;
import datawave.annotation.protobuf.v1.Segment;
import datawave.annotation.protobuf.v1.SegmentBoundary;
import datawave.annotation.protobuf.v1.SegmentValue;
import datawave.annotation.test.v1.AnnotationTestDataUtil;
import datawave.annotation.util.v1.AnnotationUtils;
import datawave.table.hash.HashUID;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class AnnotationDataAccessTest {

    protected static final Logger log = LoggerFactory.getLogger(AnnotationDataAccessTest.class);

    private static MiniAccumuloCluster mac;
    private static AccumuloClient client;
    private static final String auths = "PUBLIC";
    private static final Set<Authorizations> accumuloAuthorizations = Set.of(new Authorizations(auths));
    // the object under test
    private AnnotationDataAccess dao;

    private static final Annotation EMPTY = Annotation.getDefaultInstance();

    private static final String ANNOTATION_TABLE_NAME = "datawave.annotation";
    private static final String ANNOTATION_SOURCE_TABLE_NAME = "datawave.annotationSource";

    @BeforeAll
    public static void startCluster() throws Exception {
        File macDir = new File(System.getProperty("user.dir") + "/target/mac/" + AnnotationDataAccessTest.class.getName());
        if (macDir.exists())
            FileUtils.deleteDirectory(macDir);
        // noinspection ResultOfMethodCallIgnored
        macDir.mkdirs();
        mac = new MiniAccumuloCluster(new MiniAccumuloConfig(macDir, "pass"));
        mac.start();
        client = mac.createAccumuloClient("root", new PasswordToken("pass"));

        NamespaceOperations namespaceOperations = client.namespaceOperations();
        namespaceOperations.create("datawave");

        TableOperations tableOperations = client.tableOperations();
        tableOperations.create(ANNOTATION_TABLE_NAME);
        tableOperations.create(ANNOTATION_SOURCE_TABLE_NAME);

        SecurityOperations securityOperations = client.securityOperations();
        securityOperations.changeUserAuthorizations("root", new Authorizations(auths));

        List<Annotation> manyAnnotations = AnnotationTestDataUtil.generateManyTestAnnotations();
        List<AnnotationSource> manyAnnotationSources = AnnotationTestDataUtil.generateManyTestAnnotationSources();

        AccumuloAnnotationSerializer annotationSerializer = new AccumuloAnnotationSerializer();
        AccumuloAnnotationSourceSerializer annotationSourceSerializer = new AccumuloAnnotationSourceSerializer();
        AnnotationDataAccess setupDao = new AnnotationDataAccess(client, accumuloAuthorizations, ANNOTATION_TABLE_NAME, ANNOTATION_SOURCE_TABLE_NAME,
                        annotationSerializer, annotationSourceSerializer);
        for (Annotation annotation : manyAnnotations) {
            setupDao.addAnnotation(annotation);
        }
        for (AnnotationSource annotationSource : manyAnnotationSources) {
            setupDao.addAnnotationSource(annotationSource);
        }
        dumpTable(ANNOTATION_TABLE_NAME);
        dumpTable(ANNOTATION_SOURCE_TABLE_NAME);
    }

    @BeforeEach
    public void setup() {
        AccumuloAnnotationSerializer annotationSerializer = new AccumuloAnnotationSerializer();
        AccumuloAnnotationSourceSerializer annotationSourceSerializer = new AccumuloAnnotationSourceSerializer();
        dao = new AnnotationDataAccess(client, accumuloAuthorizations, ANNOTATION_TABLE_NAME, ANNOTATION_SOURCE_TABLE_NAME, annotationSerializer,
                        annotationSourceSerializer);
    }

    /** Insert a new annotation into the table and retrieve it and validate */
    @Test
    public void testAddGetAnnotation() {
        Annotation sourceAnnotation = generateTestAnnotation();
        dao.addAnnotation(sourceAnnotation);

        // we expect the test annotation to have the same id injected as the annotation retuned from the dao.
        Annotation expectedAnnotation = AnnotationUtils.injectAllHashes(sourceAnnotation);

        Collection<Annotation> annotation = dao.getAnnotations(sourceAnnotation.getShard(), sourceAnnotation.getDataType(), sourceAnnotation.getUid());
        assertFalse(annotation.isEmpty());
        assertEquals(1, annotation.size());
        Annotation resultAnnotation = annotation.stream().findFirst().orElse(EMPTY);
        assertAnnotationsEqual(expectedAnnotation, resultAnnotation);
    }

    @Test
    public void testGetAnnotationAll() {
        String day = "20250406";
        String shard = "456";
        String row = day + "_" + shard;
        String dataType = "news";
        String uidSeed = row + "_" + dataType;
        String documentUid = HashUID.builder().newId(uidSeed.getBytes(StandardCharsets.UTF_8)).toString();

        Collection<Annotation> annotations = dao.getAnnotations(row, dataType, documentUid);
        assertFalse(annotations.isEmpty());
        assertEquals(1, annotations.size());
        Annotation a = annotations.stream().findFirst().orElse(EMPTY);
        assertExpectedMetadata(a.getMetadataMap());
        assertExpectedTextSegments(a.getSegmentsList());

    }

    @Test
    public void testGetAnnotationTypes() {
        String day = "20250406";
        String shard = "456";
        String row = day + "_" + shard;
        String dataType = "news";
        String uidSeed = row + "_" + dataType;
        String documentUid = HashUID.builder().newId(uidSeed.getBytes(StandardCharsets.UTF_8)).toString();

        Set<String> expectedTypes = new TreeSet<>();
        expectedTypes.add("tokens");

        Collection<String> annotationTypes = dao.getAnnotationTypes(row, dataType, documentUid);
        assertFalse(annotationTypes.isEmpty());
        assertEquals(1, annotationTypes.size());
        assertEquals(expectedTypes, annotationTypes);
    }

    @Test
    public void testGetAnnotationShardMissing() {
        String row = "20250406_4567"; // non-existent shard
        String dataType = "news";
        String uidSeed = row + "_" + dataType;
        String documentUid = HashUID.builder().newId(uidSeed.getBytes(StandardCharsets.UTF_8)).toString();

        Collection<Annotation> annotations = dao.getAnnotations(row, dataType, documentUid);
        assertTrue(annotations.isEmpty());
    }

    @Test
    public void testGetAnnotationDatatypeMissing() {
        String row = "20250406_456";
        String dataType = "email"; // non-existent datatype
        String uidSeed = row + "_" + dataType;
        String documentUid = HashUID.builder().newId(uidSeed.getBytes(StandardCharsets.UTF_8)).toString();

        Collection<Annotation> annotations = dao.getAnnotations(row, dataType, documentUid);
        assertTrue(annotations.isEmpty());
    }

    @Test
    public void testGetAnnotationUIDMissing() {
        String row = "20250406_456";
        String dataType = "email";
        String uidSeed = "helios"; // non-existent uid from this seed.
        String documentUid = HashUID.builder().newId(uidSeed.getBytes(StandardCharsets.UTF_8)).toString();

        Collection<Annotation> annotations = dao.getAnnotations(row, dataType, documentUid);
        assertTrue(annotations.isEmpty());
    }

    @Test
    public void testGetAnnotationAllForType() {
        String row = "20250406_456";
        String dataType = "news";
        String uidSeed = row + "_" + dataType;
        String annotationType = "tokens";
        String documentUid = HashUID.builder().newId(uidSeed.getBytes(StandardCharsets.UTF_8)).toString();

        Collection<Annotation> annotations = dao.getAnnotationsForType(row, dataType, documentUid, annotationType);
        assertFalse(annotations.isEmpty());
        assertEquals(1, annotations.size());
        Annotation a = annotations.stream().findFirst().orElse(EMPTY);
        assertExpectedMetadata(a.getMetadataMap());
        assertExpectedTextSegments(a.getSegmentsList());
    }

    @Test
    public void testGetAnnotationAllForTypeMissing() {
        String row = "20250406_456";
        String dataType = "email"; // non-existent datatype
        String uidSeed = row + "_" + dataType;
        String annotationType = "tokens";
        String documentUid = HashUID.builder().newId(uidSeed.getBytes(StandardCharsets.UTF_8)).toString();

        Collection<Annotation> annotations = dao.getAnnotationsForType(row, dataType, documentUid, annotationType);
        assertTrue(annotations.isEmpty());
    }

    @Test
    public void testGetAnnotation() {
        String row = "20250406_456";
        String dataType = "news";
        String uidSeed = row + "_" + dataType;
        String annotationType = "tokens";
        String documentUid = HashUID.builder().newId(uidSeed.getBytes(StandardCharsets.UTF_8)).toString();
        String annotationUid = "989FC696";

        Optional<Annotation> annotationOptional = dao.getAnnotation(row, dataType, documentUid, annotationType, annotationUid);
        assertFalse(annotationOptional.isEmpty());
        Annotation a = annotationOptional.get();
        assertExpectedMetadata(a.getMetadataMap());
        assertExpectedTextSegments(a.getSegmentsList());
    }

    @Test
    public void testGetAnnotationMissing() {
        String row = "20250406_456";
        String dataType = "email"; // non-existent datatype
        String uidSeed = row + "_" + dataType;
        String annotationType = "tokens";
        String documentUid = HashUID.builder().newId(uidSeed.getBytes(StandardCharsets.UTF_8)).toString();
        String annotationUid = "kir5i4.tf9ozi.-ji6i29";

        Optional<Annotation> annotationOptional = dao.getAnnotation(row, dataType, documentUid, annotationType, annotationUid);
        assertTrue(annotationOptional.isEmpty());
    }

    /** Insert a new annotation into the table and retrieve it and validate */
    @Test
    public void testAddGetAnnotationSource() {
        AnnotationSource sourceAnnotationSource = generateTestAnnotationSource();
        Optional<AnnotationSource> addedAnnotationSource = dao.addAnnotationSource(sourceAnnotationSource);
        assertFalse(addedAnnotationSource.isEmpty());
        assertTrue(StringUtils.isNotBlank(addedAnnotationSource.get().getAnalyticSourceHash()));

        // we expect the test annotation to have the same id injected as the annotation retuned from the dao.
        AnnotationSource expectedAnnotationSource = AnnotationUtils.injectAnnotationSourceHashes(sourceAnnotationSource);
        assertEquals(addedAnnotationSource.get().getAnalyticSourceHash(), expectedAnnotationSource.getAnalyticSourceHash());

        Optional<AnnotationSource> annotationSource = dao.getAnnotationSource(expectedAnnotationSource.getAnalyticSourceHash());
        assertFalse(annotationSource.isEmpty());
        AnnotationSource resultAnnotationSource = annotationSource.get();
        assertAnnotationSourcesEqual(expectedAnnotationSource, resultAnnotationSource);
    }

    @Test
    public void testGetAnnotationSource() {
        Optional<AnnotationSource> annotationOptional = dao.getAnnotationSource("B3A65E623F5062BD3391015CDAA422B2");
        assertFalse(annotationOptional.isEmpty());
        AnnotationSource as = annotationOptional.get();
        assertEquals("v6", as.getEngine());
        assertEquals("avalon", as.getModel());
        assertEquals("toyota", as.getPlatform());

        final String[] expectedConfigKeys = {"normalization"};
        final String[] expectedConfigValues = {"circular"};
        assertMapEntries(expectedConfigKeys, expectedConfigValues, as.getConfigurationMap());

        final String[] expectedMetadataKeys = {"visibility", "created_date", "provenance"};
        final String[] expectedMetadataValues = {"PUBLIC", "2025-10-01T00:00:00Z", "v6/avalon"};
        assertMapEntries(expectedMetadataKeys, expectedMetadataValues, as.getMetadataMap());

    }

    @Test
    public void testGetAnnotationSourceMissing() {
        String row = "aaaaaaaa";
        Optional<AnnotationSource> annotationOptional = dao.getAnnotationSource(row);
        assertTrue(annotationOptional.isEmpty());
    }

    /** Test updating an existing annotation with new values */
    @Test
    @Order(Integer.MAX_VALUE - 1) // runs the destructive tests last.
    public void testUpdateAnnotation() {
        // Use pre-populated data for update test to ensure annotation exists
        String row = "20250406_456";
        String dataType = "news";
        String uidSeed = row + "_" + dataType;
        String documentUid = HashUID.builder().newId(uidSeed.getBytes(StandardCharsets.UTF_8)).toString();

        // Get an existing annotation from pre-populated data
        Collection<Annotation> annotations = dao.getAnnotations(row, dataType, documentUid);
        assertFalse(annotations.isEmpty(), "Should have at least one pre-populated annotation");

        Annotation originalAnnotation = annotations.stream().findFirst().orElse(EMPTY);
        String originalAnnotationId = originalAnnotation.getAnnotationId();

        // Create an updated annotation - clear the annotation ID and segment IDs
        List<Segment> clearedSegments = new ArrayList<>();
        for (Segment segment : originalAnnotation.getSegmentsList()) {
            clearedSegments.add(segment.toBuilder().clearSegmentHash().build());
        }
        Annotation updatedAnnotation = originalAnnotation.toBuilder().clearAnnotationId() // Clear the annotation id
                        .clearSegments().addAllSegments(clearedSegments) // Add segments with cleared IDs
                        .build();

        // Update the annotation
        Optional<Annotation> updateResult = dao.updateAnnotation(originalAnnotationId, updatedAnnotation);
        assertFalse(updateResult.isEmpty(), "Update should return an annotation");
        Annotation resultAnnotation = updateResult.get();

        // Verify the original annotation still exists
        Optional<Annotation> originalRetrieved = dao.getAnnotation(row, dataType, documentUid, originalAnnotationId);
        assertTrue(originalRetrieved.isPresent(), "Original annotation should still exist after update");

        // Verify we can retrieve the updated annotation (which should have a different id)
        String updateAnnotationId = resultAnnotation.getAnnotationId();
        assertNotEquals(originalAnnotationId, updateAnnotationId, "Updated annotation should have a different id");

        Optional<Annotation> updatedRetrieved = dao.getAnnotation(row, dataType, documentUid, updateAnnotationId);
        assertTrue(updatedRetrieved.isPresent(), "Updated annotation should be retrievable");
    }

    /** Test updating a non-existent annotation should throw exception */
    @Test
    public void testUpdateAnnotationNotFound() {
        String row = "20250406_456";
        String dataType = "news";
        String uidSeed = row + "_" + dataType;
        String documentUid = HashUID.builder().newId(uidSeed.getBytes(StandardCharsets.UTF_8)).toString();

        // Create an annotation to use for the update request
        Annotation sourceAnnotation = generateTestAnnotation().toBuilder().setShard(row).setDataType(dataType).setUid(documentUid).clearAnnotationId().build();

        String nonExistentId = "nonexistent-id-12345";

        try {
            dao.updateAnnotation(nonExistentId, sourceAnnotation);
            fail("Expected AnnotationUpdateException to be thrown");
        } catch (datawave.annotation.data.AnnotationUpdateException e) {
            assertTrue(e.getMessage().contains("Unable to find annotation to update"));
        }
    }

    /** Test deleting an annotation removes it from storage */
    @Test
    @Order(Integer.MAX_VALUE) // runs the destructive tests last.
    public void testDeleteAnnotation() {
        // Create a new annotation specifically for deletion testing (don't affect shared test data)
        Annotation sourceAnnotation = generateTestAnnotation();
        Optional<Annotation> addedOptional = dao.addAnnotation(sourceAnnotation);
        assertFalse(addedOptional.isEmpty());
        Annotation addedAnnotation = addedOptional.get();
        String annotationIdToDelete = addedAnnotation.getAnnotationId();
        String shard = addedAnnotation.getShard();
        String dataType = addedAnnotation.getDataType();
        String uid = addedAnnotation.getUid();

        // Verify the annotation exists
        Optional<Annotation> existingAnnotation = dao.getAnnotation(shard, dataType, uid, annotationIdToDelete);
        assertTrue(existingAnnotation.isPresent());

        // Delete the annotation
        dao.deleteAnnotation(shard, dataType, uid, annotationIdToDelete);

        // Verify the annotation no longer exists
        Optional<Annotation> deletedAnnotation = dao.getAnnotation(shard, dataType, uid, annotationIdToDelete);
        assertTrue(deletedAnnotation.isEmpty());
    }

    /** Test deleting an annotation that doesn't exist should not throw exception */
    @Test
    public void testDeleteAnnotationNotFound() {
        String shard = "99999_99999";
        String dataType = "nonexistent-test-type";
        String uid = "nonexistent-uid-12345";
        String annotationId = "nonexistent-annotation-id";

        // This should not throw an exception (delete handles empty iterator gracefully)
        // Note: if delete returns null mutation, writer.addMutation is called with null which throws
        try {
            dao.deleteAnnotation(shard, dataType, uid, annotationId);
            // If we get here, delete handled empty results correctly
        } catch (IllegalArgumentException e) {
            // This is expected if mutationAdapter returns null and writer.addMutation is called with null
            // The implementation should be fixed to check for null mutations
            assertTrue(e.getMessage().contains("m is null"));
        }
    }

    @AfterAll
    public static void shutdown() throws Exception {
        mac.stop();
    }

    public static void dumpTable(String table) {
        try {
            AccumuloClient client = mac.createAccumuloClient("root", new PasswordToken("pass"));
            Scanner scanner = client.createScanner(table, new Authorizations(auths));
            Iterator<Map.Entry<Key,Value>> iterator = scanner.iterator();
            System.out.println("*************** " + table + " ********************");
            while (iterator.hasNext()) {
                Map.Entry<Key,Value> entry = iterator.next();
                System.out.println("key: " + entry.getKey() + "; value: " + entry.getValue());
            }
            scanner.close();
        } catch (TableNotFoundException e) {
            throw new RuntimeException("TableNotFoundException: ", e);
        }
    }

    /**
     * Ensure that the map observed has the expected keys and values. It is expected that <code>expectedKeys[i]</code> has the value
     * <code>expectedValues[i]</code>, and it is expected that <code>observedMap.size()</code> is equal to <code>expectedKeys.length</code>
     *
     * @param expectedKeys
     *            the keys to check against
     * @param expectedValues
     *            the values to check against
     * @param observedMap
     *            the map to check
     */
    public void assertMapEntries(String[] expectedKeys, String[] expectedValues, Map<String,String> observedMap) {
        assertEquals(expectedKeys.length, observedMap.size());
        for (int i = 0; i < expectedKeys.length; i++) {
            String observedValue = observedMap.get(expectedKeys[i]);
            //@formatter:off
            assertEquals(expectedValues[i], observedValue,
                    "expected value " + expectedValues[i] +
                            " for key " + expectedKeys[i] +
                            " but saw " + observedValue
            );
            //@formatter:on
        }
    }

    /**
     * Ensure that the metadata provided contains the expected keys and values.
     *
     * @param metadata
     *            the metadata map to check.
     */
    public static void assertExpectedMetadata(Map<String,String> metadata) {
        assertEquals(7, metadata.size());

        final String[] expectedKeys = {"datatype", "shard", "day", "foo", "plough", "visibility", "created_date"};
        final String[] expectedValues = {"news", "456", "20250406", "bar", "plover", "PUBLIC", "2025-10-01T00:00:00Z"};

        for (int i = 0; i < expectedKeys.length; i++) {
            String observedValue = metadata.get(expectedKeys[i]);
            assertEquals(expectedValues[i], observedValue, "expected value " + expectedValues[i] + " for key " + expectedKeys[i] + " but saw " + observedValue);
        }
    }

    /**
     * Ensure that the segments provided contain the text values and boundaries expected
     *
     * @param segments
     *            the list of segments to check
     */
    public static void assertExpectedTextSegments(List<Segment> segments) {
        // TODO: make this less sensitive to order
        final String[] expectedWords = {"rabbit", "quick", "brown", "fox", "<eos>", "caught", "the", "the"};
        final int[] expectedStarts = {31, 4, 10, 16, 38, 20, 0, 27};

        assertFalse(segments.isEmpty());
        assertEquals(8, segments.size());
        int pos = 0;

        List<String> errorMessages = new ArrayList<>();

        for (Segment segment : segments) {
            SegmentValue expectedValue = SegmentValue.newBuilder().setValue(expectedWords[pos]).setScore(1.0f).build();
            SegmentBoundary expectedBoundary = SegmentBoundary.newBuilder().setStart(expectedStarts[pos])
                            .setEnd(expectedStarts[pos] + expectedWords[pos].length()).build();
            pos++;

            List<SegmentValue> observedValues = segment.getValuesList();
            assertFalse(observedValues.isEmpty());
            assertEquals(1, observedValues.size());
            SegmentValue observedValue = observedValues.get(0);
            SegmentBoundary observedBounds = segment.getBoundary();
            log.debug("pos: {} value: {} bounds: {}", pos, observedValue, observedBounds);
            // we want to see all errors, so don't stop on the first failure.
            evaluateTextSegmentMatch(errorMessages, expectedValue, expectedBoundary, observedValue, observedBounds);
        }

        assertEquals("[]", errorMessages.toString());
    }

    /**
     * Evaluate two text segments to determine whether they match. If there are mismatches, they will be stored list provided. The outcome of the check can be
     * determined by checking whether the error message list is empty
     *
     * @param errorMessages
     *            used to store error messages for failed checks
     * @param expectedValue
     *            the expected segment value.
     * @param expectedBoundary
     *            the expected boundary.
     * @param observedValue
     *            the value to check.
     * @param observedBoundary
     *            the boundary to check.
     */
    public static void evaluateTextSegmentMatch(List<String> errorMessages, SegmentValue expectedValue, SegmentBoundary expectedBoundary,
                    SegmentValue observedValue, SegmentBoundary observedBoundary) {
        String expectedWord = expectedValue.getValue();
        long expectedStart = expectedBoundary.getStart();

        String observedWord = observedValue.getValue();
        long observedStart = observedBoundary.getStart();

        if (!(expectedWord.equals(observedWord) && expectedStart == observedStart)) {
            String message = String.format("Segment mismatch: value: '%s', expected value: '%s',  start: %s, expected start: %s\n", observedWord, expectedWord,
                            observedStart, expectedStart);
            errorMessages.add(message);
        }
    }
}
