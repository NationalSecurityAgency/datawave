package datawave.annotation.data.v0;

import static datawave.annotation.util.v0.AnnotationTestUtil.assertAnnotationsEqual;
import static datawave.annotation.util.v0.AnnotationTestUtil.generateTestAnnotation;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.annotation.data.AnnotationDataAccess;
import datawave.annotation.model.v0.Annotation;
import datawave.annotation.model.v0.Segment;
import datawave.annotation.protobuf.v0.SegmentBoundary;
import datawave.annotation.protobuf.v0.SegmentBoundaryType;
import datawave.annotation.protobuf.v0.SegmentData;
import datawave.annotation.protobuf.v0.SegmentValue;
import datawave.annotation.util.v0.AnnotationTestUtil;
import datawave.data.hash.HashUID;

public class AnnotationDataAccessTest {

    protected static final Logger log = LoggerFactory.getLogger(AnnotationDataAccessTest.class);

    private static MiniAccumuloCluster mac;
    private static final Authorizations auths = new Authorizations("PUBLIC");

    @BeforeAll
    public static void startCluster() throws Exception {
        File macDir = new File(System.getProperty("user.dir") + "/target/mac/" + AnnotationDataAccessTest.class.getName());
        if (macDir.exists())
            FileUtils.deleteDirectory(macDir);
        // noinspection ResultOfMethodCallIgnored
        macDir.mkdirs();
        mac = new MiniAccumuloCluster(new MiniAccumuloConfig(macDir, "pass"));
        mac.start();

        AccumuloClient client = mac.createAccumuloClient("root", new PasswordToken("pass"));

        NamespaceOperations namespaceOperations = client.namespaceOperations();
        namespaceOperations.create("datawave");

        TableOperations tableOperations = client.tableOperations();
        tableOperations.create("datawave.annotations");

        SecurityOperations securityOperations = client.securityOperations();
        securityOperations.changeUserAuthorizations("root", auths);

        List<Annotation> manyAnnotations = generateManyTestAnnotations();

        AccumuloAnnotationSerializer serializer = new AccumuloAnnotationSerializer();
        AnnotationDataAccess<Annotation,Segment> setupDao = new AnnotationDataAccess<>(mac.createAccumuloClient("root", new PasswordToken("pass")), auths,
                        "datawave.annotations", serializer);
        for (Annotation annotation : manyAnnotations) {
            setupDao.save(annotation);
        }
        dumpTable("datawave.annotations");
    }

    AnnotationDataAccess<Annotation,Segment> dao;

    @BeforeEach
    public void setup() throws AccumuloException, AccumuloSecurityException, TableExistsException {
        AccumuloAnnotationSerializer serializer = new AccumuloAnnotationSerializer();
        dao = new AnnotationDataAccess<>(mac.createAccumuloClient("root", new PasswordToken("pass")), auths, "datawave.annotations", serializer);
    }

    /** Insert a new annotation into the table and retrieve it and validate */
    @Test
    public void testAnnotationSaveLoad() {
        Annotation sourceAnnotation = generateTestAnnotation();
        dao.save(sourceAnnotation);

        List<Annotation> annotation = dao.getAll(sourceAnnotation.getShard(), sourceAnnotation.getDataType(), sourceAnnotation.getUid());
        assertFalse(annotation.isEmpty());
        assertEquals(1, annotation.size());
        Annotation resultAnnotation = annotation.get(0);
        assertAnnotationsEqual(sourceAnnotation, resultAnnotation);
    }

    @Test
    public void testGetAll() {
        String day = "20250406";
        String shard = "456";
        String row = day + "_" + shard;
        String dataType = "news";
        String uidSeed = row + "_" + dataType;
        String documentUid = HashUID.builder().newId(uidSeed.getBytes(StandardCharsets.UTF_8)).toString();

        List<Annotation> annotations = dao.getAll(row, dataType, documentUid);
        assertFalse(annotations.isEmpty());
        assertEquals(1, annotations.size());
        Annotation a = annotations.get(0);
        assertExpectedMetadata(a.getMetadata());
        assertExpectedSegments(a.getSegments());

    }

    public static void assertExpectedMetadata(Map<String,String> metadata) {
        assertEquals(5, metadata.size());

        final String[] expectedKeys = {"datatype", "shard", "day", "foo", "plough"};
        final String[] expectedValues = {"news", "456", "20250406", "bar", "plover"};

        for (int i = 0; i < expectedKeys.length; i++) {
            String observedValue = metadata.get(expectedKeys[i]);
            assertEquals(expectedValues[i], observedValue, "expected value " + expectedValues[i] + " for key " + expectedKeys[i] + " but saw " + observedValue);
        }
    }

    public static void assertExpectedSegments(List<Segment> segments) {
        // the tokens are return in an order based on their segment id, _not_ the position.
        final String[] expectedWords = {"the", "fox", "brown", "<eos>", "quick", "rabbit", "caught", "the"};
        final int[] expectedStarts = {0, 16, 10, 38, 4, 31, 20, 27};

        assertFalse(segments.isEmpty());
        assertEquals(8, segments.size());
        int pos = 0;

        for (Segment segment : segments) {
            String expectedWord = expectedWords[pos];
            int expectedStart = expectedStarts[pos];
            int expectedEnd = expectedStart + expectedWord.length();
            pos++;

            SegmentData data = segment.getSegmentData();

            List<SegmentValue> values = data.getValueList();
            assertFalse(values.isEmpty());
            assertEquals(1, values.size());
            SegmentValue value = values.get(0);
            SegmentBoundary boundary = data.getBoundary();

            String message = String.format("Segment mismatch: value: '%s', expected value: '%s', start: %s, expected start: %s, end: %s, expected end: %s",
                            value.getValue(), expectedWord, boundary.getStart(), expectedStart, boundary.getEnd(), expectedEnd);

           //@formatter:off
           assertAll(message,
                   () -> assertEquals(expectedWord, value.getValue(), "term mismatch"),
                   () -> assertEquals(1.0f, value.getScore(), 0.0f, "score mismatch"),
                   () -> assertEquals("", value.getExtension(), "extension mismatch"),
                   () -> assertEquals(expectedStart, Integer.parseInt(boundary.getStart()), "start mismatch"),
                   () -> assertEquals(expectedEnd, Integer.parseInt(boundary.getEnd()), "end mismatch"));
           //@formatter:on
        }

    }

    @Test
    public void testGetShardMissing() {
        String row = "20250406_4567"; // not existent dhard
        String dataType = "news";
        String uidSeed = row + "_" + dataType;
        String documentUid = HashUID.builder().newId(uidSeed.getBytes(StandardCharsets.UTF_8)).toString();

        List<Annotation> annotations = dao.getAll(row, dataType, documentUid);
        assertTrue(annotations.isEmpty());
    }

    @Test
    public void testGetDatatypeMissing() {
        String row = "20250406_456";
        String dataType = "email"; // non-existent datatype
        String uidSeed = row + "_" + dataType;
        String documentUid = HashUID.builder().newId(uidSeed.getBytes(StandardCharsets.UTF_8)).toString();

        List<Annotation> annotations = dao.getAll(row, dataType, documentUid);
        assertTrue(annotations.isEmpty());
    }

    @Test
    public void testGetUIDMissing() {
        String row = "20250406_456";
        String dataType = "email";
        String uidSeed = "helios"; // non-existent uid from this seed.
        String documentUid = HashUID.builder().newId(uidSeed.getBytes(StandardCharsets.UTF_8)).toString();

        List<Annotation> annotations = dao.getAll(row, dataType, documentUid);
        assertTrue(annotations.isEmpty());
    }

    @Test
    public void testGetAllForType() {
        String row = "20250406_456";
        String dataType = "news";
        String uidSeed = row + "_" + dataType;
        String annotationType = "tokens";

        String documentUid = HashUID.builder().newId(uidSeed.getBytes(StandardCharsets.UTF_8)).toString();
        List<Annotation> annotations = dao.getAllForType(row, dataType, documentUid, annotationType);
        assertFalse(annotations.isEmpty());
        assertEquals(1, annotations.size());
        Annotation a = annotations.get(0);
        assertExpectedMetadata(a.getMetadata());
        assertExpectedSegments(a.getSegments());
    }

    @Test
    public void testGetAllForTypeMissing() {
        String row = "20250406_456";
        String dataType = "email"; // non-existent datatype
        String uidSeed = row + "_" + dataType;
        String annotationType = "tokens";

        String documentUid = HashUID.builder().newId(uidSeed.getBytes(StandardCharsets.UTF_8)).toString();
        List<Annotation> annotations = dao.getAllForType(row, dataType, documentUid, annotationType);
        assertTrue(annotations.isEmpty());
    }

    @Test
    public void testGet() {
        String row = "20250406_456";
        String dataType = "news";
        String uidSeed = row + "_" + dataType;
        String annotationType = "tokens";

        String documentUid = HashUID.builder().newId(uidSeed.getBytes(StandardCharsets.UTF_8)).toString();
        String annotationUid = "kir5i4.tf9ozi.-ji6i29";

        Optional<Annotation> annotationOptional = dao.get(row, dataType, documentUid, annotationType, annotationUid);
        assertFalse(annotationOptional.isEmpty());
        Annotation a = annotationOptional.get();
        assertExpectedMetadata(a.getMetadata());
        assertExpectedSegments(a.getSegments());
    }

    @Test
    public void testGetMissing() {
        String row = "20250406_456";
        String dataType = "email"; // non-existant datatype
        String uidSeed = row + "_" + dataType;
        String annotationType = "tokens";

        String documentUid = HashUID.builder().newId(uidSeed.getBytes(StandardCharsets.UTF_8)).toString();
        String annotationUid = "kir5i4.tf9ozi.-ji6i29";

        Optional<Annotation> annotationOptional = dao.get(row, dataType, documentUid, annotationType, annotationUid);
        assertTrue(annotationOptional.isEmpty());
    }

    @AfterAll
    public static void shutdown() throws Exception {
        mac.stop();
    }

    public static void dumpTable(String table) {
        try {
            AccumuloClient client = mac.createAccumuloClient("root", new PasswordToken("pass"));
            Scanner scanner = client.createScanner(table, auths);
            Iterator<Map.Entry<Key,Value>> iterator = scanner.iterator();
            System.out.println("*************** " + table + " ********************");
            while (iterator.hasNext()) {
                Map.Entry<Key,Value> entry = iterator.next();
                log.info("key: {}; value length: {}", entry.getKey(), entry.getValue().getSize());
            }
            scanner.close();
        } catch (TableNotFoundException e) {
            throw new RuntimeException("TableNotFoundException: ", e);
        }
    }

    public static List<Annotation> generateManyTestAnnotations() {
        List<Annotation> testAnnotations = new ArrayList<>();

        final String[] dataTypes = {"audio", "news", "cars"};
        final String[] annotationTypes = {"tts", "tokens", "object"};
        final String[] days = {"20250405", "20250406", "20250407"};
        final String[] shards = {"123", "456", "789"};

        for (String day : days) {
            for (String shard : shards) {
                String row = day + "_" + shard;
                for (int i = 0; i < dataTypes.length; i++) {
                    String dataType = dataTypes[i];
                    String annotationType = annotationTypes[i];
                    String seed = row + "_" + dataType;
                    String documentUid = HashUID.builder().newId(seed.getBytes(StandardCharsets.UTF_8)).toString();

                    // @formatter: off
                    Annotation a = Annotation.newBuilder().setShard(row).setDataType(dataTypes[i]).setUid(documentUid)
                                    .setSegments(generateTestSegments(day, shard, dataType)).setMetadata(generateTestMetadata(day, shard, dataType))
                                    .setAnnotationType(annotationType).build();
                    testAnnotations.add(a);
                    // @formatter on;
                }
            }
        }

        return testAnnotations;
    }

    public static List<Segment> generateTestSegments(String day, String shard, String datatype) {
        switch (datatype) {
            case "audio": // an imaginary audio (temporal) dataset
                return generateAudioSegments(day, shard);
            case "news": // an imaginary text dataset
                return generateTextSegments(day, shard);
            case "cars": // an imaginary image dataset
                return generateImageSegments(day, shard);
            default:
                return List.of(AnnotationTestUtil.generateMultiTestSegment());
        }
    }

    public static List<Segment> generateAudioSegments(String day, String shard) {
        List<Segment> segments = new ArrayList<>();
        final String[] words = {"the", "cat", "sat", "on", "the", "mat", "<eos>"};
        final String[] altWords = {"the", "bat", "ate", "<unk>", "the", "gnat", "<eos>"};
        int wordPos = 0;

        // generate a boundary of 1 second of duration every 10 seconds
        for (int i = 0; i < 100; i += 10) {
            // TODO: look into SegmentBoundary types and use PointBounds/Point for Image segments.
            SegmentBoundary boundary = SegmentBoundary.newBuilder().setType(SegmentBoundaryType.TIME).setStart(String.valueOf(i)).setEnd(String.valueOf(i + 5))
                            .build();
            SegmentValue valueOne = SegmentValue.newBuilder().setValue(words[wordPos]).setScore(.235f).build();
            SegmentValue valueTwo = SegmentValue.newBuilder().setValue(altWords[wordPos]).setScore(.21f).build();

            SegmentData data = SegmentData.newBuilder().addValue(valueOne).addValue(valueTwo).setBoundary(boundary).build();
            Segment segment = Segment.newBuilder().setSegmentData(data).build();
            segments.add(segment);

            // cycle through words
            wordPos++;
            if (wordPos >= words.length) {
                wordPos = 0;
            }
        }
        return segments;
    }

    public static List<Segment> generateTextSegments(String day, String shard) {
        List<Segment> segments = new ArrayList<>();
        final String[] words = {"the", "quick", "brown", "fox", "caught", "the", "rabbit", "<eos>"};
        int start = 0;
        for (String word : words) {
            int end = start + word.length();

            // character offsets
            SegmentBoundary boundary = SegmentBoundary.newBuilder().setType(SegmentBoundaryType.POSITION).setStart(String.valueOf(start))
                            .setEnd(String.valueOf(end)).build();
            SegmentValue valueOne = SegmentValue.newBuilder().setValue(word).setScore(1.0f).build();
            SegmentData data = SegmentData.newBuilder().addValue(valueOne).setBoundary(boundary).build();
            Segment segment = Segment.newBuilder().setSegmentData(data).build();
            segments.add(segment);

            start = end + 1;
        }
        return segments;
    }

    public static List<Segment> generateImageSegments(String day, String shard) {
        List<Segment> segments = new ArrayList<>();

        final String[] objects = {"bird", "car", "stairs", "motorcycle", "flashlight", "dog"};
        final String[] altObjects = {"crow", "truck", "", "bicycle", "", "pig"};
        final String[] model = {"alpha", "beta", "delta", "beta", "beta", "alpha"};
        final String[] upperLeft = {"0,0", "10,15", "20,20", "30,50", "60,70", "80,90"};
        final String[] lowerRight = {"5,12", "15,18", "28,47", "36,55", "70,78", "89,95"};

        for (int i = 0; i < objects.length; i++) {
            // TODO: look into SegmentBoundary types and use PointBounds/Point for Image segments.
            // character offsets
            SegmentBoundary boundary = SegmentBoundary.newBuilder().setType(SegmentBoundaryType.POSITION).setStart(upperLeft[i]).setEnd(lowerRight[i]).build();
            SegmentData.Builder dataBuilder = SegmentData.newBuilder().setBoundary(boundary);

            dataBuilder.addValue(SegmentValue.newBuilder().setValue(objects[i]).setScore(.97f).setExtension(model[i]).build());
            if (!altObjects[i].isEmpty()) {
                dataBuilder.addValue(SegmentValue.newBuilder().setValue(altObjects[i]).setScore(.86f).setExtension(model[i]).build());
            }

            SegmentData data = dataBuilder.build();
            Segment segment = Segment.newBuilder().setSegmentData(data).build();
            segments.add(segment);
        }

        return segments;
    }

    public static Map<String,String> generateTestMetadata(String day, String shard, String datatype) {
        Map<String,String> metadata = new HashMap<>();
        metadata.put("datatype", datatype);
        metadata.put("shard", shard);
        metadata.put("day", day);
        metadata.put("foo", "bar");
        metadata.put("plough", "plover");
        return metadata;
    }
}
