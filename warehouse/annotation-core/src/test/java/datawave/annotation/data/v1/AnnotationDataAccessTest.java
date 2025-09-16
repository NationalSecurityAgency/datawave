package datawave.annotation.data.v1;

import static datawave.annotation.util.v1.AnnotationTestUtil.assertAnnotationsEqual;
import static datawave.annotation.util.v1.AnnotationTestUtil.generateTestAnnotation;
import static datawave.annotation.util.v1.SegmentUtils.injectAnnotationId;
import static datawave.annotation.util.v1.SegmentUtils.injectBoundaryType;
import static datawave.annotation.util.v1.SegmentUtils.injectSegmentHash;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

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
import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.Point;
import datawave.annotation.protobuf.v1.PointList;
import datawave.annotation.protobuf.v1.Segment;
import datawave.annotation.protobuf.v1.SegmentValue;
import datawave.annotation.protobuf.v1.TextSpanChars;
import datawave.annotation.protobuf.v1.TimeSpanSeconds;
import datawave.annotation.util.v1.AnnotationTestUtil;
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
        assertExpectedMetadata(a.getMetadataMap());
        assertExpectedSegments(a.getSegmentsList());

    }

    @Test
    public void testGetTypes() {
        String day = "20250406";
        String shard = "456";
        String row = day + "_" + shard;
        String dataType = "news";
        String uidSeed = row + "_" + dataType;
        String documentUid = HashUID.builder().newId(uidSeed.getBytes(StandardCharsets.UTF_8)).toString();

        Set<String> expectedTypes = new TreeSet<>();
        expectedTypes.add("tokens");

        Collection<String> annotationTypes = dao.getTypes(row, dataType, documentUid);
        assertFalse(annotationTypes.isEmpty());
        assertEquals(1, annotationTypes.size());
        assertEquals(expectedTypes, annotationTypes);
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
        // the tokens are return in an order based on their segment id, _not_ the position, if the id's change
        // the order changes.
        final String[] expectedWords = {"<eos>", "fox", "caught", "rabbit", "the", "the", "quick", "brown"};
        final int[] expectedStarts = {38, 16, 20, 31, 0, 27, 4, 10};

        // final String[] expectedWords = {"the", "fox", "brown", "<eos>", "quick", "rabbit", "caught", "the"};
        // final int[] expectedStarts = {0, 16, 10, 38, 4, 31, 20, 27};

        assertFalse(segments.isEmpty());
        assertEquals(8, segments.size());
        int pos = 0;

        List<String> errorMessages = new ArrayList<>();

        for (Segment segment : segments) {

            SegmentValue expectedValue = SegmentValue.newBuilder().setValue(expectedWords[pos]).setScore(1.0f).setExtension("").build();
            TextSpanChars expectedSpan = TextSpanChars.newBuilder().setStartCharacter(expectedStarts[pos])
                            .setEndCharacter(expectedStarts[pos] + expectedWords[pos].length()).build();
            pos++;

            List<SegmentValue> observedValues = segment.getSegmentValueList();
            assertFalse(observedValues.isEmpty());
            assertEquals(1, observedValues.size());
            SegmentValue observedValue = observedValues.get(0);

            TextSpanChars observedSpan = segment.getCharacters();

            evaluateSegmentMatch(expectedValue, expectedSpan, observedValue, observedSpan, errorMessages);

           //@formatter:off
           /*
            assertAll(message,
                   () -> assertEquals(expectedWord, value.getValue(), "term mismatch"),
                   () -> assertEquals(1.0f, value.getScore(), 0.0f, "score mismatch"),
                   () -> assertEquals("", value.getExtension(), "extension mismatch"));
           */
           //@formatter:on
        }

        assertEquals("[]", errorMessages.toString());
    }

    public static void evaluateSegmentMatch(SegmentValue expectedValue, TextSpanChars expectedBoundary, SegmentValue observedValue,
                    TextSpanChars observedBoundary, List<String> messages) {
        String expectedWord = expectedValue.getValue();
        long expectedStart = expectedBoundary.getStartCharacter();

        String observedWord = observedValue.getValue();
        long observedStart = observedBoundary.getStartCharacter();

        if (!(expectedWord.equals(observedWord) && expectedStart == observedStart)) {
            String message = String.format("Segment mismatch: value: '%s', expected value: '%s',  start: %s, expected start: %s\n", observedWord, expectedWord,
                            observedStart, expectedStart);
            messages.add(message);
        }
    }

    @Test
    public void testGetShardMissing() {
        String row = "20250406_4567"; // non-existent shard
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
        assertExpectedMetadata(a.getMetadataMap());
        assertExpectedSegments(a.getSegmentsList());
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
        String annotationUid = "9d672f3e";

        Optional<Annotation> annotationOptional = dao.get(row, dataType, documentUid, annotationType, annotationUid);
        assertFalse(annotationOptional.isEmpty());
        Annotation a = annotationOptional.get();
        assertExpectedMetadata(a.getMetadataMap());
        assertExpectedSegments(a.getSegmentsList());
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
                    Annotation annotation = Annotation.newBuilder().setShard(row).setDataType(dataTypes[i]).setUid(documentUid)
                                    .addAllSegments(generateTestSegments(day, shard, dataType)).putAllMetadata(generateTestMetadata(day, shard, dataType))
                                    .setAnnotationType(annotationType).build();
                    testAnnotations.add(injectAnnotationId(annotation));
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
            TimeSpanSeconds timeSpan = TimeSpanSeconds.newBuilder().setStartSeconds(i).setEndSeconds(i + 5).build();
            SegmentValue valueOne = SegmentValue.newBuilder().setValue(words[wordPos]).setScore(.235f).build();
            SegmentValue valueTwo = SegmentValue.newBuilder().setValue(altWords[wordPos]).setScore(.21f).build();
            Segment segment = Segment.newBuilder().setTime(timeSpan).addSegmentValue(valueOne).addSegmentValue(valueTwo).build();
            segments.add(injectSegmentHash(segment));

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
            TextSpanChars charSpan = TextSpanChars.newBuilder().setStartCharacter(start).setEndCharacter(end).build();
            SegmentValue valueOne = SegmentValue.newBuilder().setValue(word).setScore(1.0f).build();
            Segment segment = Segment.newBuilder().setCharacters(charSpan).addSegmentValue(valueOne).build();
            segments.add(injectSegmentHash(segment));

            start = end + 1;
        }
        return segments;
    }

    public static List<Segment> generateImageSegments(String day, String shard) {
        List<Segment> segments = new ArrayList<>();

        final String[] objects = {"bird", "car", "stairs", "motorcycle", "flashlight", "dog"};
        final String[] altObjects = {"crow", "truck", "", "bicycle", "", "pig"};
        final String[] model = {"alpha", "beta", "delta", "beta", "beta", "alpha"};
        final int[][] upperLeft = {{0, 0}, {10, 15}, {20, 20}, {30, 50}, {60, 70}, {80, 90}};
        final int[][] lowerRight = {{5, 12}, {15, 18}, {28, 47}, {36, 55}, {70, 78}, {89, 95}};

        for (int i = 0; i < objects.length; i++) {
            // TODO: look into SegmentBoundary types and use PointBounds/Point for Image segments.
            Point topLeft = Point.newBuilder().setLabel("topLeft").setX(upperLeft[i][0]).setY(upperLeft[i][1]).build();
            Point bottomRight = Point.newBuilder().setLabel("bottomRight").setX(lowerRight[i][0]).setY(lowerRight[i][1]).build();
            PointList rectangle = PointList.newBuilder().addPoint(topLeft).addPoint(bottomRight).build();
            Segment.Builder segmentBuilder = Segment.newBuilder().setPointList(rectangle);

            segmentBuilder.addSegmentValue(SegmentValue.newBuilder().setValue(objects[i]).setScore(.97f).setExtension(model[i]).build());
            if (!altObjects[i].isEmpty()) {
                segmentBuilder.addSegmentValue(SegmentValue.newBuilder().setValue(altObjects[i]).setScore(.86f).setExtension(model[i]).build());
            }
            segments.add(injectSegmentHash(injectBoundaryType(segmentBuilder.build())));
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
