package datawave.annotation.test.v1;

import static datawave.annotation.protobuf.v1.BoundaryType.ALL;
import static datawave.annotation.protobuf.v1.BoundaryType.POINTS;
import static datawave.annotation.protobuf.v1.BoundaryType.TEXT_CHAR;
import static datawave.annotation.protobuf.v1.BoundaryType.TIME_MILLI;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.AnnotationSource;
import datawave.annotation.protobuf.v1.Point;
import datawave.annotation.protobuf.v1.Segment;
import datawave.annotation.protobuf.v1.SegmentBoundary;
import datawave.annotation.protobuf.v1.SegmentValue;
import datawave.annotation.util.v1.AnnotationUtils;
import datawave.data.hash.HashUID;

/**
 * Various utility methods to generating test data. Generally the items created by utilities will not have identifiers injected so that they can be used to test
 * the data access objects.
 */
public class AnnotationTestDataUtil {
    public static final String CREATED_DATE = "2025-10-01T00:00:00Z";
    public static final String VISIBILITY = "PUBLIC";

    public static AnnotationSource generateTestAnnotationSource() {
        //@formatter:off
        return AnnotationSource.newBuilder()
                .setEngine("inline v6")
                .setModel("GR Supra")
                .putMetadata("visibility", VISIBILITY)
                .putMetadata("created_date",CREATED_DATE)
                .putConfiguration("octane","99")
                .putConfiguration("model_year", "2025")
                .build();
        //@formatter:on
    }

    public static Annotation generateTestAnnotation() {
        //@formatter:off
        AnnotationSource partialSource = generateTestAnnotationSource();
        AnnotationSource source = AnnotationUtils.injectAnnotationSourceHashes(partialSource);

        return Annotation.newBuilder()
                .setShard("20250704_249")
                .setDataType("testDataType")
                .setUid("abcde.fghij.klmno")
                .setAnnotationType("testAnnotationType")
                .setDocumentId("1234567890")
                .setSource(source)
                .setAnalyticSourceHash(source.getAnalyticSourceHash())
                .addAllSegments(List.of(generateMultiTestSegment()))
                .putAllMetadata(generateTestAnnotationMetadata()).build();
        //@formatter:on
    }

    public static Map<String,String> generateTestAnnotationMetadata() {
        Map<String,String> metadata = new HashMap<>();
        metadata.put("foo", "bar");
        metadata.put("plough", "plover");
        metadata.put("visibility", VISIBILITY);
        metadata.put("created_date", CREATED_DATE);
        return metadata;
    }

    public static Segment generateTestSegment() {
        SegmentValue segmentValue = SegmentValue.newBuilder().setValue("horse").setScore(.21f).build();
        SegmentBoundary bounds = SegmentBoundary.newBuilder().setBoundaryType(TIME_MILLI).setStart(1540).setEnd(5200).build();
        return Segment.newBuilder().addValues(segmentValue).setBoundary(bounds).build();
    }

    public static Segment generateMultiTestSegment() {
        SegmentValue segmentValueOne = SegmentValue.newBuilder().setValue("cow").setScore(.235f).build();
        Map<String,String> extension = new HashMap<>();
        extension.put("objectType", "animal");
        SegmentValue segmentValueTwo = SegmentValue.newBuilder().setValue("horse").setScore(.21f).putAllExtension(extension).build();
        SegmentBoundary bounds = SegmentBoundary.newBuilder().setBoundaryType(TIME_MILLI).setStart(1540).setEnd(5200).build();
        return Segment.newBuilder().addValues(segmentValueOne).addValues(segmentValueTwo).setBoundary(bounds).build();
    }

    public static List<Annotation> generateManyTestAnnotations() {
        List<Annotation> testAnnotations = new ArrayList<>();

        final String[] dataTypes = {"audio", "news", "cars", "media"};
        final String[] annotationTypes = {"tts", "tokens", "object", "image"};
        final String[] days = {"20250405", "20250406", "20250407"};
        final String[] shards = {"123", "456", "789"};

        AnnotationSource baseAnnotationSource = generateTestAnnotationSource();
        AnnotationSource annotationSource = AnnotationUtils.injectAnnotationSourceHashes(baseAnnotationSource);

        int documentId = 0;

        for (String day : days) {
            for (String shard : shards) {
                String row = day + "_" + shard;
                for (int i = 0; i < dataTypes.length; i++) {
                    String dataType = dataTypes[i];
                    String annotationType = annotationTypes[i];
                    String seed = row + "_" + dataType;
                    String documentUid = HashUID.builder().newId(seed.getBytes(StandardCharsets.UTF_8)).toString();

                    //@formatter:off
                    Annotation annotation = Annotation.newBuilder()
                            .setShard(row)
                            .setDataType(dataTypes[i])
                            .setUid(documentUid)
                            .setDocumentId(String.format("%012d", ++documentId))
                            .setAnalyticSourceHash(annotationSource.getAnalyticSourceHash())
                            .setSource(annotationSource)
                            .addAllSegments(generateTestSegments(day, shard, dataType))
                            .putAllMetadata(generateTestMetadata(day, shard, dataType))
                            .setAnnotationType(annotationType).build();
                    testAnnotations.add(annotation);
                    //@formatter:on
                }
            }
        }

        return testAnnotations;
    }

    public static List<AnnotationSource> generateManyTestAnnotationSources() {
        List<AnnotationSource> testAnnotationSources = new ArrayList<>();

        final String[] engines = {"v4", "v6", "v8"};
        final String[] models = {"camry", "corolla", "avalon"};
        final String[] sourceLabels = {"toyota", "honda", "mitsubishi"};
        final String[] configurations = {"circular", "reduction", "inherit", "standalone", "inline"};

        int iteration = 0;
        for (String engine : engines) {
            for (String model : models) {
                for (String sourceLabel : sourceLabels) {
                    int pos = iteration % configurations.length;
                    iteration++;
                    //@formatter:off
                    Map<String, String> metadata = Map.of(
                        "visibility", VISIBILITY,
                        "created_date", CREATED_DATE,
                        "provenance", engine + "/" + model
                    );

                    Map<String, String> configuration = Map.of(
                        "normalization", configurations[pos]
                    );
                    AnnotationSource annotationSource = AnnotationSource.newBuilder()
                            .setEngine(engine)
                            .setModel(model)
                            .putAllMetadata(metadata)
                            .putAllConfiguration(configuration)
                            .build();
                    testAnnotationSources.add(annotationSource);
                    //@formatter:on
                }
            }
        }
        return testAnnotationSources;
    }

    public static List<Segment> generateTestSegments(String day, String shard, String datatype) {
        switch (datatype) {
            case "audio": // an imaginary audio (temporal) dataset
                return generateAudioSegments(day, shard);
            case "news": // an imaginary text dataset
                return generateTextSegments(day, shard);
            case "cars": // an imaginary image dataset
                return generateImageBoxSegments(day, shard);
            case "media":
                return generateImageAllSegments(day, shard);
            default:
                return List.of(generateMultiTestSegment());
        }
    }

    public static List<Segment> generateAudioSegments(String day, String shard) {
        List<Segment> segments = new ArrayList<>();
        final String[] words = {"the", "cat", "sat", "on", "the", "mat", "<eos>"};
        final String[] altWords = {"the", "bat", "ate", "<unk>", "the", "gnat", "<eos>"};
        int wordPos = 0;

        // generate a boundary of 1 second of duration every 10 seconds
        for (int i = 0; i < 100; i += 10) {
            SegmentBoundary bounds = SegmentBoundary.newBuilder().setBoundaryType(TIME_MILLI).setStart(i * 1000).setEnd((i + 5) * 1000).build();

            SegmentValue valueOne = SegmentValue.newBuilder().setValue(words[wordPos]).setScore(.235f).build();
            SegmentValue valueTwo = SegmentValue.newBuilder().setValue(altWords[wordPos]).setScore(.21f).build();
            Segment segment = Segment.newBuilder().setBoundary(bounds).addValues(valueOne).addValues(valueTwo).build();
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
            SegmentBoundary bounds = SegmentBoundary.newBuilder().setBoundaryType(TEXT_CHAR).setStart(start).setEnd(end).build();

            SegmentValue valueOne = SegmentValue.newBuilder().setValue(word).setScore(1.0f).build();
            Segment segment = Segment.newBuilder().setBoundary(bounds).addValues(valueOne).build();
            segments.add(segment);

            start = end + 1;
        }
        return segments;
    }

    public static List<Segment> generateImageBoxSegments(String day, String shard) {
        List<Segment> segments = new ArrayList<>();

        final String[] objects = {"bird", "car", "stairs", "motorcycle", "flashlight", "dog"};
        final String[] altObjects = {"crow", "truck", "", "bicycle", "", "pig"};
        final String[] model = {"alpha", "beta", "delta", "beta", "beta", "alpha"};
        final int[][] upperLeft = {{0, 0}, {10, 15}, {20, 20}, {30, 50}, {60, 70}, {80, 90}};
        final int[][] lowerRight = {{5, 12}, {15, 18}, {28, 47}, {36, 55}, {70, 78}, {89, 95}};

        for (int i = 0; i < objects.length; i++) {
            Point topLeft = Point.newBuilder().setLabel("topLeft").setX(upperLeft[i][0]).setY(upperLeft[i][1]).build();
            Point bottomRight = Point.newBuilder().setLabel("bottomRight").setX(lowerRight[i][0]).setY(lowerRight[i][1]).build();
            SegmentBoundary bounds = SegmentBoundary.newBuilder().setBoundaryType(POINTS).addPoints(topLeft).addPoints(bottomRight).build();
            Segment.Builder segmentBuilder = Segment.newBuilder().setBoundary(bounds);

            Map<String,String> extension = new HashMap<>();
            extension.put("version", model[i]);
            segmentBuilder.addValues(SegmentValue.newBuilder().setValue(objects[i]).setScore(.97f).putAllExtension(extension).build());
            if (!altObjects[i].isEmpty()) {
                segmentBuilder.addValues(SegmentValue.newBuilder().setValue(altObjects[i]).setScore(.86f).putAllExtension(extension).build());
            }
            segments.add(segmentBuilder.build());
        }

        return segments;
    }

    public static List<Segment> generateImageAllSegments(String day, String shard) {
        List<Segment> segments = new ArrayList<>();

        final String[] objects = {"landscape", "portrait", "scene", "evening", "astral", "material"};
        final String[] altObjects = {"underground", "postcard", "", "afternoon", "ethereal", "fabric"};
        final String[] model = {"charlie", "bravo", "lima", "victor", "delta", "micro"};

        SegmentBoundary bounds = SegmentBoundary.newBuilder().setBoundaryType(ALL).build();
        Segment.Builder segmentBuilder = Segment.newBuilder().setBoundary(bounds);

        for (int i = 0; i < objects.length; i++) {
            Map<String,String> extension = new HashMap<>();
            extension.put("version", model[i]);
            segmentBuilder.addValues(SegmentValue.newBuilder().setValue(objects[i]).setScore(.97f).putAllExtension(extension).build());
            if (!altObjects[i].isEmpty()) {
                segmentBuilder.addValues(SegmentValue.newBuilder().setValue(altObjects[i]).setScore(.86f).putAllExtension(extension).build());
            }
        }

        segments.add(segmentBuilder.build());

        return segments;
    }

    public static Map<String,String> generateTestMetadata(String day, String shard, String datatype) {
        Map<String,String> metadata = new HashMap<>();
        metadata.put("datatype", datatype);
        metadata.put("shard", shard);
        metadata.put("day", day);
        metadata.put("foo", "bar");
        metadata.put("plough", "plover");
        metadata.put("visibility", VISIBILITY);
        metadata.put("created_date", CREATED_DATE);
        return metadata;
    }
}
