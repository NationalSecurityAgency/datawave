package datawave.annotation.test.v1;

import static datawave.annotation.util.v1.AnnotationUtils.injectBoundaryType;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.Point;
import datawave.annotation.protobuf.v1.PointList;
import datawave.annotation.protobuf.v1.Segment;
import datawave.annotation.protobuf.v1.SegmentValue;
import datawave.annotation.protobuf.v1.TextSpanChars;
import datawave.annotation.protobuf.v1.TimeSpanSeconds;
import datawave.data.hash.HashUID;

/**
 * Various utility methods to generating test data. Generally the items created by utilities will not have identifiers injected so that they can be used to test
 * the data access objects.
 */
public class AnnotationTestDataUtil {
    public static Annotation generateTestAnnotation() {
        //@formatter:off
        return Annotation.newBuilder()
                .setShard("20250704_249")
                .setDataType("testDataType")
                .setUid("abcde.fghij.klmno")
                .setAnnotationType("testAnnotationType")
                .addAllSegments(List.of(generateMultiTestSegment()))
                .putAllMetadata(generateTestMetadata()).build();
        //@formatter:on
    }

    public static Map<String,String> generateTestMetadata() {
        Map<String,String> metadata = new HashMap<>();
        metadata.put("foo", "bar");
        metadata.put("plough", "plover");
        metadata.put("visibility", "PUBLIC");
        metadata.put("created_date", "2025-10-01T00:00:00.000Z");
        return metadata;
    }

    public static Segment generateTestSegment() {
        TimeSpanSeconds time = TimeSpanSeconds.newBuilder().setStartSeconds(0.154).setEndSeconds(0.52).build();
        SegmentValue segmentValue = SegmentValue.newBuilder().setValue("horse").setScore(.21f).build();
        Segment segment = Segment.newBuilder().addSegmentValue(segmentValue).setTime(time).build();
        return injectBoundaryType(segment);
    }

    public static Segment generateMultiTestSegment() {
        TimeSpanSeconds time = TimeSpanSeconds.newBuilder().setStartSeconds(0.154).setEndSeconds(0.52).build();
        SegmentValue segmentValueOne = SegmentValue.newBuilder().setValue("cow").setScore(.235f).build();
        SegmentValue segmentValueTwo = SegmentValue.newBuilder().setValue("horse").setScore(.21f).setExtension("animal").build();
        Segment segment = Segment.newBuilder().addSegmentValue(segmentValueOne).addSegmentValue(segmentValueTwo).setTime(time).build();
        return injectBoundaryType(segment);
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
                    testAnnotations.add(annotation);
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
            TimeSpanSeconds timeSpan = TimeSpanSeconds.newBuilder().setStartSeconds(i).setEndSeconds(i + 5).build();
            SegmentValue valueOne = SegmentValue.newBuilder().setValue(words[wordPos]).setScore(.235f).build();
            SegmentValue valueTwo = SegmentValue.newBuilder().setValue(altWords[wordPos]).setScore(.21f).build();
            Segment segment = Segment.newBuilder().setTime(timeSpan).addSegmentValue(valueOne).addSegmentValue(valueTwo).build();
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
            TextSpanChars charSpan = TextSpanChars.newBuilder().setStartCharacter(start).setEndCharacter(end).build();
            SegmentValue valueOne = SegmentValue.newBuilder().setValue(word).setScore(1.0f).build();
            Segment segment = Segment.newBuilder().setCharacters(charSpan).addSegmentValue(valueOne).build();
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
        final int[][] upperLeft = {{0, 0}, {10, 15}, {20, 20}, {30, 50}, {60, 70}, {80, 90}};
        final int[][] lowerRight = {{5, 12}, {15, 18}, {28, 47}, {36, 55}, {70, 78}, {89, 95}};

        for (int i = 0; i < objects.length; i++) {
            Point topLeft = Point.newBuilder().setLabel("topLeft").setX(upperLeft[i][0]).setY(upperLeft[i][1]).build();
            Point bottomRight = Point.newBuilder().setLabel("bottomRight").setX(lowerRight[i][0]).setY(lowerRight[i][1]).build();
            PointList rectangle = PointList.newBuilder().addPoint(topLeft).addPoint(bottomRight).build();
            Segment.Builder segmentBuilder = Segment.newBuilder().setPointList(rectangle);

            segmentBuilder.addSegmentValue(SegmentValue.newBuilder().setValue(objects[i]).setScore(.97f).setExtension(model[i]).build());
            if (!altObjects[i].isEmpty()) {
                segmentBuilder.addSegmentValue(SegmentValue.newBuilder().setValue(altObjects[i]).setScore(.86f).setExtension(model[i]).build());
            }
            segments.add(segmentBuilder.build());
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
        metadata.put("visibility", "PUBLIC");
        metadata.put("created_date", "2025-10-01T00:00:00.000Z");
        return metadata;
    }
}
