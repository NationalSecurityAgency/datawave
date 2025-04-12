package datawave.annotation.util.v0;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import datawave.annotation.model.v0.Annotation;
import datawave.annotation.model.v0.Segment;
import datawave.annotation.protobuf.v0.SegmentBoundary;
import datawave.annotation.protobuf.v0.SegmentBoundaryType;
import datawave.annotation.protobuf.v0.SegmentData;
import datawave.annotation.protobuf.v0.SegmentValue;

public class AnnotationTestUtil {
    public static Annotation generateTestAnnotation() {
        //@formatter:off
        return Annotation.newBuilder()
                .setShard("20250704_249")
                .setDataType("testDataType")
                .setUid("abcde.fghij.klmno")
                .setAnnotationType("testAnnotationType")
                .setSegments(List.of(generateMultiTestSegment()))
                .setMetadata(generateTestMetadata())
                .build();
        //@formatter:on
    }

    public static Map<String,String> generateTestMetadata() {
        Map<String,String> metadata = new HashMap<>();
        metadata.put("foo", "bar");
        metadata.put("plough", "plover");
        return metadata;
    }

    public static Segment generateTestSegment() {
        SegmentBoundary boundary = SegmentBoundary.newBuilder().setType(SegmentBoundaryType.TIME).setStart("0.154").setEnd("0.52").build();

        SegmentValue segmentValue = SegmentValue.newBuilder().setValue("horse").setScore(.21f).setExtension("animal").build();

        SegmentData segmentData = SegmentData.newBuilder().addValue(segmentValue).setBoundary(boundary).build();

        return Segment.newBuilder().setSegmentData(segmentData).build();
    }

    public static Segment generateMultiTestSegment() {
        SegmentBoundary boundary = SegmentBoundary.newBuilder().setType(SegmentBoundaryType.TIME).setStart("0.154").setEnd("0.52").build();

        SegmentValue segmentValueOne = SegmentValue.newBuilder().setValue("cow").setScore(.235f).build();

        SegmentValue segmentValueTwo = SegmentValue.newBuilder().setValue("horse").setScore(.21f).setExtension("animal").build();

        SegmentData segmentData = SegmentData.newBuilder().addValue(segmentValueOne).addValue(segmentValueTwo).setBoundary(boundary).build();

        return Segment.newBuilder().setSegmentData(segmentData).build();
    }

    public static void assertMetadataEqual(Map<String,String> expectedMetadata, List<Map.Entry<String,String>> observedMetadata) {
        Map<String,String> testMetadata = new HashMap<>(expectedMetadata);
        for (Map.Entry<String,String> observedMetadataItem : observedMetadata) {
            String key = observedMetadataItem.getKey();
            String expectedValue = testMetadata.remove(key);
            String observedValue = observedMetadataItem.getValue();
            assertNotNull("unexpected metadata key " + key, expectedValue);
            assertEquals("unexpected metadata value for key " + key, expectedValue, observedValue);
        }

        if (!testMetadata.isEmpty()) {
            fail("did not see expected metadata entries " + testMetadata);
        }
    }

    public static void assertAnnotationsEqual(Annotation t, Annotation a) {
        assertEquals(t.getShard(), a.getShard());
        assertEquals(t.getDataType(), a.getDataType());
        assertEquals(t.getUid().toString(), a.getUid().toString());
        assertEquals(t.getAnnotationType(), a.getAnnotationType());
        assertEquals(t.getAnnotationId().toString(), a.getAnnotationId().toString());
        assertEquals(t.getMetadata(), a.getMetadata());
        assertSegmentsEqual(t.getSegments(), a.getSegments());
    }

    /** Assert that two lists of segment are equal. We start by ensuring that both of the segment id's */
    public static void assertSegmentsEqual(List<Segment> expected, List<Segment> result) {
        Map<String,Segment> expectedByUID = indexSegments(expected);
        Set<String> expectedUIDs = expectedByUID.keySet();

        Map<String,Segment> resultByUID = indexSegments(result);
        Set<String> resultUIDs = resultByUID.keySet();

        List<String> missing = new ArrayList<>(expectedUIDs);
        missing.removeAll(resultUIDs);

        List<String> extra = new ArrayList<>(resultUIDs);
        extra.removeAll(expectedUIDs);

        List<String> mismatchMessages = new ArrayList<>();
        if (!missing.isEmpty()) {
            mismatchMessages.add("Results are missing expected uids: " + missing);
        }
        if (!extra.isEmpty()) {
            mismatchMessages.add("Results have extra uids: " + extra);
        }
        assertTrue("Mismatch in uuids observed: " + mismatchMessages, mismatchMessages.isEmpty());

        List<String> mismatchedSegmentMessages = new ArrayList<>();
        for (Map.Entry<String,Segment> expectedSegmentEntry : expectedByUID.entrySet()) {
            String expectedKey = expectedSegmentEntry.getKey();
            Segment resultSegment = resultByUID.get(expectedKey);
            compareSegments(expectedSegmentEntry.getValue(), resultSegment, mismatchedSegmentMessages);
        }

        assertTrue("Segment mismatches observed: " + mismatchedSegmentMessages, mismatchedSegmentMessages.isEmpty());
    }

    /**
     * Generate an index of segments by segment id so that we can compare them a collection of segments to ensure they are the same.
     *
     * @param input
     *            the list of segments we'll be comparing/
     * @return a map of segment id to segment.
     */
    public static Map<String,Segment> indexSegments(List<Segment> input) {
        final Map<String,Segment> index = new HashMap<>();
        for (Segment s : input) {
            index.put(s.getSegmentId().toString(), s);
        }
        return index;
    }

    /**
     * Compare two segments, including their embedded boundaries and values. If there is a mismatch, the details will be written to errorMessages. If, after
     * this method is run errorMessages is empty, the segments are identical.
     *
     * @param expected
     *            the expected segment
     * @param result
     *            the result segment to compae to
     * @param errorMessages
     *            results stored here.
     */
    public static void compareSegments(Segment expected, Segment result, List<String> errorMessages) {
        if (!expected.getSegmentId().equals(result.getSegmentId())) {
            errorMessages.add("Mismatched UIDs: expected " + expected.getSegmentId() + " result: " + result.getSegmentId());
        }

        SegmentData expectedData = expected.getSegmentData();
        SegmentData resultData = result.getSegmentData();

        if (expectedData.getValueCount() != resultData.getValueCount()) {
            errorMessages.add("Mismatched Value counts: expected " + expectedData.getValueCount() + " result: " + resultData.getValueCount());
        }

        SegmentBoundary expectedBoundary = expectedData.getBoundary();
        SegmentBoundary resultBoundary = resultData.getBoundary();

        compareSegmentBoundaries(expectedBoundary, resultBoundary, errorMessages);
        compareSegmentValues(expectedData.getValueList(), resultData.getValueList(), errorMessages);

    }

    /**
     * Compare two segment boundaries. If there is a mismatch, the details will be written to errorMessages. If, after this method is run, errorMessages is
     * empty, the boundaries are identical.
     *
     * @param expectedBoundary
     *            the expected boundary.
     * @param resultsBoundary
     *            the result boundary to compare against.
     * @param errorMessages
     *            results stored here.
     */
    public static void compareSegmentBoundaries(SegmentBoundary expectedBoundary, SegmentBoundary resultsBoundary, List<String> errorMessages) {
        if (!expectedBoundary.getStart().equals(resultsBoundary.getStart())) {
            errorMessages.add("Mismatched boundary start: expected + " + expectedBoundary.getStart() + " result: " + resultsBoundary.getStart());
        }

        if (!expectedBoundary.getEnd().equals(resultsBoundary.getEnd())) {
            errorMessages.add("Mismatched boundary end: expected + " + expectedBoundary.getEnd() + " result: " + resultsBoundary.getEnd());
        }

        if (!expectedBoundary.getType().equals(resultsBoundary.getType())) {
            errorMessages.add("Mismatched boundary type: expected + " + expectedBoundary.getType() + " result: " + resultsBoundary.getType());
        }

        if (expectedBoundary.getRotation() != resultsBoundary.getRotation()) {
            errorMessages.add("Mismatched boundary rotation: expected + " + expectedBoundary.getRotation() + " result: " + resultsBoundary.getRotation());
        }
    }

    /**
     * Compare two lists of segment values. If there is a mismatch, the details will be written to errorMessages. If, after this method is run, errorMessages is
     * empty, the lists are identical.
     *
     * @param expectedValues
     *            the list of expected values
     * @param resultValues
     *            the list of result values.
     * @param errorMessages
     *            results stored here.
     */
    public static void compareSegmentValues(List<SegmentValue> expectedValues, List<SegmentValue> resultValues, List<String> errorMessages) {
        List<String> expectedValueStrings = expectedValues.stream().map(AnnotationTestUtil::valueToString).collect(Collectors.toList());
        List<String> unexpectedValueStrings = new ArrayList<>();
        for (SegmentValue resultValue : resultValues) {
            String resultValueString = valueToString(resultValue);
            if (!expectedValueStrings.remove(resultValueString)) {
                unexpectedValueStrings.add(resultValueString);
            }
        }

        if (!expectedValueStrings.isEmpty()) {
            errorMessages.add("Did not observe expected segment values: " + expectedValueStrings);
        }

        if (!unexpectedValueStrings.isEmpty()) {
            errorMessages.add("Observed unexpected segment values: " + unexpectedValueStrings);
        }
    }

    /**
     * Cheat on comparison, convert the values to strings
     *
     * @param segmentValue
     *            the segment value to convert
     * @return the value converted to a string (human-readable)
     */
    public static String valueToString(SegmentValue segmentValue) {
        StringBuilder b = new StringBuilder().append(segmentValue.getValue());
        if (segmentValue.hasExtension()) {
            b.append("-").append(segmentValue.getExtension());
        }
        b.append(":").append(segmentValue.getScore());
        return b.toString();
    }
}
