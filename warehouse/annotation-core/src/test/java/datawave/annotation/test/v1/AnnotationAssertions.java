package datawave.annotation.test.v1;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.Segment;
import datawave.annotation.protobuf.v1.SegmentValue;

/** Utility methods for asserting annotation identity in unit tests */
public class AnnotationAssertions {
    public static void assertMetadataEqual(Map<String,String> expectedMetadata, List<Map.Entry<String,String>> observedMetadata) {
        Map<String,String> testMetadata = new HashMap<>(expectedMetadata);
        for (Map.Entry<String,String> observedMetadataItem : observedMetadata) {
            String key = observedMetadataItem.getKey();
            String expectedValue = testMetadata.remove(key);
            String observedValue = observedMetadataItem.getValue();
            assertNotNull(expectedValue, "unexpected metadata key " + key);
            assertEquals(expectedValue, observedValue, "unexpected metadata value for key " + key);
        }

        if (!testMetadata.isEmpty()) {
            fail("did not see expected metadata entries " + testMetadata);
        }
    }

    public static void assertAnnotationsEqual(Annotation t, Annotation a) {
        assertEquals(t.getShard(), a.getShard());
        assertEquals(t.getDataType(), a.getDataType());
        assertEquals(t.getUid(), a.getUid());
        assertEquals(t.getAnnotationType(), a.getAnnotationType());
        assertEquals(t.getAnnotationId(), a.getAnnotationId());
        assertEquals(t.getMetadataMap(), a.getMetadataMap());
        assertSegmentsEqual(t.getSegmentsList(), a.getSegmentsList());
    }

    /** Assert that two lists of segment are equal. We start by ensuring that both of the segment id's */
    public static void assertSegmentsEqual(Collection<Segment> expected, Collection<Segment> result) {
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
        assertTrue(mismatchMessages.isEmpty(), "Mismatch in uuids observed: " + mismatchMessages);

        List<String> mismatchedSegmentMessages = new ArrayList<>();
        for (Map.Entry<String,Segment> expectedSegmentEntry : expectedByUID.entrySet()) {
            String expectedKey = expectedSegmentEntry.getKey();
            Segment resultSegment = resultByUID.get(expectedKey);
            compareSegments(expectedSegmentEntry.getValue(), resultSegment, mismatchedSegmentMessages);
        }

        assertTrue(mismatchedSegmentMessages.isEmpty(), "Segment mismatches observed: " + mismatchedSegmentMessages);
    }

    /**
     * Generate an index of segments by segment id so that we can compare them a collection of segments to ensure they are the same.
     *
     * @param input
     *            the list of segments we'll be comparing/
     * @return a map of segment id to segment.
     */
    public static Map<String,Segment> indexSegments(Collection<Segment> input) {
        final Map<String,Segment> index = new HashMap<>();
        for (Segment s : input) {
            index.put(s.getSegmentId(), s);
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
            errorMessages.add("Mismatched segment id's: expected " + expected.getSegmentId() + " result: " + result.getSegmentId());
        }

        if (!expected.getAnnotationId().equals(result.getAnnotationId())) {
            errorMessages.add("Mismatched annotation id's: expected " + expected.getAnnotationId() + " result: " + result.getAnnotationId());
        }

        List<SegmentValue> expectedData = expected.getSegmentValueList();
        List<SegmentValue> resultData = result.getSegmentValueList();

        if (expectedData.size() != resultData.size()) {
            errorMessages.add("Mismatched Value counts: expected " + expectedData.size() + " result: " + resultData.size());
        }

        compareSegmentBoundaries(expected, result, errorMessages);
        compareSegmentValues(expectedData, resultData, errorMessages);

    }

    /**
     * Compare two segment boundaries. If there is a mismatch, the details will be written to errorMessages. If, after this method is run, errorMessages is
     * empty, the boundaries are identical.
     *
     * @param expected
     *            the expected segment to compare.
     * @param result
     *            the result to compare against.
     * @param errorMessages
     *            results stored here.
     */
    public static void compareSegmentBoundaries(Segment expected, Segment result, List<String> errorMessages) {

        if (!expected.getBoundaryType().equals(result.getBoundaryType())) {
            errorMessages.add("Mismatched boundary types: expected " + expected.getBoundaryType() + " result: " + result.getBoundaryType());

        }

        if (!expected.getBoundaryCase().equals(result.getBoundaryCase())) {
            errorMessages.add("Mismatched boundary case: expected " + expected.getBoundaryCase() + " result: " + result.getBoundaryCase());
        } else {
            switch (expected.getBoundaryCase()) {
                case TIME:
                    if (!expected.getTime().equals(result.getTime())) {
                        errorMessages.add("Mismatched time boundary: expected " + expected.getTime() + " result: " + result.getTime());
                    }
                    break;
                case CHARACTERS:
                    if (!expected.getCharacters().equals(result.getCharacters())) {
                        errorMessages.add("Mismatched text boundary: expected " + expected.getCharacters() + " result: " + result.getCharacters());
                    }
                    break;
                case POINTLIST:
                    if (!expected.getPointList().equals(result.getPointList())) {
                        errorMessages.add("Mismatched rectangle boundary: expected " + expected.getPointList() + " result: " + result.getPointList());
                    }
                    break;
                case ALL:
                case BOUNDARY_NOT_SET:
                    // no error, safe to ignore.
                    break;
                default:
                    throw new IllegalArgumentException("Encountered unexpected boundary case: " + expected.getBoundaryCase());
            }
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
        List<String> expectedValueStrings = expectedValues.stream().map(AnnotationAssertions::valueToString).collect(Collectors.toList());
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
        if (!StringUtils.isEmpty(segmentValue.getExtension())) {
            b.append("-").append(segmentValue.getExtension());
        }
        b.append(":").append(segmentValue.getScore());
        return b.toString();
    }
}
