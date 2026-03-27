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

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.AnnotationSource;
import datawave.annotation.protobuf.v1.BoundaryType;
import datawave.annotation.protobuf.v1.Segment;
import datawave.annotation.protobuf.v1.SegmentBoundary;
import datawave.annotation.protobuf.v1.SegmentValue;

/** Utility methods for asserting annotation identity in unit tests */
public class AnnotationAssertions {
    public static void assertAnnotationSourcesEqual(AnnotationSource t, AnnotationSource a) {
        assertEquals(t.getAnalyticHash(), a.getAnalyticHash());
        assertEquals(t.getAnalyticSourceHash(), a.getAnalyticSourceHash());
        assertEquals(t.getEngine(), a.getEngine());
        assertEquals(t.getModel(), a.getModel());
        assertEquals(t.getConfigurationMap(), a.getConfigurationMap());
        assertEquals(t.getMetadataMap(), a.getMetadataMap());
    }

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
        assertEquals(t.getDocumentId(), a.getDocumentId());
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
            index.put(s.getSegmentHash(), s);
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
        if (!expected.getSegmentHash().equals(result.getSegmentHash())) {
            errorMessages.add("Mismatched segment hashes: expected " + expected.getSegmentHash() + " result: " + result.getSegmentHash());
        }

        List<SegmentValue> expectedData = expected.getValuesList();
        List<SegmentValue> resultData = result.getValuesList();

        if (expectedData.size() != resultData.size()) {
            errorMessages.add("Mismatched Value counts: expected " + expectedData.size() + " result: " + resultData.size());
        }

        compareSegmentBoundaries(expected.getBoundary(), result.getBoundary(), errorMessages);
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
    public static void compareSegmentBoundaries(SegmentBoundary expected, SegmentBoundary result, List<String> errorMessages) {

        if (!expected.getBoundaryType().equals(result.getBoundaryType())) {
            errorMessages.add("Mismatched boundary type: expected " + expected.getBoundaryType() + " result: " + result.getBoundaryType());
        } else {
            BoundaryType boundaryType = result.getBoundaryType();
            switch (boundaryType) {
                case TIME_MILLI:
                case TEXT_CHAR:
                    if ((expected.getStart() != result.getStart()) || (expected.getEnd() != result.getEnd())) {
                        errorMessages.add("Mismatched " + boundaryType + " boundary: expected [" + expected.getStart() + "-" + expected.getEnd() + "], result ["
                                        + result.getStart() + "-" + result.getEnd() + "]");
                    }
                    break;
                case POINTS:
                    if (!expected.getPointsList().equals(result.getPointsList())) {
                        errorMessages.add("Mismatched rectangle boundary: expected " + expected.getPointsList() + " result: " + result.getPointsList());
                    }
                    break;
                case ALL:
                case UNKNOWN:
                    // no error, safe to ignore.
                    break;
                default:
                    throw new IllegalArgumentException("Encountered unexpected boundary case: " + boundaryType);
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
        Map<String,String> extensionMap = segmentValue.getExtensionMap();
        if (!extensionMap.isEmpty()) {
            for (Map.Entry<String,String> e : extensionMap.entrySet()) {
                b.append('-').append(e.getKey()).append(':').append(e.getValue());
            }
        }
        b.append("--").append(segmentValue.getScore());
        return b.toString();
    }
}
