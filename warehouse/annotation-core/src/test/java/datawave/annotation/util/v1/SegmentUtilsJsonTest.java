package datawave.annotation.util.v1;

import static datawave.annotation.util.v1.AnnotationUtils.getBoundaryCase;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;

import datawave.annotation.protobuf.v1.Segment;
import datawave.annotation.protobuf.v1.SegmentBoundary;
import datawave.annotation.protobuf.v1.SegmentValue;
import datawave.annotation.protobuf.v1.TextSpanChars;
import datawave.annotation.protobuf.v1.TimeSpanSeconds;
import datawave.annotation.test.v1.AnnotationTestDataUtil;

public class SegmentUtilsJsonTest {

    private static final Logger log = LoggerFactory.getLogger(SegmentUtilsJsonTest.class);

    //@formatter:off
    final String testJson = "{\n" +
            "  \"segmentId\": \"5674ff10\",\n" +
            "  \"boundary\": {\n" +
            "    \"boundaryType\": \"TIME_SPAN\",\n" +
            "    \"timeSpan\": {\n" +
            "      \"startSeconds\": 0.154,\n" +
            "      \"endSeconds\": 0.52\n" +
            "    }\n" +
            "  },\n" +
            "  \"values\": [{\n" +
            "    \"value\": \"horse\",\n" +
            "    \"score\": 0.20999999344348907\n" +
            "  }]\n" +
            "}";

    final String testMalformedJsonOne = "{\n" +
            "  \"segmentId\": \"5674ff10\",\n" +
            "  \"boundary\": {\n" +
            "    \"boundaryType\": \"TIME_SPAN\",\n" +
            "    \"timeSpan\": {\n" +
            "      \"startSeconds\": 0.154,\n" +
            "      \"endSeconds\": 0.52\n" +
            "    }\n" +
            "  },\n" +
            "  \"values\": {\n" + // missing list in segment values.
            "    \"value\": \"horse\",\n" +
            "    \"score\": 0.20999999344348907\n" +
            "  }\n" +
            "}";

    final String testMalformedJsonTwo = "{\n" +
            "  \"segmentId\": \"5674ff10\",\n" +
            "  \"boundary\": [{\n" + // list in boundary
            "    \"boundaryType\": \"TIME_SPAN\",\n" +
            "    \"timeSpan\": {\n" +
            "      \"startSeconds\": 0.154,\n" +
            "      \"endSeconds\": 0.52\n" +
            "    }\n" +
            "  }],\n" +
            "  \"values\": [{\n" +
            "    \"value\": \"horse\",\n" +
            "    \"score\": 0.20999999344348907\n" +
            "  }]\n" +
            "}";
    //@formatter:on

    @Test
    public void testToJson() throws Exception {
        Segment s = AnnotationTestDataUtil.generateTestSegment();
        String json = AnnotationJsonUtils.segmentToJsonWithBoundaryType(s);
        log.info(json);
        assertTrue(json.contains("\"boundaryType\": \"TIME_SPAN\""));
        assertTrue(json.contains("\"startSeconds\": 0.154"));
        assertTrue(json.contains("\"endSeconds\": 0.52"));

    }

    @Test
    public void testEmptyToJson() throws InvalidProtocolBufferException {
        Segment s = Segment.newBuilder().build();
        String json = AnnotationJsonUtils.segmentToJsonWithBoundaryType(s);
        log.info(json);
        assertTrue(json.contains("{\n}"));
    }

    @Test
    public void testMultiSegmentBoundaryToJson() throws InvalidProtocolBufferException {
        TimeSpanSeconds timeSpanSeconds = TimeSpanSeconds.newBuilder().setStartSeconds(1).setEndSeconds(2).build();
        TextSpanChars textSpanChars = TextSpanChars.newBuilder().setStartCharacter(4).setEndCharacter(10).build();

        // the behavior of this step is undefined - we should not try to set two different spans on a single segment boundary
        // in this case the behavior is determined by how we resolve boundary types in AnnotationUtils, but that's an
        // implementation detail. Also, this would be caught by something that checks the object for validity prior to
        // persistence.
        SegmentBoundary bounds = SegmentBoundary.newBuilder().setTimeSpan(timeSpanSeconds).setCharacterSpan(textSpanChars).build();

        Segment s = Segment.newBuilder().setBoundary(bounds).build();
        String json = AnnotationJsonUtils.segmentToJsonWithBoundaryType(s);
        log.info(json);
        assertFalse(json.contains("CHARACTER_SPAN"));
        assertTrue(json.contains("TIME_SPAN"));
    }

    @Test
    public void testFromJson() throws Exception {
        Segment s = AnnotationJsonUtils.segmentFromJson(testJson);
        log.info(s.toString());
        assertEquals("5674ff10", s.getSegmentId());
        List<SegmentValue> segmentValueList = s.getValuesList();
        assertEquals(1, segmentValueList.size());
        SegmentValue sv = segmentValueList.get(0);
        assertEquals("horse", sv.getValue());
        assertEquals(0.2, sv.getScore(), 0.1f);
        SegmentBoundary bounds = s.getBoundary();
        assertEquals(AnnotationUtils.BoundaryCase.TIME_SPAN, getBoundaryCase(bounds));
        TimeSpanSeconds span = bounds.getTimeSpan();
        assertEquals(0.154, span.getStartSeconds());
        assertEquals(0.52, span.getEndSeconds());
        assertEquals("TIME_SPAN", bounds.getBoundaryType());
    }

    @Test
    public void testFromMalformedJsonOne() {
        //@formatter:off
        Exception e = assertThrows(
                InvalidProtocolBufferException.class,
                () -> AnnotationJsonUtils.segmentFromJson(testMalformedJsonOne),
                "Expected an exception from malformed json");
        //@formatter:on
        System.out.println(e.getMessage());
        assertTrue(e.getMessage().contains("Expect an array"));

    }

    @Test
    public void testFromMalformedJsonTwo() {
        //@formatter:off
        Exception e = assertThrows(
                InvalidProtocolBufferException.class,
                () -> AnnotationJsonUtils.segmentFromJson(testMalformedJsonTwo),
                "Expected an exception from malformed json");
        //@formatter:on

        System.out.println(e.getMessage());
        assertTrue(e.getMessage().contains("Expect message object"));
    }
}
