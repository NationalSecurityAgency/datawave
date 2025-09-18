package datawave.annotation.util.v1;

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
import datawave.annotation.protobuf.v1.SegmentValue;
import datawave.annotation.protobuf.v1.TextSpanChars;
import datawave.annotation.protobuf.v1.TimeSpanSeconds;
import datawave.annotation.test.v1.AnnotationTestDataUtil;

public class SegmentUtilsJsonTest {

    private static final Logger log = LoggerFactory.getLogger(SegmentUtilsJsonTest.class);

    //@formatter:off
    final String testJson = "{\n" +
            "  \"segmentId\": \"5674ff10\",\n" +
            "  \"segmentValue\": [{\n" +
            "    \"value\": \"horse\",\n" +
            "    \"score\": 0.20999999344348907\n" +
            "  }],\n" +
            "  \"boundaryType\": \"TIME\",\n" +
            "  \"time\": {\n" +
            "    \"startSeconds\": 0.154,\n" +
            "    \"endSeconds\": 0.52\n" +
            "  }\n" +
            "}";

    final String testMalformedJsonOne = "{\n" +
            "  \"segmentId\": \"5674ff10\",\n" +
            "  \"segmentValue\": {\n" + // missing list in segmentValue
            "    \"value\": \"horse\",\n" +
            "    \"score\": 0.20999999344348907\n" +
            "  },\n" +
            "  \"boundaryType\": \"TIME\",\n" +
            "  \"time\": {\n" +
            "    \"startSeconds\": 0.154,\n" +
            "    \"endSeconds\": 0.52\n" +
            "  }\n" +
            "}";

    final String testMalformedJsonTwo = "{\n" +
            "  \"segmentId\": \"5674ff10\",\n" +
            "  \"segmentValue\": {\n" + // missing list in segmentValue
            "    \"value\": \"horse\",\n" +
            "    \"score\": \"0.20\",\n" +
            "  },\n" +
            "  \"boundaryType\": \"TIME\",\n" +
            "  \"time\": {\n" +
            "    \"startSeconds\": 0.154,\n" +
            "    \"endSeconds\": 0.52\n" +
            "  }\n" +
            "}";
    //@formatter:on

    @Test
    public void testToJson() throws Exception {
        Segment s = AnnotationTestDataUtil.generateTestSegment();
        String json = AnnotationUtils.segmentToJsonWithBoundaryType(s); // TODO: don't throw Exception, choose something better
        log.info(json);
        assertTrue(json.contains("\"boundaryType\": \"TIME\""));
        assertTrue(json.contains("\"startSeconds\": 0.154"));
        assertTrue(json.contains("\"endSeconds\": 0.52"));

    }

    @Test
    public void testEmptyToJson() throws InvalidProtocolBufferException {
        Segment s = Segment.newBuilder().build();
        String json = AnnotationUtils.segmentToJsonWithBoundaryType(s);
        log.info(json);
        assertTrue(json.contains("{\n}"));
    }

    @Test
    public void testMultiBoundsToJson() throws InvalidProtocolBufferException {
        TimeSpanSeconds timeSpace = TimeSpanSeconds.newBuilder().setStartSeconds(1).setEndSeconds(2).build();
        TextSpanChars textSpanChars = TextSpanChars.newBuilder().setStartCharacter(4).setEndCharacter(10).build();
        Segment s = Segment.newBuilder().setTime(timeSpace).setCharacters(textSpanChars).build();
        String json = AnnotationUtils.segmentToJsonWithBoundaryType(s);
        log.info(json);
        assertTrue(json.contains("CHARACTERS"));
        assertFalse(json.contains("TIME"));
    }

    @Test
    public void testFromJson() throws Exception {
        Segment s = AnnotationUtils.segmentFromJson(testJson);
        log.info(s.toString());
        assertEquals("5674ff10", s.getSegmentId());
        List<SegmentValue> segmentValueList = s.getSegmentValueList();
        assertEquals(1, segmentValueList.size());
        SegmentValue sv = segmentValueList.get(0);
        assertEquals("horse", sv.getValue());
        assertEquals(0.2, sv.getScore(), 0.1f);
        assertEquals(Segment.BoundaryCase.TIME, s.getBoundaryCase());
        TimeSpanSeconds span = s.getTime();
        assertEquals(0.154, span.getStartSeconds());
        assertEquals(0.52, span.getEndSeconds());
        assertEquals("TIME", s.getBoundaryType());
    }

    @Test
    public void testFromMalformedJsonOne() {
        //@formatter:off
        Exception e = assertThrows(
                InvalidProtocolBufferException.class,
                () -> AnnotationUtils.segmentFromJson(testMalformedJsonOne),
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
                () -> AnnotationUtils.segmentFromJson(testMalformedJsonTwo),
                "Expected an exception from malformed json");
        //@formatter:on

        System.out.println(e.getMessage());
        assertTrue(e.getMessage().contains("MalformedJsonException"));
    }
}
