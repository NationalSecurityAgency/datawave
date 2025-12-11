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

import datawave.annotation.protobuf.v1.BoundaryType;
import datawave.annotation.protobuf.v1.Segment;
import datawave.annotation.protobuf.v1.SegmentBoundary;
import datawave.annotation.protobuf.v1.SegmentValue;
import datawave.annotation.test.v1.AnnotationTestDataUtil;

public class SegmentUtilsJsonTest {

    private static final Logger log = LoggerFactory.getLogger(SegmentUtilsJsonTest.class);

    //@formatter:off
    final String testJson = "{\n" +
            "  \"segmentHash\": \"5674ff10\",\n" +
            "  \"boundary\": {\n" +
            "    \"boundaryType\": \"TIME_MILLI\",\n" +
            "    \"start\": 1540,\n" +
            "    \"end\": 5200\n" +
            "  },\n" +
            "  \"values\": [{\n" +
            "    \"value\": \"horse\",\n" +
            "    \"score\": 0.20999999344348907\n" +
            "  }]\n" +
            "}";

    final String testMalformedJsonOne = "{\n" +
            "  \"segmentHash\": \"5674ff10\",\n" +
            "  \"boundary\": {\n" +
            "    \"boundaryType\": \"TIME_MILLI\",\n" +
            "    \"start\": 1540,\n" +
            "    \"end\": 5200\n" +
            "  },\n" +
            "  \"values\": {\n" + // missing list in segment values.
            "    \"value\": \"horse\",\n" +
            "    \"score\": 0.20999999344348907\n" +
            "  }\n" +
            "}";

    final String testMalformedJsonTwo = "{\n" +
            "  \"segmentHash\": \"5674ff10\",\n" +
            "  \"boundary\": [{\n" +
            "    \"boundaryType\": \"TIME_MILLI\",\n" +
            "    \"start\": 1540,\n" +
            "    \"end\": 5200\n" +
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
        String json = AnnotationJsonUtils.segmentToJson(s);
        log.info(json);
        assertTrue(json.contains("\"boundaryType\": \"TIME_MILLI\""));
        assertTrue(json.contains("\"start\": 1540"));
        assertTrue(json.contains("\"end\": 5200"));

    }

    @Test
    public void testEmptyToJson() throws InvalidProtocolBufferException {
        Segment s = Segment.newBuilder().build();
        String json = AnnotationJsonUtils.segmentToJson(s);
        log.info(json);
        assertTrue(json.contains("{\n}"));
    }

    @Test
    public void testMultiSegmentBoundaryToJson() throws InvalidProtocolBufferException {
        // the behavior of this step is undefined - we should not try to set two different spans on a single segment boundary
        // in this case the behavior is determined by how we resolve boundary types in AnnotationUtils, but that's an
        // implementation detail. Also, this would be caught by something that checks the object for validity prior to
        // persistence.
        SegmentBoundary bounds = SegmentBoundary.newBuilder().setBoundaryType(BoundaryType.TIME_MILLI).setBoundaryType(BoundaryType.TEXT_CHAR).setStart(1)
                        .setEnd(2).setStart(4).setEnd(10).build();

        Segment s = Segment.newBuilder().setBoundary(bounds).build();
        String json = AnnotationJsonUtils.segmentToJson(s);
        log.info(json);
        assertTrue(json.contains("TEXT_CHAR"));
        assertFalse(json.contains("TIME_MILLI"));
    }

    @Test
    public void testFromJson() throws Exception {
        Segment s = AnnotationJsonUtils.segmentFromJson(testJson);
        log.info(s.toString());
        assertEquals("5674ff10", s.getSegmentHash());
        List<SegmentValue> segmentValueList = s.getValuesList();
        assertEquals(1, segmentValueList.size());
        SegmentValue sv = segmentValueList.get(0);
        assertEquals("horse", sv.getValue());
        assertEquals(0.2, sv.getScore(), 0.1f);
        SegmentBoundary bounds = s.getBoundary();
        assertEquals(BoundaryType.TIME_MILLI, bounds.getBoundaryType());
        assertEquals(1540, bounds.getStart());
        assertEquals(5200, bounds.getEnd());
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
