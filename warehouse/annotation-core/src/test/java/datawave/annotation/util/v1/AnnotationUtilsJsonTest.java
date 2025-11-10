package datawave.annotation.util.v1;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.test.v1.AnnotationAssertions;
import datawave.annotation.test.v1.AnnotationTestDataUtil;

public class AnnotationUtilsJsonTest {

    private static final Logger log = LoggerFactory.getLogger(AnnotationUtilsJsonTest.class);

    //@formatter:off
    String testAnnotationJson = "{\n" +
            "  \"shard\": \"20250704_249\",\n" +
            "  \"dataType\": \"testDataType\",\n" +
            "  \"uid\": \"abcde.fghij.klmno\",\n" +
            "  \"annotationType\": \"testAnnotationType\",\n" +
            "  \"annotationId\": \"628b014c\",\n" +
            "  \"segments\": [{\n" +
            "    \"segmentId\": \"cb693def\",\n" +
            "    \"boundary\": {\n" +
            "      \"boundaryType\": \"TIME_SPAN\",\n" +
            "      \"timeSpan\": {\n" +
            "        \"startSeconds\": 0.154,\n" +
            "        \"endSeconds\": 0.52\n" +
            "      }\n" +
            "    },\n" +
            "    \"values\": [{\n" +
            "      \"value\": \"cow\",\n" +
            "      \"score\": 0.23499999940395355\n" +
            "    }, {\n" +
            "      \"value\": \"horse\",\n" +
            "      \"score\": 0.20999999344348907,\n" +
            "      \"extension\": {\n" +
            "        \"objectType\": \"animal\"\n" +
            "      }\n" +
            "    }]\n" +
            "  }],\n" +
            "  \"metadata\": {\n" +
            "    \"visibility\": \"PUBLIC\",\n" +
            "    \"foo\": \"bar\",\n" +
            "    \"plough\": \"plover\",\n" +
            "    \"created_date\": \"2025-10-01T00:00:00.000Z\"\n" +
            "  }\n" +
            "}";
    //@formater:on

    @Test
    public void testToJson() throws Exception {
        Annotation a = AnnotationTestDataUtil.generateTestAnnotation();
        String json = AnnotationJsonUtils.annotationToJsonWithBoundaryTypesAndIds(a);
        log.info(json);
    }

    @Test
    public void testFromJson() throws Exception {
        Annotation testAnnotation = AnnotationTestDataUtil.generateTestAnnotation();
        Annotation expectedAnnotation = AnnotationUtils.injectAnnotationAndSegmentIds(testAnnotation);
        Annotation observedAnnotation = AnnotationJsonUtils.annotationFromJson(testAnnotationJson);
        AnnotationAssertions.assertAnnotationsEqual(expectedAnnotation, observedAnnotation);
    }
}
