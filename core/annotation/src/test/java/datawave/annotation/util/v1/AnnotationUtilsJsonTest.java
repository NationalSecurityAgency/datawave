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
            "  \"annotationId\": \"23BD91EC\",\n" +
            "  \"documentId\": \"1234567890\",\n" +
            "  \"annotationType\": \"testAnnotationType\",\n" +
            "  \"analyticSourceHash\": \"F1A0463C207B3778B472B506F3F8351A\",\n" +
            "  \"source\": {\n" +
            "    \"analyticHash\": \"86226841\",\n" +
            "    \"analyticSourceHash\": \"F1A0463C207B3778B472B506F3F8351A\",\n" +
            "    \"engine\": \"inline v6\",\n" +
            "    \"model\": \"GR Supra\",\n" +
            "    \"configuration\": {\n" +
            "      \"octane\": \"99\",\n" +
            "      \"model_year\": \"2025\"\n" +
            "    },\n" +
            "    \"metadata\": {\n" +
            "      \"visibility\": \"PUBLIC\",\n" +
            "      \"created_date\": \"2025-10-01T00:00:00Z\"\n" +
            "    }\n" +
            "  },\n" +
            "  \"segments\": [{\n" +
            "    \"segmentHash\": \"053D8C2B\",\n" +
            "    \"boundary\": {\n" +
            "      \"boundaryType\": \"TIME_MILLI\",\n" +
            "      \"start\": 1540,\n" +
            "      \"end\": 5200\n" +
            "    },\n" +
            "    \"values\": [{\n" +
            "      \"value\": \"cow\",\n" +
            "      \"score\": 0.235\n" +
            "    }, {\n" +
            "      \"value\": \"horse\",\n" +
            "      \"score\": 0.21,\n" +
            "      \"extension\": {\n" +
            "        \"objectType\": \"animal\"\n" +
            "      }\n" +
            "    }]\n" +
            "  }],\n" +
            "  \"metadata\": {\n" +
            "    \"visibility\": \"PUBLIC\",\n" +
            "    \"foo\": \"bar\",\n" +
            "    \"plough\": \"plover\",\n" +
            "    \"created_date\": \"2025-10-01T00:00:00Z\"\n" +
            "  }\n" +
            "}";
    //@formater:on

    @Test
    public void testToJson() throws Exception {
        Annotation a = AnnotationTestDataUtil.generateTestAnnotation();
        String json = AnnotationJsonUtils.annotationToJsonWithIds(a);
        log.info(json);
    }

    @Test
    public void testFromJson() throws Exception {
        Annotation testAnnotation = AnnotationTestDataUtil.generateTestAnnotation();
        Annotation expectedAnnotation = AnnotationUtils.injectAllHashes(testAnnotation);
        Annotation observedAnnotation = AnnotationJsonUtils.annotationFromJson(testAnnotationJson);
        AnnotationAssertions.assertAnnotationsEqual(expectedAnnotation, observedAnnotation);
    }
}
