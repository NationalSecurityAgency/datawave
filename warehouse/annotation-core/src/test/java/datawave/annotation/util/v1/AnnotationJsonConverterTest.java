package datawave.annotation.util.v1;

import datawave.annotation.protobuf.v1.Annotation;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AnnotationJsonConverterTest {

    private static final Logger log = LoggerFactory.getLogger(AnnotationJsonConverterTest.class);

    String testJson = "{\n" +
            "  \"shard\": \"20250704_249\",\n" +
            "  \"dataType\": \"testDataType\",\n" +
            "  \"uid\": \"abcde.fghij.klmno\",\n" +
            "  \"annotationType\": \"testAnnotationType\",\n" +
            "  \"annotationId\": \"bcb2bb84\",\n" +
            "  \"metadata\": {\n" +
            "    \"foo\": \"bar\",\n" +
            "    \"plough\": \"plover\"\n" +
            "  },\n" +
            "  \"segments\": [{\n" +
            "    \"segmentId\": \"5a7bcdd9\",\n" +
            "    \"segmentValue\": [{\n" +
            "      \"value\": \"cow\",\n" +
            "      \"score\": 0.23499999940395355\n" +
            "    }, {\n" +
            "      \"value\": \"horse\",\n" +
            "      \"score\": 0.20999999344348907,\n" +
            "      \"extension\": \"animal\"\n" +
            "    }],\n" +
            "    \"boundaryType\": \"TIME\",\n" +
            "    \"time\": {\n" +
            "      \"startSeconds\": 0.154,\n" +
            "      \"endSeconds\": 0.52\n" +
            "    }\n" +
            "  }]\n" +
            "}";

    @Test
    public void testToJson() throws Exception {
        Annotation a = AnnotationTestUtil.generateTestAnnotation();
        String json = AnnotationUtils.toJsonWithBoundaryTypes(a); // TODO: don't throw Exception, choose something better
        log.info(json);
    }

    @Test
    public void testFromJson() throws Exception {
        Annotation expectedAnnotation = AnnotationTestUtil.generateTestAnnotation();
        Annotation observedAnnotation = AnnotationUtils.fromJson(testJson);
        AnnotationTestUtil.assertAnnotationsEqual(expectedAnnotation, observedAnnotation);
    }
}
