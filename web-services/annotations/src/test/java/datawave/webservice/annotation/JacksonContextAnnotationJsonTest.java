package datawave.webservice.annotation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.util.JsonFormat;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.test.v1.AnnotationAssertions;
import datawave.annotation.test.v1.AnnotationTestDataUtil;
import datawave.webservice.common.json.JacksonContextResolver;

/**
 * Ensures that the Jackson annotation serialization configured for the webservice is compatible with canonical Protobuf based JSON serialization for
 * Annotations and Segments
 */
public class JacksonContextAnnotationJsonTest {

    private static final JacksonContextResolver jacksonResolver = new JacksonContextResolver();
    private static final ObjectMapper objectMapper = jacksonResolver.getContext(null);

    private static final JsonFormat.Printer protobufPrinter = JsonFormat.printer();
    private static final JsonFormat.Parser protobufParser = JsonFormat.parser();

    @Test
    public void testJacksonCanSerialize() {
        assertTrue(objectMapper.canSerialize(Annotation.class));
    }

    @Test
    public void testEqualSerialization() throws Exception {
        Annotation a = AnnotationTestDataUtil.generateTestAnnotation();
        String jacksonString = objectMapper.writeValueAsString(a);
        String protobufString = protobufPrinter.print(a);
        assertEquals(protobufString, jacksonString);
    }

    @Test
    public void testJacksonDeserialization() throws Exception {
        Annotation a = AnnotationTestDataUtil.generateTestAnnotation();
        String protobufString = protobufPrinter.print(a);
        Annotation b = objectMapper.readValue(protobufString, Annotation.class);
        AnnotationAssertions.assertAnnotationsEqual(a, b);
    }

    @Test
    public void testProtobufDeserialization() throws Exception {
        Annotation a = AnnotationTestDataUtil.generateTestAnnotation();
        String jacksonString = objectMapper.writeValueAsString(a);
        Annotation.Builder bb = Annotation.newBuilder();
        protobufParser.merge(jacksonString, bb);
        Annotation b = bb.build();
        AnnotationAssertions.assertAnnotationsEqual(a, b);
    }
}
