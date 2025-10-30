package datawave.annotation.util.v1;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.protobuf.util.JsonFormat;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.test.v1.AnnotationAssertions;
import datawave.annotation.test.v1.AnnotationTestDataUtil;

/**
 * Ensures that the Jackson Datatype Protobuf serialization is compatible with canonical Protobuf based JSON serialization for Annotations and Segments
 */
public class JacksonJsonSerializationTest {

    private static final JsonFormat.Printer protobufPrinter = JsonFormat.printer();
    private static final JsonFormat.Parser protobufParser = JsonFormat.parser();

    static ObjectMapper objectMapper;

    @BeforeAll
    public static void configureObjectMapper() {
        objectMapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(Annotation.class, new JacksonAnnotationDeserializer());
        module.addSerializer(Annotation.class, new JacksonAnnotationSerializer());
        objectMapper.registerModule(module);
    }

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
