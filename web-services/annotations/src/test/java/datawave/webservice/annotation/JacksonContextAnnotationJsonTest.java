package datawave.webservice.annotation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.util.JsonFormat;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.Segment;
import datawave.annotation.protobuf.v1.VectorPayload;
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

    @ParameterizedTest
    @ValueSource(strings = {"upstream-embedding-f16.json", "upstream-embedding-f32.json"})
    public void testUpstreamVectorResponseSerialization(String filename) throws Exception {
        Annotation.Builder builder = Annotation.newBuilder();
        try (InputStream stream = getClass().getResourceAsStream("/embedding/" + filename)) {
            assertNotNull(stream, filename);
            protobufParser.merge(new String(stream.readAllBytes(), StandardCharsets.UTF_8), builder);
        }
        Annotation annotation = builder.build();
        VectorPayload vector = annotation.getSegments(0).getValues(0).getVector();
        String json = objectMapper.writeValueAsString(annotation);
        JsonNode valueJson = objectMapper.readTree(json).path("segments").get(0).path("values").get(0);
        JsonNode vectorJson = valueJson.path("vector");
        assertFalse(valueJson.has("value"));
        assertTrue(vectorJson.has("normalized"));
        assertFalse(vectorJson.path("normalized").booleanValue());
        assertEquals(Base64.getEncoder().encodeToString(vector.getData().toByteArray()), vectorJson.path("data").textValue());
        assertEquals(vector.getElementType().name(), vectorJson.path("elementType").textValue());
        assertEquals("IMAGE", vectorJson.path("modality").textValue());
        assertEquals("image-space-v1", vectorJson.path("embeddingSpaceId").textValue());
        assertEquals(annotation, objectMapper.readValue(json, Annotation.class));

        String listJson = objectMapper.writeValueAsString(List.of(annotation));
        assertEquals(List.of(annotation), objectMapper.readValue(listJson, new TypeReference<List<Annotation>>() {}));

        Segment segment = annotation.getSegments(0);
        assertEquals(segment, objectMapper.readValue(objectMapper.writeValueAsString(segment), Segment.class));
    }
}
