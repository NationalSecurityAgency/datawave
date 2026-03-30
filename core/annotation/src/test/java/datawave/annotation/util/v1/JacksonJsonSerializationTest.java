package datawave.annotation.util.v1;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.collections4.IteratorUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.protobuf.util.JsonFormat;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.AnnotationList;
import datawave.annotation.test.v1.AnnotationAssertions;
import datawave.annotation.test.v1.AnnotationTestDataUtil;

/**
 * Ensures that the Jackson Datatype Protobuf serialization is compatible with canonical Protobuf based JSON serialization for Annotations and Segments
 */
public class JacksonJsonSerializationTest {

    private static final Logger log = LoggerFactory.getLogger(JacksonJsonSerializationTest.class);

    private static final JsonFormat.Printer protobufPrinter = AnnotationJsonUtils.getPrinter();
    private static final JsonFormat.Parser protobufParser = AnnotationJsonUtils.getParser();

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
        log.debug(jacksonString);
        Annotation.Builder bb = Annotation.newBuilder();
        protobufParser.merge(jacksonString, bb);
        Annotation b = bb.build();
        AnnotationAssertions.assertAnnotationsEqual(a, b);
    }

    @Test
    @ResourceLock("annotation_baseline")
    public void testProtobufSerializationListBaseline() throws Exception {
        JsonFormat.Printer printer = AnnotationJsonUtils.getPrinter();
        Path p = Path.of("src/test/resources/annotation_baseline.json");
        PrintWriter out = new PrintWriter(new FileWriter(p.toFile()));
        //@formatter:off
        // boundary type and segment id generation are usually taken care of by the dao layer, we add them
        // here to create a simulation of that behavior.
        List<Annotation> testAnnotations = AnnotationTestDataUtil.generateManyTestAnnotations().stream()
                .map(AnnotationUtils::injectAllHashes)
                .collect(Collectors.toList());
        //@formatter:on
        AnnotationList annotationList = AnnotationList.newBuilder().addAllAnnotations(testAnnotations).build();
        printer.appendTo(annotationList, out);
        out.close();
    }

    @Test
    @ResourceLock("annotation_baseline")
    public void testProtobufDeserializationListBaseline() throws Exception {
        JsonFormat.Parser parser = AnnotationJsonUtils.getParser();
        Path p = Path.of("src/test/resources/annotation_baseline.json");
        List<Annotation> parsedAnnotations = new ArrayList<>();
        try (FileReader reader = new FileReader(p.toFile())) {
            AnnotationList.Builder builder = AnnotationList.newBuilder();
            parser.merge(reader, builder);
            AnnotationList aas = builder.build();
            parsedAnnotations.addAll(aas.getAnnotationsList());
        } catch (IOException e) {
            log.debug("Reached end of file?", e);
        }
        assertEquals(36, parsedAnnotations.size());
        // TODO more validation
    }

    @Test
    @ResourceLock("annotation_baseline_ndjson")
    public void testProtobufSerializationBaselineNdJson() throws Exception {
        JsonFormat.Printer printer = AnnotationJsonUtils.getPrinter();
        Path p = Path.of("src/test/resources/annotation_baseline.ndjson");
        PrintWriter out = new PrintWriter(new FileWriter(p.toFile()));
        //@formatter:off
        // boundary type and segment id generation are usually taken care of by the dao layer, we add them
        // here to create a simulation of that behavior.
        List<Annotation> testAnnotations = AnnotationTestDataUtil.generateManyTestAnnotations().stream()
                .map(AnnotationUtils::injectAllHashes)
                .collect(Collectors.toList());
        //@formatter:on
        AnnotationList annotationList = AnnotationList.newBuilder().addAllAnnotations(testAnnotations).build();
        for (Annotation annotation : annotationList.getAnnotationsList()) {
            printer.appendTo(annotation, out);
        }
        out.close();
    }

    @Test
    @ResourceLock("annotation_baseline_ndjson")
    public void testProtobufDeserializationBaselineNdJson() throws Exception {
        JsonFormat.Parser parser = AnnotationJsonUtils.getParser();
        Path p = Path.of("src/test/resources/annotation_baseline.ndjson");
        List<Annotation> parsedAnnotations = new ArrayList<>();
        // idiomatic approach for reading annotations in ndjson format.
        try (FileInputStream is = new FileInputStream(p.toFile())) {
            JsonReader reader = new JsonReader(new InputStreamReader(is));
            reader.setLenient(true);
            Iterator<JsonElement> jsonIterator = null;
            while (true) {
                if (jsonIterator == null || !jsonIterator.hasNext()) {
                    if (reader.peek() == JsonToken.END_DOCUMENT) {
                        break; // done reading all json
                    }
                    JsonElement root = JsonParser.parseReader(reader);
                    jsonIterator = root.isJsonArray() ? root.getAsJsonArray().iterator() : IteratorUtils.singletonIterator(root);
                }

                if (jsonIterator.hasNext()) {
                    JsonElement jsonElement = jsonIterator.next();
                    Annotation.Builder builder = Annotation.newBuilder();
                    String jsonString = jsonElement.toString();
                    parser.merge(jsonString, builder);
                    Annotation aas = builder.build();
                    parsedAnnotations.add(aas);
                }
            }
        } catch (IOException e) {
            log.debug("Reached end of file?", e);
        }
        assertEquals(36, parsedAnnotations.size());
        // TODO more validation
    }
}
