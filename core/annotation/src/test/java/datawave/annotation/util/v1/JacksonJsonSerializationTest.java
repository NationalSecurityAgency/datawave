package datawave.annotation.util.v1;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.collections4.IteratorUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.protobuf.util.JsonFormat;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.AnnotationList;
import datawave.annotation.protobuf.v1.SegmentValue;
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
    @ResourceLock(value = "annotation_baseline", mode = ResourceAccessMode.READ)
    public void testProtobufDeserializationListBaselineWithZeroScore() throws Exception {
        JsonFormat.Parser parser = AnnotationJsonUtils.getParser();
        AnnotationList parsed = parseAnnotationListWithBaselineSegmentValueMutations(true, false, parser);
        SegmentValue value = parsed.getAnnotations(0).getSegments(0).getValues(0);

        assertEquals(0.0f, value.getScore(), 0.0f);
        assertTrue(value.hasScore());
    }

    @Test
    @ResourceLock(value = "annotation_baseline", mode = ResourceAccessMode.READ)
    public void testProtobufDeserializationListBaselineWithMissingScore() throws Exception {
        JsonFormat.Parser parser = AnnotationJsonUtils.getParser();
        AnnotationList parsed = parseAnnotationListWithBaselineSegmentValueMutations(false, true, parser);
        SegmentValue value = parsed.getAnnotations(0).getSegments(0).getValues(1);

        assertEquals(0.0f, value.getScore(), 0.0f);
        assertFalse(value.hasScore());
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

    /**
     * Checks that the current serialized form matches the baseline, if not please update <code>src/test/resources/annotation_baseline.bin</code> once you can
     * explain why this broke
     */
    @Test
    @ResourceLock(value = "annotation_baseline_bin", mode = ResourceAccessMode.READ)
    public void testProtobufSerializationBaselineBinary() throws Exception {
        Path expectedPath = Path.of("src/test/resources/annotation_baseline.bin");
        byte[] expectedBytes = Files.readAllBytes(expectedPath);

        Path outputPath = Path.of("target/annotation_baseline.bin");
        try (FileOutputStream out = new FileOutputStream(outputPath.toFile())) {
            //@formatter:off
            // boundary type and segment id generation are usually taken care of by the dao layer, we add them
            // here to create a simulation of that behavior.
            List<Annotation> testAnnotations = AnnotationTestDataUtil.generateManyTestAnnotations().stream()
                    .map(AnnotationUtils::injectAllHashes)
                    .collect(Collectors.toList());
            //@formatter:on
            AnnotationList annotationList = AnnotationList.newBuilder().addAllAnnotations(testAnnotations).build();
            annotationList.writeTo(out);
        }
        byte[] outputBytes = Files.readAllBytes(outputPath);
        assertArrayEquals(expectedBytes, outputBytes);
    }

    /**
     * Checks that the baseline serialized form can be decoded without errors, if this fails someone made a breaking change to the protobuf definition. Do not
     * roll out code with a broken test here unless a migration plan is in place
     */
    @Test
    @ResourceLock(value = "annotation_baseline_bin", mode = ResourceAccessMode.READ)
    public void testProtobufDeserializationBaselineBinary() throws Exception {
        Path p = Path.of("src/test/resources/annotation_baseline.bin");
        List<Annotation> parsedAnnotations = new ArrayList<>();
        // idiomatic approach for reading annotations in ndjson format.
        try (FileInputStream is = new FileInputStream(p.toFile())) {
            parsedAnnotations.addAll(AnnotationList.parseFrom(is).getAnnotationsList());
        } catch (IOException e) {
            log.debug("Reached end of file?", e);
        }
        assertEquals(36, parsedAnnotations.size());
        // TODO more validation
    }

    private AnnotationList parseAnnotationListWithBaselineSegmentValueMutations(boolean setFirstScoreToZero, boolean removeSecondScore,
                    JsonFormat.Parser parser) throws Exception {
        Path p = Path.of("src/test/resources/annotation_baseline.json");
        ObjectNode root = (ObjectNode) objectMapper.readTree(Files.readString(p));
        ArrayNode values = root.withArray("annotations").get(0).withArray("segments").get(0).withArray("values");

        if (setFirstScoreToZero) {
            ((ObjectNode) values.get(0)).put("score", 0.0);
        }
        if (removeSecondScore) {
            ((ObjectNode) values.get(1)).remove("score");
        }

        AnnotationList.Builder builder = AnnotationList.newBuilder();
        parser.merge(objectMapper.writeValueAsString(root), builder);
        return builder.build();
    }
}
