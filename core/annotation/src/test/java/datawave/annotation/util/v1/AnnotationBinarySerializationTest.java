package datawave.annotation.util.v1;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import com.google.protobuf.util.JsonFormat;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.AnnotationList;
import datawave.annotation.test.v1.AnnotationTestDataUtil;

/**
 * Performs regression tests to ensure that the protobuf binary serialization and deserialization process for Annotations remains compatible with historic
 * versions.
 */
public class AnnotationBinarySerializationTest {

    private static final String SERIALIZED_ANNOTATION_VERSIONS_DIR = "src/test/resources/serialized_annotation_versions/";
    private static final String SERIALIZED_ANNOTATION_README = "See " + SERIALIZED_ANNOTATION_VERSIONS_DIR + "README.md for more details";

    @Test
    public void testProtobufSerializationBaselineBinary() throws Exception {
        JsonFormat.Printer printer = AnnotationJsonUtils.getPrinter();
        Path baselineData = Path.of(SERIALIZED_ANNOTATION_VERSIONS_DIR + "annotation_baseline.bin");
        Path currentData = Path.of(SERIALIZED_ANNOTATION_VERSIONS_DIR + "annotation_current.bin");

        try (FileOutputStream out = new FileOutputStream(currentData.toFile())) {
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

        byte[] baselineBytes = Files.readAllBytes(baselineData);
        byte[] currentBytes = Files.readAllBytes(currentData);

        //@formatter:off
        assertArrayEquals(baselineBytes, currentBytes, "If this test fails it means that the protobuf binary" +
                " serialization process has changed. The output in " + SERIALIZED_ANNOTATION_VERSIONS_DIR +
                " annotation_current.bin should be added as a new test input in " + SERIALIZED_ANNOTATION_VERSIONS_DIR +
                " so that testProtobufDeserializationBaselineBinary will attempt to decode it in future versions. This" +
                " ensures backward compatibility of the deserialization process. " + SERIALIZED_ANNOTATION_README);
        //@formatter:on

        // delete the current version on successful completion of the test, it is only needed for debugging when the test fails.
        Files.delete(currentData);
    }

    @Test
    public void testProtobufDeserializationBaselineBinary() throws Exception {
        Path basedir = Path.of(SERIALIZED_ANNOTATION_VERSIONS_DIR);
        try (Stream<Path> fileStream = Files.walk(basedir).filter(p -> p.toString().endsWith(".bin"))) {
            fileStream.forEach(p -> {
                final List<Annotation> parsedAnnotations;

                //@formatter:off
                final String failureMessage =
                        "If this test fails it means that there is a regression in the protobuf binary deserialization" +
                        " process. The code in the current branch could not read historic serialized" +
                        " protobuf data from an earlier version of datawave stored in " + SERIALIZED_ANNOTATION_VERSIONS_DIR + "." +
                        " This is a critical failure and should be investigated immediately. " + SERIALIZED_ANNOTATION_README;
                //@formatter:on

                // idiomatic approach for reading annotations in ndjson format.
                try (FileInputStream is = new FileInputStream(p.toFile())) {
                    parsedAnnotations = new ArrayList<>(AnnotationList.parseFrom(is).getAnnotationsList());

                    assertEquals(36, parsedAnnotations.size(), "Did not observe the expected number of annotations in " + p + ". " + failureMessage);
                } catch (IOException e) {
                    fail("IOException deserializing input file: " + p + ". " + failureMessage, e);
                }
            });
        }
    }
}
