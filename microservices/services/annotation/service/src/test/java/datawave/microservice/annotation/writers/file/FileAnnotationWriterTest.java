package datawave.microservice.annotation.writers.file;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.test.v1.AnnotationTestDataUtil;
import datawave.microservice.annotation.writers.AnnotationWriter;
import datawave.microservice.annotation.writers.file.config.FileAnnotationWriterConfig;
import datawave.microservice.annotation.writers.file.config.FileAnnotationWriterProperties;

/**
 * Verifies that the file annotation writer bean is created when {@code annotation.writers.file.enabled=true}, that its properties bind under
 * {@code annotation.writers.file}, and that the writer is configured with the expected path.
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = FileAnnotationWriterConfig.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles({"FileAnnotationWriterTest", "file-enabled"})
public class FileAnnotationWriterTest {

    @Autowired
    private ApplicationContext context;

    @Autowired(required = false)
    private FileAnnotationWriterProperties fileAnnotationWriterProperties;

    @Test
    public void testFileWriterBeanPresent() {
        assertTrue(context.containsBean("fileAnnotationWriter"), "expected fileAnnotationWriter to be present");
        assertTrue(context.containsBean("fileAnnotationWriterProperties"), "expected fileAnnotationWriterProperties to be present");
    }

    @Test
    public void testFileWriterPropertiesBound() {
        assertNotNull(fileAnnotationWriterProperties, "expected annotation.writers.file properties to bind");
        assertEquals("file:///tmp/annotation-test-file-writer", fileAnnotationWriterProperties.getPathUri());
    }

    /**
     * Regression test: the {@link FileAnnotationWriter.Builder}'s own hardcoded default prefix was previously left over, verbatim, from the audit-service class
     * this writer was copied from ("audit"), and was never overridden by production configuration (the {@code annotation.writers.file.prefix} property has no
     * default and {@code annotation.yml} never sets it). This meant real annotation file-fallback output was misleadingly named {@code audit-<timestamp>.json}
     * instead of something annotation-specific. Verifies that a writer built with no explicit prefix (mirroring production configuration) produces files named
     * {@code annotation-<timestamp>.json}.
     */
    @Test
    public void testDefaultPrefixProducesAnnotationNamedFiles() throws Exception {
        java.nio.file.Path tempDir = Files.createTempDirectory("annotation-file-writer-naming-test");
        try {
            AnnotationWriter writer = new FileAnnotationWriter.Builder<>().setPath("file://" + tempDir.toAbsolutePath()).build();

            Annotation annotation = AnnotationTestDataUtil.generateTestAnnotation();
            Optional<Annotation> result = writer.write(annotation);
            assertNotNull(result, "expected a write result");
            assertTrue(result.isPresent(), "expected the write to succeed");

            try (Stream<java.nio.file.Path> files = Files.list(tempDir)) {
                java.nio.file.Path writtenFile = files.filter(p -> !Files.isDirectory(p)).findFirst()
                                .orElseThrow(() -> new AssertionError("expected a file to have been written"));
                assertTrue(writtenFile.getFileName().toString().startsWith("annotation-"),
                                "expected the written file to be named with the annotation-specific prefix, but was: " + writtenFile.getFileName());
            }
        } finally {
            try (Stream<java.nio.file.Path> walk = Files.walk(tempDir)) {
                walk.sorted((a, b) -> b.compareTo(a)).forEach(p -> {
                    try {
                        Files.deleteIfExists(p);
                    } catch (IOException ignored) {
                        // best-effort cleanup
                    }
                });
            }
        }
    }

    /**
     * Regression test: {@code FileAnnotationWriter} previously called {@code UserGroupInformation.setLoginUser(ugi)} during construction, mutating JVM-global
     * Hadoop login state. Constructing a writer must not alter the process-wide login user, since multiple writer instances (e.g. a "file" writer and a "dump"
     * writer) can be constructed in the same JVM.
     */
    @Test
    public void testConstructionDoesNotMutateGlobalLoginUser() throws Exception {
        UserGroupInformation loginUserBefore = UserGroupInformation.getLoginUser();

        java.nio.file.Path tempDir = Files.createTempDirectory("annotation-file-writer-ugi-test");
        try {
            new FileAnnotationWriter.Builder<>().setPath("file://" + tempDir.toAbsolutePath()).setUser("someOtherImpersonatedUser").build();

            UserGroupInformation loginUserAfter = UserGroupInformation.getLoginUser();
            assertEquals(loginUserBefore.getUserName(), loginUserAfter.getUserName(),
                            "constructing a FileAnnotationWriter must not mutate the JVM-global Hadoop login user");
        } finally {
            try (Stream<java.nio.file.Path> walk = Files.walk(tempDir)) {
                walk.sorted((a, b) -> b.compareTo(a)).forEach(p -> {
                    try {
                        Files.deleteIfExists(p);
                    } catch (IOException ignored) {
                        // best-effort cleanup
                    }
                });
            }
        }
    }
}
