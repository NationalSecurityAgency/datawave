package datawave.microservice.annotation.writers.file;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

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
}
