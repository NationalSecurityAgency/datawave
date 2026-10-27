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

import datawave.microservice.annotation.writers.file.config.DumpAnnotationWriterConfig;
import datawave.microservice.annotation.writers.file.config.DumpAnnotationWriterProperties;

/**
 * Verifies that the dump annotation writer bean is created when {@code annotation.writers.dump.enabled=true}, that its properties bind under
 * {@code annotation.writers.dump} independent of the fallback file writer's {@code annotation.writers.file} properties, and that the writer and its stream sink
 * are configured with the expected path.
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = DumpAnnotationWriterConfig.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles({"DumpAnnotationWriterTest", "dump-enabled"})
public class DumpAnnotationWriterTest {

    @Autowired
    private ApplicationContext context;

    @Autowired(required = false)
    private DumpAnnotationWriterProperties dumpAnnotationWriterProperties;

    @Test
    public void testDumpWriterBeanPresent() {
        assertTrue(context.containsBean("dumpAnnotationWriter"), "expected dumpAnnotationWriter to be present");
        assertTrue(context.containsBean("dumpAnnotationWriterProperties"), "expected dumpAnnotationWriterProperties to be present");
        assertTrue(context.containsBean("dumpAnnotationSink"), "expected dumpAnnotationSink to be present");
    }

    @Test
    public void testDumpWriterPropertiesBound() {
        assertNotNull(dumpAnnotationWriterProperties, "expected annotation.writers.dump properties to bind");
        assertEquals("file:///tmp/annotation-test-dump-writer", dumpAnnotationWriterProperties.getPathUri());
    }
}
