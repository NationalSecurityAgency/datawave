package datawave.microservice.annotation.writers.file;

import static org.junit.jupiter.api.Assertions.assertFalse;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import datawave.microservice.annotation.writers.file.config.DumpAnnotationWriterConfig;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = DumpAnnotationWriterConfig.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles({"DumpAnnotationWriterConfigDisabledTest", "dump-disabled"})
public class DumpAnnotationWriterConfigDisabledTest {

    @Autowired
    private ApplicationContext context;

    @Test
    public void testBeansMissing() {
        assertFalse(context.containsBean("dumpAnnotationWriter"));
        assertFalse(context.containsBean("dumpAnnotationWriterProperties"));
        assertFalse(context.containsBean("dumpAnnotationSink"));
    }
}
