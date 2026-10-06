package datawave.microservice.annotation.writers.accumulo;

import static org.junit.jupiter.api.Assertions.assertFalse;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import datawave.microservice.annotation.common.config.AccumuloConfiguration;
import datawave.microservice.annotation.common.config.AnnotationSerializerConfiguration;
import datawave.microservice.annotation.writers.accumulo.config.AccumuloAnnotationWriterConfig;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {AccumuloAnnotationWriterConfig.class, AccumuloConfiguration.class, AnnotationSerializerConfiguration.class})
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles({"AccumuloAnnotationWriterConfigDisabledTest", "accumulo-disabled"})
public class AccumuloAnnotationWriterConfigDisabledTest {

    @Autowired
    private ApplicationContext context;

    @Test
    public void testBeansMissing() {
        assertFalse(context.containsBean("accumuloAnnotationSink"));
        assertFalse(context.containsBean("accumuloAnnotationWriter"));
        assertFalse(context.containsBean("connector"));
    }
}
