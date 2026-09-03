package datawave.microservice.annotation.writers.file;

import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import datawave.microservice.annotation.common.config.AccumuloConfiguration;
import datawave.microservice.annotation.common.config.AnnotationSerializerConfiguration;
import datawave.microservice.annotation.writers.accumulo.AccumuloAnnotationWriterTest;
import datawave.microservice.annotation.writers.accumulo.config.AccumuloAnnotationWriterConfig;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {AccumuloAnnotationWriterConfig.class, AccumuloConfiguration.class, AnnotationSerializerConfiguration.class,
        AccumuloAnnotationWriterTest.AccumuloAnnotationWriterTestConfiguration.class})
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = "spring.main.allow-bean-definition-overriding=true")
@ActiveProfiles({"FileAnnotationWriterTest", "accumulo-enabled"})
public class FileAnnotationWriterTest {}
