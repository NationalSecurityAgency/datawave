package datawave.microservice.annotation.writers.accumulo.config;

import javax.validation.Valid;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import datawave.annotation.data.v1.AccumuloAnnotationSerializer;
import datawave.annotation.data.v1.AccumuloAnnotationSourceSerializer;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.microservice.annotation.common.AnnotationConsumer;
import datawave.microservice.annotation.writers.AnnotationWriter;
import datawave.microservice.annotation.writers.accumulo.AccumuloAnnotationWriter;

/**
 * Configures the AccumuloAnnotationWriter to process messages received by the annotation service. This configuration is activated via the
 * 'annotation.writers.accumulo.enabled' property. When enabled, this configuration will also enable the appropriate Spring Cloud Stream configuration for the
 * accumulo annotation writer binding, as specified in the annotation config.
 */
@Configuration
@EnableConfigurationProperties(AccumuloAnnotationWriterProperties.class)
@ConditionalOnProperty(name = "annotation.writers.accumulo.enabled", havingValue = "true")
public class AccumuloAnnotationWriterConfig {

    @Bean("accumuloAnnotationWriterProperties")
    @Valid
    public AccumuloAnnotationWriterProperties accumuloAnnotationWriterProperties() {
        return new AccumuloAnnotationWriterProperties();
    }

    @Bean
    public AnnotationConsumer accumuloAnnotationSink(AnnotationWriter accumuloAnnotationWriter) {
        return new AnnotationConsumer(accumuloAnnotationWriter);
    }

    @Bean
    public AnnotationWriter accumuloAnnotationWriter(AccumuloAnnotationWriterProperties accumuloAnnotationWriterProperties,
                    AccumuloConnectionFactory accumuloPool, AccumuloAnnotationSerializer annotationSerializer,
                    AccumuloAnnotationSourceSerializer annotationSourceSerializer) {
        return new AccumuloAnnotationWriter(accumuloPool, accumuloAnnotationWriterProperties, annotationSerializer, annotationSourceSerializer);
    }
}
