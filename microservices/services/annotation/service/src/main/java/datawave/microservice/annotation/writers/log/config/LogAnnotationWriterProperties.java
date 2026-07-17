package datawave.microservice.annotation.writers.log.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import datawave.microservice.annotation.common.AnnotationConsumer;
import datawave.microservice.annotation.writers.AnnotationWriter;
import datawave.microservice.annotation.writers.log.LogAnnotationWriter;

/**
 * Configures the LogAnnotationWriter to process messages received by the annotation service. This configuration is activated via the
 * 'annotation.writers.log.enabled' property. When enabled, this configuration will also enable the appropriate Spring Cloud Stream configuration for the log
 * annotation binding, as specified in the annotation config.
 */
@Configuration
@ConditionalOnProperty(name = "annotation.writers.log.enabled", havingValue = "true")
public class LogAnnotationWriterProperties {
    @Bean
    public AnnotationConsumer logAnnotationSink(AnnotationWriter annotationWriter) {
        return new AnnotationConsumer(annotationWriter);
    }

    @Bean
    public AnnotationWriter logAnnotationWriter() {
        return new LogAnnotationWriter();
    }
}
