package datawave.microservice.annotation.writers.file.config;

import java.util.List;

import javax.validation.Valid;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import datawave.microservice.annotation.common.AnnotationConsumer;
import datawave.microservice.annotation.writers.AnnotationWriter;
import datawave.microservice.annotation.writers.file.FileAnnotationWriter;

/**
 * Configures a {@link FileAnnotationWriter} to dump messages to the filesystem on demand. This configuration is activated via the
 * 'annotation.writers.dump.enabled' property. When enabled, this configuration will also enable the appropriate Spring Cloud Stream configuration for the dump
 * binding, as specified in the annotation config, subscribing to its own dedicated 'dump' consumer group independent of the standard messaging path.
 */
@Configuration
@EnableConfigurationProperties(DumpAnnotationWriterProperties.class)
@ConditionalOnProperty(name = "annotation.writers.dump.enabled", havingValue = "true")
public class DumpAnnotationWriterConfig {

    @Bean("dumpAnnotationWriterProperties")
    @Valid
    public DumpAnnotationWriterProperties dumpAnnotationWriterProperties() {
        return new DumpAnnotationWriterProperties();
    }

    @Bean(name = "dumpAnnotationWriter")
    public AnnotationWriter dumpAnnotationWriter(@Qualifier("dumpAnnotationWriterProperties") DumpAnnotationWriterProperties dumpAnnotationWriterProperties)
                    throws Exception {
        List<String> fsConfigResources = dumpAnnotationWriterProperties.getFsConfigResources();

        String subPath = dumpAnnotationWriterProperties.getSubPath();
        if (subPath == null && dumpAnnotationWriterProperties.getSubPathEnvVar() != null)
            subPath = System.getenv(dumpAnnotationWriterProperties.getSubPathEnvVar());

        // @formatter:off
        return new FileAnnotationWriter.Builder<>()
                .setUser(dumpAnnotationWriterProperties.getUser())
                .setPath(dumpAnnotationWriterProperties.getPathUri())
                .setSubPath(subPath)
                .setFsConfigResources(fsConfigResources)
                .setMaxFileAgeSeconds(dumpAnnotationWriterProperties.getMaxFileAgeSeconds())
                .setMaxFileLengthMB(dumpAnnotationWriterProperties.getMaxFileLengthMB())
                .setPrefix((dumpAnnotationWriterProperties.getPrefix() != null) ? dumpAnnotationWriterProperties.getPrefix() : "dump")
                .build();
        // @formatter:on
    }

    @Bean
    public AnnotationConsumer dumpAnnotationSink(@Qualifier("dumpAnnotationWriter") AnnotationWriter dumpAnnotationWriter) {
        return new AnnotationConsumer(dumpAnnotationWriter);
    }
}
