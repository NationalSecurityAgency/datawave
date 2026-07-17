package datawave.microservice.annotation.writers.file.config;

import java.util.List;

import javax.validation.Valid;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import datawave.microservice.annotation.writers.AnnotationWriter;
import datawave.microservice.annotation.writers.file.FileAnnotationWriter;

/**
 * Configures an FileAnnotationWriter to process messages received by the annotation service in the case that our messaging infrastructure has failed. This
 * configuration is activated via the 'annotation.writers.file.enabled' property.
 *
 */
@Configuration
@ConditionalOnProperty(name = "annotation.writer.file.enabled", havingValue = "true")
public class FileAnnotationWriterConfig {

    @Bean("fileAnnotationWriterProperties")
    @Valid
    @ConfigurationProperties("annotation.writer.file")
    public FileAnnotationWriterProperties fileAnnotationWriterProperties() {
        return new FileAnnotationWriterProperties();
    }

    @Bean(name = "fileAnnotationWriter")
    public AnnotationWriter fileAnnotationWriter(@Qualifier("fileAnnotationWriterProperties") FileAnnotationWriterProperties fileAnnotationWriterProperties)
                    throws Exception {
        List<String> fsConfigResources = fileAnnotationWriterProperties.getFsConfigResources();

        String subPath = fileAnnotationWriterProperties.getSubPath();
        if (subPath == null && fileAnnotationWriterProperties.getSubPathEnvVar() != null)
            subPath = System.getenv(fileAnnotationWriterProperties.getSubPathEnvVar());

        // @formatter:off
        return new FileAnnotationWriter.Builder<>()
                .setUser(fileAnnotationWriterProperties.getUser())
                .setPath(fileAnnotationWriterProperties.getPathUri())
                .setSubPath(subPath)
                .setFsConfigResources(fsConfigResources)
                .setMaxFileAgeSeconds(fileAnnotationWriterProperties.getMaxFileAgeSeconds())
                .setMaxFileLengthMB(fileAnnotationWriterProperties.getMaxFileLengthMB())
                .setPrefix(fileAnnotationWriterProperties.getPrefix())
                .build();
        // @formatter:on
    }
}
