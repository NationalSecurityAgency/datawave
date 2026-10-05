package datawave.microservice.annotation.writers.file.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Properties for the dump {@link datawave.microservice.annotation.writers.file.FileAnnotationWriter}, bound to the 'annotation.writers.dump' prefix. This
 * writer reuses the same {@link datawave.microservice.annotation.writers.file.FileAnnotationWriter} implementation as the fallback file writer, but is
 * configured independently and is intended to be triggered on-demand (e.g. via a dedicated stream consumer group) rather than as a messaging fallback.
 */
@Validated
@ConfigurationProperties(prefix = "annotation.writers.dump")
public class DumpAnnotationWriterProperties extends FileAnnotationWriterProperties {

}
