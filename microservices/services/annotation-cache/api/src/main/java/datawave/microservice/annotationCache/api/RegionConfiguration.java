package datawave.microservice.annotationCache.api;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import lombok.Data;

/**
 * Configures this service's logical annotation-cache region. Its name is compared with each annotation's {@code region.id} to identify local writes and avoid
 * re-federating them.
 */
@Configuration
@Data
@ConfigurationProperties(prefix = "region")
public class RegionConfiguration {
    /** This deployment's region identity, corresponding to the annotation {@code region.id} value. */
    String name;
}
