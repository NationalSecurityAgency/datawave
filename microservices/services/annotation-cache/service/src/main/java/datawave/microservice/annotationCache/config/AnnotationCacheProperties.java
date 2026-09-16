package datawave.microservice.annotationCache.config;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/** Retention policy for annotation-cache Hazelcast maps. */
@Configuration
@ConfigurationProperties(prefix = "annotation-cache")
public class AnnotationCacheProperties {
    private Duration maxCacheAge = Duration.ofHours(1);
    private Duration maxFetchAge = Duration.ofMinutes(5);

    public Duration getMaxCacheAge() {
        return maxCacheAge;
    }

    public void setMaxCacheAge(Duration maxCacheAge) {
        this.maxCacheAge = maxCacheAge;
    }

    public Duration getMaxFetchAge() {
        return maxFetchAge;
    }

    public void setMaxFetchAge(Duration maxFetchAge) {
        this.maxFetchAge = maxFetchAge;
    }
}
