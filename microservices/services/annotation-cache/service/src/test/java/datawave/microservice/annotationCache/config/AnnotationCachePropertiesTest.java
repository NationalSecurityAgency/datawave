package datawave.microservice.annotationCache.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;

import org.junit.jupiter.api.Test;

class AnnotationCachePropertiesTest {

    @Test
    void federationLockWaitDefaultsToFiveSeconds() {
        AnnotationCacheProperties properties = new AnnotationCacheProperties();

        assertEquals(Duration.ofSeconds(5), properties.getFederationLockWait());
    }

    @Test
    void federationLockWaitCanRepresentConfiguredBoundaryValues() {
        AnnotationCacheProperties properties = new AnnotationCacheProperties();

        properties.setFederationLockWait(Duration.ofMillis(1));
        assertEquals(Duration.ofMillis(1), properties.getFederationLockWait());

        properties.setFederationLockWait(Duration.ofHours(1));
        assertEquals(Duration.ofHours(1), properties.getFederationLockWait());
    }

    @Test
    void federationLockWaitSetterPreservesInvalidValuesForConsumerValidation() {
        AnnotationCacheProperties properties = new AnnotationCacheProperties();

        properties.setFederationLockWait(Duration.ZERO);
        assertEquals(Duration.ZERO, properties.getFederationLockWait());

        properties.setFederationLockWait(Duration.ofNanos(-1));
        assertEquals(Duration.ofNanos(-1), properties.getFederationLockWait());
    }
}
