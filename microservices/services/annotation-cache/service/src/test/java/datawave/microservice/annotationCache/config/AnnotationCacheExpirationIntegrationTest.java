package datawave.microservice.annotationCache.config;

import static datawave.microservice.annotationCache.api.Constants.ANNOTATIONS_MAP;
import static datawave.microservice.annotationCache.api.Constants.FETCH_MAP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.AnnotationMessage;
import datawave.microservice.annotationCache.AnnotationMapStore;
import datawave.microservice.annotationCache.AnnotationSyncListener;

/**
 * Verifies configured annotation/fetch TTL behavior and single-member annotation-expiry invalidation. Cross-member listener propagation is tested separately.
 */
class AnnotationCacheExpirationIntegrationTest {
    private static final String CACHE_KEY = "UUID:document";
    private static final String ANNOTATION_MAP = ANNOTATIONS_MAP + CACHE_KEY;

    private HazelcastInstance hazelcastInstance;

    @AfterEach
    void tearDown() {
        if (hazelcastInstance != null) {
            hazelcastInstance.shutdown();
        }
    }

    /**
     * Confirms fetch records expire before annotations, then verifies annotation expiry invalidates a deliberately longer-lived fetch record. This test focuses
     * on TTL policy and local listener behavior; it does not test multi-member propagation.
     */
    @Test
    void annotationAndFetchExpirationRemainConsistent() throws InterruptedException {
        AnnotationCacheProperties properties = new AnnotationCacheProperties();
        properties.setMaxCacheAge(Duration.ofSeconds(5));
        properties.setMaxFetchAge(Duration.ofSeconds(2));

        AnnotationSyncListener listener = new AnnotationSyncListener();
        Config config = isolatedConfig();
        new AnnotationCacheConfiguration(properties).configureMaps(config, mock(AnnotationMapStore.class), listener);
        hazelcastInstance = Hazelcast.newHazelcastInstance(config);
        listener.setHazelcastInstance(hazelcastInstance);

        IMap<String,AnnotationMessage> annotations = hazelcastInstance.getMap(ANNOTATION_MAP);
        IMap<String,String> fetchRecords = hazelcastInstance.getMap(FETCH_MAP + CACHE_KEY);

        annotations.set("first", annotationMessage("first"));
        fetchRecords.set("default-ttl", "record");
        await("fetch record to expire using its configured TTL", () -> fetchRecords.get("default-ttl") == null);
        assertNotNull(annotations.get("first"), "the longer-lived annotation should still be cached when the fetch record expires");

        // Expiration of an annotation invalidates document freshness state.
        fetchRecords.set("listener-invalidation", "record", 30, TimeUnit.SECONDS);
        await("first annotation to expire", () -> annotations.get("first") == null);
        await("fetch records to be invalidated after annotation expiration", fetchRecords::isEmpty);

        annotations.set("second", annotationMessage("second"));
        fetchRecords.set("second-invalidation", "record", 30, TimeUnit.SECONDS);
        await("second annotation to expire", () -> annotations.get("second") == null);
        await("fetch records to be invalidated after second annotation expiration", fetchRecords::isEmpty);

        assertNull(annotations.get("first"));
        assertNull(annotations.get("second"));
        assertEquals(0, fetchRecords.size());
    }

    private Config isolatedConfig() {
        Config config = new Config();
        config.setClusterName("annotation-cache-test-" + UUID.randomUUID());
        config.setProperty("hazelcast.logging.type", "none");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getNetworkConfig().setPort(0).setPortAutoIncrement(true);
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getAutoDetectionConfig().setEnabled(false);
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        return config;
    }

    private AnnotationMessage annotationMessage(String annotationId) {
        Annotation annotation = Annotation.newBuilder().setDocumentId("document").setAnnotationId(annotationId).build();
        return AnnotationMessage.newBuilder().addAnnotations(annotation).build();
    }

    private void await(String description, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(25);
        }
        throw new AssertionError("Timed out waiting for " + description);
    }
}
