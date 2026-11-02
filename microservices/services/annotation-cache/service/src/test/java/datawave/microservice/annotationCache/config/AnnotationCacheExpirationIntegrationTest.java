package datawave.microservice.annotationCache.config;

import static datawave.microservice.annotationCache.api.Constants.ANNOTATIONS_MAP;
import static datawave.microservice.annotationCache.api.Constants.FETCH_MAP;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
import datawave.microservice.annotationCache.api.AnnotationKey;
import datawave.microservice.annotationCache.api.FetchKey;
import datawave.microservice.annotationCache.api.FetchRecord;

/** Verifies configured annotation/fetch TTL behavior and annotation-expiry invalidation using a real Hazelcast member. */
class AnnotationCacheExpirationIntegrationTest {
    private static final AnnotationKey FIRST_ANNOTATION = new AnnotationKey("UUID", "document", "first");
    private static final AnnotationKey SECOND_ANNOTATION = new AnnotationKey("UUID", "document", "second");

    private HazelcastInstance hazelcastInstance;

    @AfterEach
    void tearDown() {
        if (hazelcastInstance != null) {
            hazelcastInstance.shutdown();
        }
    }

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

        IMap<AnnotationKey,AnnotationMessage> annotations = hazelcastInstance.getMap(ANNOTATIONS_MAP);
        IMap<FetchKey,FetchRecord> fetchRecords = hazelcastInstance.getMap(FETCH_MAP);

        annotations.put(FIRST_ANNOTATION, annotationMessage("first"), 30, TimeUnit.SECONDS);
        FetchKey defaultTtlMarker = fetchKey("UUID", "document", "default-ttl");
        fetchRecords.put(defaultTtlMarker, record());
        await("fetch record to expire using its configured TTL", () -> !fetchRecords.containsKey(defaultTtlMarker));
        assertNotNull(annotations.get(FIRST_ANNOTATION), "the longer-lived annotation should still be cached when the fetch record expires");

        // Expiration of an annotation invalidates every auth-context marker for only that idType/document pair.
        annotations.set(SECOND_ANNOTATION, annotationMessage("second"));
        FetchKey invalidatedMarker = fetchKey("UUID", "document", "listener-invalidation");
        FetchKey secondMarker = fetchKey("UUID", "document", "second-invalidation");
        FetchKey otherTypeMarker = fetchKey("PAGE_ID", "document", "other-type");
        fetchRecords.set(invalidatedMarker, record(), 30, TimeUnit.SECONDS);
        fetchRecords.set(secondMarker, record(), 30, TimeUnit.SECONDS);
        fetchRecords.set(otherTypeMarker, record(), 30, TimeUnit.SECONDS);
        await("second annotation to expire using its configured TTL", () -> !annotations.containsKey(SECOND_ANNOTATION));
        await("matching fetch records to be invalidated after annotation expiration",
                        () -> !fetchRecords.containsKey(invalidatedMarker) && !fetchRecords.containsKey(secondMarker));
        assertTrue(fetchRecords.containsKey(otherTypeMarker), "expiration under one id type must not invalidate another id type");

        assertNotNull(annotations.get(FIRST_ANNOTATION));
        assertNull(annotations.get(SECOND_ANNOTATION));
        assertTrue(fetchRecords.containsKey(otherTypeMarker));
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

    private FetchKey fetchKey(String idType, String documentId, String authHash) {
        return new FetchKey(idType, documentId, authHash);
    }

    private FetchRecord record() {
        return new FetchRecord(System.currentTimeMillis(), 1);
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
