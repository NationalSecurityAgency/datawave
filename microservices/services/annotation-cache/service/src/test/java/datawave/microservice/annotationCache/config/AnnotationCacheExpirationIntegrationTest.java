package datawave.microservice.annotationCache.config;

import static datawave.microservice.annotationCache.api.Constants.ANNOTATIONS_MAP;
import static datawave.microservice.annotationCache.api.Constants.DOC_ANNOTATIONS_MAP;
import static datawave.microservice.annotationCache.api.Constants.FETCH_MAP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;

import java.time.Duration;
import java.util.Set;
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

/** Exercises expiration and listener synchronization against a real embedded Hazelcast member. */
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

    @Test
    void expirationKeepsAnnotationIndexAndFetchRecordsConsistent() throws InterruptedException {
        AnnotationCacheProperties properties = new AnnotationCacheProperties();
        properties.setMaxCacheAge(Duration.ofSeconds(2));
        properties.setMaxFetchAge(Duration.ofSeconds(1));

        AnnotationSyncListener listener = new AnnotationSyncListener();
        Config config = isolatedConfig();
        new AnnotationCacheConfiguration(properties).configureMaps(config, mock(AnnotationMapStore.class), listener);
        hazelcastInstance = Hazelcast.newHazelcastInstance(config);
        listener.setHazelcastInstance(hazelcastInstance);

        IMap<String,AnnotationMessage> annotations = hazelcastInstance.getMap(ANNOTATION_MAP);
        IMap<String,Set<String>> documentIndex = hazelcastInstance.getMap(DOC_ANNOTATIONS_MAP);
        IMap<String,String> fetchRecords = hazelcastInstance.getMap(FETCH_MAP + CACHE_KEY);

        annotations.set("first", annotationMessage("first"));
        fetchRecords.set("default-ttl", "record");

        await("first annotation to be indexed", () -> Set.of("first").equals(documentIndex.get(CACHE_KEY)));
        await("fetch record to expire using its configured TTL", () -> fetchRecords.get("default-ttl") == null);
        assertNotNull(annotations.get("first"), "the longer-lived annotation should still be cached when the fetch record expires");

        // Stagger the insertions to prove that expiration removes one ID without deleting a still-live annotation from the document index.
        annotations.set("second", annotationMessage("second"));
        await("second annotation to be indexed", () -> Set.of("first", "second").equals(documentIndex.get(CACHE_KEY)));

        // This record deliberately outlives the annotation TTL; expiration of the first annotation must clear it through the listener.
        fetchRecords.set("listener-invalidation", "record", 30, TimeUnit.SECONDS);
        await("first annotation to expire", () -> annotations.get("first") == null);
        await("first ID to be removed and fetch records invalidated", () -> Set.of("second").equals(documentIndex.get(CACHE_KEY)) && fetchRecords.isEmpty());
        assertNotNull(annotations.get("second"), "expiration of one annotation must not remove another live annotation");

        await("second annotation to expire", () -> annotations.get("second") == null);
        await("empty document index to be removed", () -> documentIndex.get(CACHE_KEY) == null);

        assertNull(annotations.get("first"));
        assertNull(annotations.get("second"));
        assertNull(documentIndex.get(CACHE_KEY));
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
