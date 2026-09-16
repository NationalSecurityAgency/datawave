package datawave.microservice.annotationCache.config;

import static datawave.microservice.annotationCache.api.Constants.ANNOTATIONS_MAP;
import static datawave.microservice.annotationCache.api.Constants.DOC_ANNOTATIONS_MAP;
import static datawave.microservice.annotationCache.api.Constants.FETCH_MAP;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
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

/** Exercises explicit entry and map removal events against a real Hazelcast member. */
class AnnotationRemovalIntegrationTest {
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
    void removeEvictClearAndEvictAllSynchronizeDerivedMaps() throws InterruptedException {
        AnnotationCacheProperties properties = new AnnotationCacheProperties();
        properties.setMaxCacheAge(Duration.ofSeconds(30));
        properties.setMaxFetchAge(Duration.ofSeconds(10));

        AnnotationSyncListener listener = new AnnotationSyncListener();
        Config config = isolatedConfig();
        new AnnotationCacheConfiguration(properties).configureMaps(config, mock(AnnotationMapStore.class), listener);
        hazelcastInstance = Hazelcast.newHazelcastInstance(config);
        listener.setHazelcastInstance(hazelcastInstance);

        IMap<String,AnnotationMessage> annotations = hazelcastInstance.getMap(ANNOTATION_MAP);
        IMap<String,Set<String>> documentIndex = hazelcastInstance.getMap(DOC_ANNOTATIONS_MAP);
        IMap<String,String> fetchRecords = hazelcastInstance.getMap(FETCH_MAP + CACHE_KEY);

        annotations.set("first", annotationMessage("first"));
        annotations.set("second", annotationMessage("second"));
        await("initial annotations to be indexed", () -> Set.of("first", "second").equals(documentIndex.get(CACHE_KEY)));

        fetchRecords.set("remove", "record", 30, TimeUnit.SECONDS);
        assertNotNull(annotations.remove("first"));
        await("removed ID and fetch record to be synchronized", () -> Set.of("second").equals(documentIndex.get(CACHE_KEY)) && fetchRecords.isEmpty());
        assertNotNull(annotations.get("second"));

        fetchRecords.set("evict", "record", 30, TimeUnit.SECONDS);
        assertTrue(annotations.evict("second"));
        await("evicted final ID and fetch record to be synchronized", () -> documentIndex.get(CACHE_KEY) == null && fetchRecords.isEmpty());

        annotations.set("third", annotationMessage("third"));
        annotations.set("fourth", annotationMessage("fourth"));
        await("annotations to be indexed before clear", () -> Set.of("third", "fourth").equals(documentIndex.get(CACHE_KEY)));
        fetchRecords.set("clear", "record", 30, TimeUnit.SECONDS);
        annotations.clear();
        await("map clear to remove the index and fetch records", () -> documentIndex.get(CACHE_KEY) == null && fetchRecords.isEmpty());
        assertTrue(annotations.isEmpty());

        annotations.set("fifth", annotationMessage("fifth"));
        annotations.set("sixth", annotationMessage("sixth"));
        await("annotations to be indexed before evictAll", () -> Set.of("fifth", "sixth").equals(documentIndex.get(CACHE_KEY)));
        fetchRecords.set("evict-all", "record", 30, TimeUnit.SECONDS);
        annotations.evictAll();
        await("map eviction to remove the index and fetch records", () -> documentIndex.get(CACHE_KEY) == null && fetchRecords.isEmpty());

        assertTrue(annotations.isEmpty());
        assertNull(documentIndex.get(CACHE_KEY));
        assertTrue(fetchRecords.isEmpty());
    }

    private Config isolatedConfig() {
        Config config = new Config();
        config.setClusterName("annotation-removal-test-" + UUID.randomUUID());
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
