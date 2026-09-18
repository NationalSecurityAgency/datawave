package datawave.microservice.annotationCache.config;

import static datawave.microservice.annotationCache.api.Constants.ANNOTATIONS_MAP;
import static datawave.microservice.annotationCache.api.Constants.DOC_ANNOTATIONS_MAP;
import static datawave.microservice.annotationCache.api.Constants.ID_TYPE_PARAMETER;
import static datawave.microservice.annotationCache.api.Constants.PERSISTENCE_MODE_PARAMETER;
import static datawave.microservice.annotationCache.api.Constants.REGION_ID_PARAMETER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.AnnotationMessage;
import datawave.microservice.annotationCache.AnnotationMapStore;
import datawave.microservice.annotationCache.AnnotationSyncListener;
import datawave.microservice.annotationCache.LoadCacheConsumer;
import datawave.microservice.annotationCache.api.PersistenceMode;
import datawave.microservice.annotationCache.api.RegionConfiguration;

/** Exercises local write-through and federated transient insertion against a real Hazelcast member. */
class FederatedCacheIntegrationTest {
    private static final String LOCAL_REGION = "local";
    private static final String REMOTE_REGION = "remote";
    private static final String ID_TYPE = "UUID";
    private static final String DOCUMENT_ID = "document";
    private static final String CACHE_KEY = ID_TYPE + ":" + DOCUMENT_ID;
    private static final String ANNOTATION_MAP = ANNOTATIONS_MAP + CACHE_KEY;

    private HazelcastInstance hazelcastInstance;

    @AfterEach
    void tearDown() {
        if (hazelcastInstance != null) {
            hazelcastInstance.shutdown();
        }
    }

    @Test
    void federatedInsertionUpdatesIndexWithoutRepublishingToMapStore() throws InterruptedException {
        AnnotationCacheProperties properties = new AnnotationCacheProperties();
        properties.setMaxCacheAge(Duration.ofSeconds(30));
        properties.setMaxFetchAge(Duration.ofSeconds(10));

        AnnotationMapStore mapStore = mock(AnnotationMapStore.class);
        AnnotationSyncListener listener = new AnnotationSyncListener();
        Config config = isolatedConfig();
        new AnnotationCacheConfiguration(properties).configureMaps(config, mapStore, listener);
        hazelcastInstance = Hazelcast.newHazelcastInstance(config);
        listener.setHazelcastInstance(hazelcastInstance);

        IMap<String,AnnotationMessage> annotations = hazelcastInstance.getMap(ANNOTATION_MAP);
        IMap<String,Set<String>> documentIndex = hazelcastInstance.getMap(DOC_ANNOTATIONS_MAP);

        AnnotationMessage localMessage = message(LOCAL_REGION, "local-annotation");
        annotations.set("local-annotation", localMessage);
        verify(mapStore).store("local-annotation", localMessage);
        await("local annotation to be indexed", () -> Set.of("local-annotation").equals(documentIndex.get(CACHE_KEY)));

        RegionConfiguration region = new RegionConfiguration();
        region.setName(LOCAL_REGION);
        Consumer<AnnotationMessage> consumer = new LoadCacheConsumer(hazelcastInstance, region).loadCache();

        // The queue also receives locally produced messages. They must be ignored because the originating write is already in this cluster.
        consumer.accept(localMessage);
        verify(mapStore).store("local-annotation", localMessage);

        clearInvocations(mapStore);
        AnnotationMessage remoteMessage = message(REMOTE_REGION, "remote-annotation");
        consumer.accept(remoteMessage);

        await("remote annotation to be cached and indexed", () -> remoteMessage.equals(annotations.get("remote-annotation"))
                        && Set.of("local-annotation", "remote-annotation").equals(documentIndex.get(CACHE_KEY)));
        verify(mapStore, after(250).never()).store(eq("remote-annotation"), any());

        EntryView<String,AnnotationMessage> entryView = annotations.getEntryView("remote-annotation");
        assertNotNull(entryView);
        long remainingTtl = entryView.getExpirationTime() - System.currentTimeMillis();
        assertTrue(remainingTtl > 0 && remainingTtl <= 30_000, "the federated entry should use the configured annotation TTL");

        // A duplicate delivery with the same immutable ID must leave the original value untouched and must not publish it locally.
        AnnotationMessage duplicate = message(REMOTE_REGION, "remote-annotation").toBuilder().setSource("duplicate").build();
        consumer.accept(duplicate);
        assertEquals(remoteMessage, annotations.get("remote-annotation"));
        verify(mapStore, never()).store(eq("remote-annotation"), any());
    }

    private Config isolatedConfig() {
        Config config = new Config();
        config.setClusterName("federated-cache-test-" + UUID.randomUUID());
        config.setProperty("hazelcast.logging.type", "none");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getNetworkConfig().setPort(0).setPortAutoIncrement(true);
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getAutoDetectionConfig().setEnabled(false);
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        return config;
    }

    private AnnotationMessage message(String region, String annotationId) {
        Annotation annotation = Annotation.newBuilder().setDocumentId(DOCUMENT_ID).setAnnotationId(annotationId).build();
        return AnnotationMessage.newBuilder().putParameters(REGION_ID_PARAMETER, region).putParameters(ID_TYPE_PARAMETER, ID_TYPE)
                        .putParameters(PERSISTENCE_MODE_PARAMETER, PersistenceMode.WRITE_THROUGH.value()).addAnnotations(annotation).build();
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
