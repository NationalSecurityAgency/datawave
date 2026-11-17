package datawave.microservice.annotationCache.config;

import static datawave.microservice.annotationCache.api.Constants.ANNOTATIONS_MAP;
import static datawave.microservice.annotationCache.api.Constants.ID_TYPE_PARAMETER;
import static datawave.microservice.annotationCache.api.Constants.PERSISTENCE_MODE_PARAMETER;
import static datawave.microservice.annotationCache.api.Constants.REGION_ID_PARAMETER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
import datawave.microservice.annotationCache.api.AnnotationKey;
import datawave.microservice.annotationCache.api.AnnotationStorageException;
import datawave.microservice.annotationCache.api.PersistenceMode;
import datawave.microservice.annotationCache.api.RegionConfiguration;

/** Exercises local write-through and federated transient insertion against a real Hazelcast member. */
class FederatedCacheIntegrationTest {
    private static final String LOCAL_REGION = "local";
    private static final String REMOTE_REGION = "remote";
    private static final String ID_TYPE = "UUID";
    private static final String DOCUMENT_ID = "document";

    private HazelcastInstance hazelcastInstance;

    @AfterEach
    void tearDown() {
        if (hazelcastInstance != null) {
            hazelcastInstance.shutdown();
        }
    }

    @Test
    void federatedInsertionDoesNotRepublishToMapStore() throws InterruptedException {
        AnnotationCacheProperties properties = new AnnotationCacheProperties();
        properties.setMaxCacheAge(Duration.ofSeconds(30));
        properties.setMaxFetchAge(Duration.ofSeconds(10));

        AnnotationMapStore mapStore = mock(AnnotationMapStore.class);
        AnnotationSyncListener listener = new AnnotationSyncListener();
        Config config = isolatedConfig();
        new AnnotationCacheConfiguration(properties).configureMaps(config, mapStore, listener);
        hazelcastInstance = Hazelcast.newHazelcastInstance(config);
        listener.setHazelcastInstance(hazelcastInstance);

        IMap<AnnotationKey,AnnotationMessage> annotations = hazelcastInstance.getMap(ANNOTATIONS_MAP);

        AnnotationKey localKey = key(ID_TYPE, DOCUMENT_ID, "local-annotation");
        AnnotationMessage localMessage = message(LOCAL_REGION, ID_TYPE, DOCUMENT_ID, "local-annotation");
        annotations.set(localKey, localMessage);
        verify(mapStore).store(localKey, localMessage);
        await("local annotation to be cached", () -> localMessage.equals(annotations.get(localKey)));

        RegionConfiguration region = new RegionConfiguration();
        region.setName(LOCAL_REGION);
        Consumer<AnnotationMessage> consumer = new LoadCacheConsumer(hazelcastInstance, region, properties).loadCache();

        // The queue also receives locally produced messages. They must be ignored because the originating write is already in this cluster.
        consumer.accept(localMessage);
        verify(mapStore).store(localKey, localMessage);

        clearInvocations(mapStore);
        AnnotationKey remoteKey = key(ID_TYPE, DOCUMENT_ID, "remote-annotation");
        AnnotationMessage remoteMessage = message(REMOTE_REGION, ID_TYPE, DOCUMENT_ID, "remote-annotation");
        consumer.accept(remoteMessage);

        await("remote annotation to be cached", () -> remoteMessage.equals(annotations.get(remoteKey)));
        verify(mapStore, after(250).never()).store(eq(remoteKey), any());

        EntryView<AnnotationKey,AnnotationMessage> entryView = annotations.getEntryView(remoteKey);
        assertNotNull(entryView);
        long remainingTtl = entryView.getExpirationTime() - System.currentTimeMillis();
        assertTrue(remainingTtl > 0 && remainingTtl <= 30_000, "the federated entry should use the configured annotation TTL");

        // A duplicate delivery with the same immutable ID must leave the original value untouched and must not publish it locally.
        AnnotationMessage duplicate = message(REMOTE_REGION, ID_TYPE, DOCUMENT_ID, "remote-annotation").toBuilder().setSource("duplicate").build();
        consumer.accept(duplicate);
        assertEquals(remoteMessage, annotations.get(remoteKey));
        verify(mapStore, never()).store(eq(remoteKey), any());
    }

    @Test
    void federatedEntriesWithSameAnnotationIdRemainDistinctAcrossDocumentsAndIdentifierTypes() {
        AnnotationCacheProperties properties = new AnnotationCacheProperties();
        Config config = isolatedConfig();
        new AnnotationCacheConfiguration(properties).configureMaps(config, mock(AnnotationMapStore.class), new AnnotationSyncListener());
        hazelcastInstance = Hazelcast.newHazelcastInstance(config);

        RegionConfiguration region = new RegionConfiguration();
        region.setName(LOCAL_REGION);
        Consumer<AnnotationMessage> consumer = new LoadCacheConsumer(hazelcastInstance, region, properties).loadCache();
        IMap<AnnotationKey,AnnotationMessage> annotations = hazelcastInstance.getMap(ANNOTATIONS_MAP);

        consumer.accept(message(REMOTE_REGION, "UUID", "first-document", "same-id"));
        consumer.accept(message(REMOTE_REGION, "UUID", "second-document", "same-id"));
        consumer.accept(message(REMOTE_REGION, "PAGE_ID", "first-document", "same-id"));

        assertEquals(3, annotations.size());
        assertNotNull(annotations.get(key("UUID", "first-document", "same-id")));
        assertNotNull(annotations.get(key("UUID", "second-document", "same-id")));
        assertNotNull(annotations.get(key("PAGE_ID", "first-document", "same-id")));
    }

    @Test
    void federatedConsumerTimesOutWhenAnnotationLockIsContended() throws Exception {
        AnnotationCacheProperties properties = new AnnotationCacheProperties();
        properties.setMaxCacheAge(Duration.ofSeconds(30));
        properties.setMaxFetchAge(Duration.ofSeconds(10));
        properties.setFederationLockWait(Duration.ofMillis(150));

        AnnotationSyncListener listener = new AnnotationSyncListener();
        Config config = isolatedConfig();
        new AnnotationCacheConfiguration(properties).configureMaps(config, mock(AnnotationMapStore.class), listener);
        hazelcastInstance = Hazelcast.newHazelcastInstance(config);
        listener.setHazelcastInstance(hazelcastInstance);

        IMap<AnnotationKey,AnnotationMessage> annotations = hazelcastInstance.getMap(ANNOTATIONS_MAP);
        RegionConfiguration region = new RegionConfiguration();
        region.setName(LOCAL_REGION);
        Consumer<AnnotationMessage> consumer = new LoadCacheConsumer(hazelcastInstance, region, properties).loadCache();
        AnnotationKey contendedKey = key(ID_TYPE, DOCUMENT_ID, "contended");

        annotations.lock(contendedKey);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> attempt = executor.submit(() -> consumer.accept(message(REMOTE_REGION, ID_TYPE, DOCUMENT_ID, "contended")));
            ExecutionException failure = assertThrows(ExecutionException.class, () -> attempt.get(2, TimeUnit.SECONDS));
            assertTrue(failure.getCause() instanceof AnnotationStorageException);
            assertTrue(failure.getCause().getMessage().contains("Timed out acquiring Hazelcast lock"));
            assertTrue(annotations.get(contendedKey) == null);
        } finally {
            executor.shutdownNow();
            annotations.unlock(contendedKey);
        }
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

    private AnnotationKey key(String idType, String documentId, String annotationId) {
        return new AnnotationKey(idType, documentId, annotationId);
    }

    private AnnotationMessage message(String region, String idType, String documentId, String annotationId) {
        Annotation annotation = Annotation.newBuilder().setDocumentId(documentId).setAnnotationId(annotationId).build();
        return AnnotationMessage.newBuilder().putParameters(REGION_ID_PARAMETER, region).putParameters(ID_TYPE_PARAMETER, idType)
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
