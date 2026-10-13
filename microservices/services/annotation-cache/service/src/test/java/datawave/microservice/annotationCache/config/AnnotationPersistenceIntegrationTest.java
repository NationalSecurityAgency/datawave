package datawave.microservice.annotationCache.config;

import static datawave.microservice.annotationCache.api.Constants.ANNOTATIONS_MAP;
import static datawave.microservice.annotationCache.api.Constants.ID_TYPE_PARAMETER;
import static datawave.microservice.annotationCache.api.Constants.PERSISTENCE_MODE_PARAMETER;
import static datawave.microservice.annotationCache.api.Constants.REGION_ID_PARAMETER;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.messaging.Message;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.AnnotationMessage;
import datawave.microservice.annotationCache.AnnotationMapStore;
import datawave.microservice.annotationCache.AnnotationMessagePublisher;
import datawave.microservice.annotationCache.AnnotationSyncListener;
import datawave.microservice.annotationCache.api.AnnotationKey;
import datawave.microservice.annotationCache.api.AnnotationStorageException;
import datawave.microservice.annotationCache.api.PersistenceMode;
import datawave.microservice.annotationCache.api.RegionConfiguration;

/** Verifies persistence-mode filtering and write-through acceptance through real Hazelcast MapStore invocations. */
class AnnotationPersistenceIntegrationTest {
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
    void onlyLocalWriteThroughAnnotationsArePublished() {
        AnnotationMessagePublisher publisher = mock(AnnotationMessagePublisher.class);
        // @formatter:off
        when(publisher.send(any(Message.class))).thenAnswer(invocation -> {
            Message<?> message = invocation.getArgument(0);
            CorrelationData correlationData = (CorrelationData) message.getHeaders().get("amqp_publishConfirmCorrelation");
            correlationData.getFuture().set(new CorrelationData.Confirm(true, null));
            return true;
        });
        // @formatter:on

        IMap<AnnotationKey,AnnotationMessage> annotations = startMember(publisher, 0);
        // @formatter:off
        AnnotationMessage hydration = message("hydration", LOCAL_REGION, PersistenceMode.CACHE_ONLY);
        AnnotationMessage localWrite = message("local-write", LOCAL_REGION, PersistenceMode.WRITE_THROUGH);
        AnnotationMessage remoteWrite = message("remote-write", REMOTE_REGION, PersistenceMode.WRITE_THROUGH);
        // @formatter:on

        annotations.set(new AnnotationKey(ID_TYPE, DOCUMENT_ID, "hydration"), hydration);
        annotations.set(new AnnotationKey(ID_TYPE, DOCUMENT_ID, "local-write"), localWrite);
        annotations.set(new AnnotationKey(ID_TYPE, DOCUMENT_ID, "remote-write"), remoteWrite);

        assertEquals(hydration, annotations.get(new AnnotationKey(ID_TYPE, DOCUMENT_ID, "hydration")));
        assertEquals(localWrite, annotations.get(new AnnotationKey(ID_TYPE, DOCUMENT_ID, "local-write")));
        assertEquals(remoteWrite, annotations.get(new AnnotationKey(ID_TYPE, DOCUMENT_ID, "remote-write")));
        verify(publisher, times(1)).send(any(Message.class));
    }

    /** Control: synchronous MapStore publication failure must fail the map write and leave the annotation absent. */
    @Test
    void zeroWriteDelayRejectsLocalWriteWhenPublicationFails() {
        AnnotationMessagePublisher publisher = mock(AnnotationMessagePublisher.class);
        when(publisher.send(any(Message.class))).thenReturn(false);
        IMap<AnnotationKey,AnnotationMessage> annotations = startMember(publisher, 0);
        AnnotationKey key = new AnnotationKey(ID_TYPE, DOCUMENT_ID, "failed-local-write");
        AnnotationMessage value = message("failed-local-write", LOCAL_REGION, PersistenceMode.WRITE_THROUGH);

        assertThrows(AnnotationStorageException.class, () -> annotations.set(key, value));
        assertNull(annotations.get(key), "A failed publication must not leave an accepted annotation in the cache");
        verify(publisher, times(1)).send(any(Message.class));
    }

    /** Reproduces review finding #3: a configured delay must not let local write-through acceptance bypass publication. */
    @Test
    void positiveWriteDelayMustNotAcceptLocalWriteBeforePublication() {
        AnnotationMessagePublisher publisher = mock(AnnotationMessagePublisher.class);
        when(publisher.send(any(Message.class))).thenReturn(false);

        // A positive delay schedules MapStore publication for later. Exercise the immediate acceptance boundary without waiting for that delay.
        IMap<AnnotationKey,AnnotationMessage> annotations = startMember(publisher, 60);
        AnnotationKey key = new AnnotationKey(ID_TYPE, DOCUMENT_ID, "delayed-local-write");
        AnnotationMessage value = message("delayed-local-write", LOCAL_REGION, PersistenceMode.WRITE_THROUGH);
        try {
            // Collect all three observations: caller failure, cache contents, and whether publication was attempted.
            // At the reviewed implementation this fails because set() returns normally, the value is cached, and the publisher is never called.
            assertAll("Local write-through acceptance must honor publication failure even when a write delay is supplied",
                            () -> assertThrows(AnnotationStorageException.class, () -> annotations.set(key, value),
                                            () -> "Write returned without publication failure; retained write delay="
                                                            + hazelcastInstance.getConfig().findMapConfig(ANNOTATIONS_MAP).getMapStoreConfig()
                                                                            .getWriteDelaySeconds()),
                            () -> assertNull(annotations.get(key), "A failed publication must not leave an accepted annotation in the cache"),
                            () -> verify(publisher, times(1)).send(any(Message.class)));
        } finally {
            // Graceful shutdown can flush the queued write into the intentionally failing publisher. Terminate this disposable member instead.
            hazelcastInstance.getLifecycleService().terminate();
            hazelcastInstance = null;
        }
    }

    private IMap<AnnotationKey,AnnotationMessage> startMember(AnnotationMessagePublisher publisher, int writeDelaySeconds) {
        AnnotationCacheProperties properties = new AnnotationCacheProperties();
        properties.setMaxCacheAge(Duration.ofSeconds(30));
        properties.setMaxFetchAge(Duration.ofSeconds(10));
        AnnotationSyncListener listener = new AnnotationSyncListener();
        AnnotationMapStore mapStore = new AnnotationMapStore(publisher, region(LOCAL_REGION));
        Config config = isolatedConfig();

        // Supply an exact map configuration, as deployment XML could, before the real service configuration helper runs.
        config.getMapConfig(ANNOTATIONS_MAP).getMapStoreConfig().setWriteDelaySeconds(writeDelaySeconds);
        new AnnotationCacheConfiguration(properties).configureMaps(config, mapStore, listener);
        hazelcastInstance = Hazelcast.newHazelcastInstance(config);
        listener.setHazelcastInstance(hazelcastInstance);
        return hazelcastInstance.getMap(ANNOTATIONS_MAP);
    }

    private Config isolatedConfig() {
        Config config = new Config();
        config.setClusterName("annotation-persistence-test-" + UUID.randomUUID());
        config.setProperty("hazelcast.logging.type", "none");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getNetworkConfig().setPort(0).setPortAutoIncrement(true);
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getAutoDetectionConfig().setEnabled(false);
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        return config;
    }

    private AnnotationMessage message(String annotationId, String region, PersistenceMode persistenceMode) {
        Annotation annotation = Annotation.newBuilder().setDocumentId(DOCUMENT_ID).setAnnotationId(annotationId).build();
        // @formatter:off
        return AnnotationMessage.newBuilder()
                        .putParameters(REGION_ID_PARAMETER, region)
                        .putParameters(ID_TYPE_PARAMETER, ID_TYPE)
                        .putParameters(PERSISTENCE_MODE_PARAMETER, persistenceMode.value())
                        .addAnnotations(annotation)
                        .build();
        // @formatter:on
    }

    private RegionConfiguration region(String name) {
        RegionConfiguration region = new RegionConfiguration();
        region.setName(name);
        return region;
    }
}
