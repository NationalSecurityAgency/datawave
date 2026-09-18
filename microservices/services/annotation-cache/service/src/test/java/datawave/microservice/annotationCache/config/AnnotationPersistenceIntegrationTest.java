package datawave.microservice.annotationCache.config;

import static datawave.microservice.annotationCache.api.Constants.ANNOTATIONS_MAP;
import static datawave.microservice.annotationCache.api.Constants.ID_TYPE_PARAMETER;
import static datawave.microservice.annotationCache.api.Constants.PERSISTENCE_MODE_PARAMETER;
import static datawave.microservice.annotationCache.api.Constants.REGION_ID_PARAMETER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

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
import datawave.microservice.annotationCache.api.PersistenceMode;
import datawave.microservice.annotationCache.api.RegionConfiguration;

/** Verifies persistence-mode and region filtering through a real Hazelcast MapStore invocation. */
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
        org.mockito.Mockito.when(publisher.send(any(Message.class))).thenAnswer(invocation -> {
            Message<?> message = invocation.getArgument(0);
            CorrelationData correlationData = (CorrelationData) message.getHeaders().get("amqp_publishConfirmCorrelation");
            correlationData.getFuture().set(new CorrelationData.Confirm(true, null));
            return true;
        });
        // @formatter:on

        AnnotationCacheProperties properties = new AnnotationCacheProperties();
        properties.setMaxCacheAge(Duration.ofSeconds(30));
        properties.setMaxFetchAge(Duration.ofSeconds(10));
        AnnotationSyncListener listener = new AnnotationSyncListener();
        AnnotationMapStore mapStore = new AnnotationMapStore(publisher, region(LOCAL_REGION));
        Config config = isolatedConfig();
        new AnnotationCacheConfiguration(properties).configureMaps(config, mapStore, listener);
        hazelcastInstance = Hazelcast.newHazelcastInstance(config);
        listener.setHazelcastInstance(hazelcastInstance);

        IMap<String,AnnotationMessage> annotations = hazelcastInstance.getMap(ANNOTATIONS_MAP + ID_TYPE + ":" + DOCUMENT_ID);
        // @formatter:off
        AnnotationMessage hydration = message("hydration", LOCAL_REGION, PersistenceMode.CACHE_ONLY);
        AnnotationMessage localWrite = message("local-write", LOCAL_REGION, PersistenceMode.WRITE_THROUGH);
        AnnotationMessage remoteWrite = message("remote-write", REMOTE_REGION, PersistenceMode.WRITE_THROUGH);
        // @formatter:on

        annotations.set("hydration", hydration);
        annotations.set("local-write", localWrite);
        annotations.set("remote-write", remoteWrite);

        assertEquals(hydration, annotations.get("hydration"));
        assertEquals(localWrite, annotations.get("local-write"));
        assertEquals(remoteWrite, annotations.get("remote-write"));
        verify(publisher, times(1)).send(any(Message.class));
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
