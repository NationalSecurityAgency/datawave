package datawave.microservice.annotationCache;

import static datawave.microservice.annotationCache.api.Constants.PERSISTENCE_MODE_PARAMETER;
import static datawave.microservice.annotationCache.api.Constants.REGION_ID_PARAMETER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.messaging.Message;

import datawave.annotation.protobuf.v1.AnnotationMessage;
import datawave.microservice.annotationCache.api.AnnotationStorageException;
import datawave.microservice.annotationCache.api.PersistenceMode;
import datawave.microservice.annotationCache.api.RegionConfiguration;

class AnnotationMapStoreTest {
    private static final String LOCAL_REGION = "local";
    private static final String REMOTE_REGION = "remote";

    private AnnotationMessagePublisher publisher;
    private AnnotationMapStore mapStore;

    @BeforeEach
    void setUp() {
        publisher = mock(AnnotationMessagePublisher.class);
        mapStore = new AnnotationMapStore(publisher, region(LOCAL_REGION));
    }

    @AfterEach
    void clearInterrupt() {
        Thread.interrupted();
    }

    @Test
    void constructorRequiresConfiguredRegion() {
        assertThrows(IllegalStateException.class, () -> new AnnotationMapStore(publisher, null));
        assertThrows(IllegalStateException.class, () -> new AnnotationMapStore(publisher, new RegionConfiguration()));
        assertThrows(IllegalStateException.class, () -> new AnnotationMapStore(publisher, region("  ")));
    }

    @Test
    void ignoresNonAnnotationValuesIncludingNull() {
        mapStore.store("null", null);
        mapStore.store("string", "value");

        verify(publisher, never()).send(any());
    }

    @Test
    void cacheOnlyAnnotationIsNotPublished() {
        mapStore.store("annotation", message(LOCAL_REGION, PersistenceMode.CACHE_ONLY));

        verify(publisher, never()).send(any());
    }

    @Test
    void writeThroughAnnotationFromAnotherRegionIsNotPublished() {
        mapStore.store("annotation", message(REMOTE_REGION, PersistenceMode.WRITE_THROUGH));

        verify(publisher, never()).send(any());
    }

    @Test
    void localWriteThroughAnnotationIsPublishedAndConfirmed() {
        confirmPublication(true, null);
        AnnotationMessage annotationMessage = message(LOCAL_REGION, PersistenceMode.WRITE_THROUGH);

        mapStore.store("annotation", annotationMessage);

        @SuppressWarnings("rawtypes")
        ArgumentCaptor<Message> messageCaptor = ArgumentCaptor.forClass(Message.class);
        verify(publisher).send(messageCaptor.capture());
        assertSame(annotationMessage, messageCaptor.getValue().getPayload());
        assertTrue(messageCaptor.getValue().getHeaders().get("amqp_publishConfirmCorrelation") instanceof CorrelationData);
    }

    @Test
    void missingPersistenceModeDefaultsToWriteThroughForCompatibility() {
        confirmPublication(true, null);
        // @formatter:off
        AnnotationMessage annotationMessage = AnnotationMessage.newBuilder()
                        .putParameters(REGION_ID_PARAMETER, LOCAL_REGION)
                        .build();
        // @formatter:on

        mapStore.store("annotation", annotationMessage);

        verify(publisher).send(any(Message.class));
    }

    @Test
    void missingRegionDefaultsToLocalForCompatibility() {
        confirmPublication(true, null);
        // @formatter:off
        AnnotationMessage annotationMessage = AnnotationMessage.newBuilder()
                        .putParameters(PERSISTENCE_MODE_PARAMETER, PersistenceMode.WRITE_THROUGH.value())
                        .build();
        // @formatter:on

        mapStore.store("annotation", annotationMessage);

        verify(publisher).send(any(Message.class));
    }

    @Test
    void rejectsUnknownPersistenceMode() {
        // @formatter:off
        AnnotationMessage annotationMessage = AnnotationMessage.newBuilder()
                        .putParameters(REGION_ID_PARAMETER, LOCAL_REGION)
                        .putParameters(PERSISTENCE_MODE_PARAMETER, "unknown")
                        .build();
        // @formatter:on

        AnnotationStorageException exception = assertThrows(AnnotationStorageException.class, () -> mapStore.store("annotation", annotationMessage));
        assertTrue(exception.getMessage().contains("unknown"));
        verify(publisher, never()).send(any());
    }

    @Test
    void failedStreamBridgeHandoffFailsTheWrite() {
        when(publisher.send(any(Message.class))).thenReturn(false);

        assertThrows(AnnotationStorageException.class, () -> mapStore.store("annotation", message(LOCAL_REGION, PersistenceMode.WRITE_THROUGH)));
    }

    @Test
    void brokerNackFailsTheWrite() {
        confirmPublication(false, "rejected");

        AnnotationStorageException exception = assertThrows(AnnotationStorageException.class,
                        () -> mapStore.store("annotation", message(LOCAL_REGION, PersistenceMode.WRITE_THROUGH)));
        assertTrue(exception.getMessage().contains("NACK"));
    }

    @Test
    void interruptedConfirmationRestoresInterruptStatus() {
        when(publisher.send(any(Message.class))).thenAnswer(invocation -> {
            Thread.currentThread().interrupt();
            return true;
        });

        assertThrows(AnnotationStorageException.class, () -> mapStore.store("annotation", message(LOCAL_REGION, PersistenceMode.WRITE_THROUGH)));
        assertTrue(Thread.currentThread().isInterrupted());
    }

    @Test
    void storeAllAppliesPersistencePolicyToEachEntry() {
        confirmPublication(true, null);

        // @formatter:off
        mapStore.storeAll(Map.of(
                        "write", message(LOCAL_REGION, PersistenceMode.WRITE_THROUGH),
                        "write-two", message(LOCAL_REGION, PersistenceMode.WRITE_THROUGH),
                        "hydrate", message(LOCAL_REGION, PersistenceMode.CACHE_ONLY),
                        "remote", message(REMOTE_REGION, PersistenceMode.WRITE_THROUGH)));
        // @formatter:on

        @SuppressWarnings("rawtypes")
        ArgumentCaptor<Message> messageCaptor = ArgumentCaptor.forClass(Message.class);
        verify(publisher, times(2)).send(messageCaptor.capture());
        assertEquals(2, messageCaptor.getAllValues().size());
        // @formatter:off
        assertTrue(messageCaptor.getAllValues().stream().allMatch(message -> {
            AnnotationMessage payload = (AnnotationMessage) message.getPayload();
            return PersistenceMode.WRITE_THROUGH.value().equals(payload.getParametersOrThrow(PERSISTENCE_MODE_PARAMETER))
                            && LOCAL_REGION.equals(payload.getParametersOrThrow(REGION_ID_PARAMETER));
        }));
        // @formatter:on
    }

    private void confirmPublication(boolean ack, String reason) {
        when(publisher.send(any(Message.class))).thenAnswer(invocation -> {
            Message<?> message = invocation.getArgument(0);
            CorrelationData correlationData = (CorrelationData) message.getHeaders().get("amqp_publishConfirmCorrelation");
            correlationData.getFuture().set(new CorrelationData.Confirm(ack, reason));
            return true;
        });
    }

    private AnnotationMessage message(String sourceRegion, PersistenceMode persistenceMode) {
        return AnnotationMessage.newBuilder().putParameters(REGION_ID_PARAMETER, sourceRegion)
                        .putParameters(PERSISTENCE_MODE_PARAMETER, persistenceMode.value()).build();
    }

    private RegionConfiguration region(String name) {
        RegionConfiguration region = new RegionConfiguration();
        region.setName(name);
        return region;
    }
}
