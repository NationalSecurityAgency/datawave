package datawave.microservice.annotationCache;

import static datawave.microservice.annotationCache.api.Constants.ANNOTATIONS_MAP;
import static datawave.microservice.annotationCache.api.Constants.DOC_ANNOTATIONS_MAP;
import static datawave.microservice.annotationCache.api.Constants.ID_TYPE_PARAMETER;
import static datawave.microservice.annotationCache.api.Constants.PERSISTENCE_MODE_PARAMETER;
import static datawave.microservice.annotationCache.api.Constants.REGION_ID_PARAMETER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.AnnotationMessage;
import datawave.microservice.annotationCache.api.AnnotationStorageException;
import datawave.microservice.annotationCache.api.PersistenceMode;
import datawave.microservice.annotationCache.api.RegionConfiguration;
import datawave.microservice.annotationCache.config.AnnotationCacheProperties;

class LoadCacheConsumerTest {
    private static final String LOCAL_REGION = "local";
    private static final String REMOTE_REGION = "remote";
    private static final String ID_TYPE = "UUID";

    private HazelcastInstance hazelcastInstance;
    private Config config;
    private RegionConfiguration regionConfiguration;
    private AnnotationCacheProperties properties;
    private IMap<String,Set<String>> documentIndex;
    private Consumer<AnnotationMessage> consumer;

    @BeforeEach
    void setUp() {
        hazelcastInstance = mock(HazelcastInstance.class);
        config = mock(Config.class);
        when(hazelcastInstance.getConfig()).thenReturn(config);
        documentIndex = mock(IMap.class);
        when(hazelcastInstance.<String,Set<String>> getMap(DOC_ANNOTATIONS_MAP)).thenReturn(documentIndex);

        regionConfiguration = new RegionConfiguration();
        regionConfiguration.setName(LOCAL_REGION);
        properties = new AnnotationCacheProperties();
        consumer = new LoadCacheConsumer(hazelcastInstance, regionConfiguration, properties).loadCache();
    }

    @Test
    void constructorRequiresRegionConfiguration() {
        assertThrows(IllegalStateException.class, () -> new LoadCacheConsumer(hazelcastInstance, null, properties));

        RegionConfiguration missingName = new RegionConfiguration();
        assertThrows(IllegalStateException.class, () -> new LoadCacheConsumer(hazelcastInstance, missingName, properties));

        missingName.setName("  ");
        assertThrows(IllegalStateException.class, () -> new LoadCacheConsumer(hazelcastInstance, missingName, properties));
    }

    @Test
    void federationLockWaitDefaultsToFiveSeconds() throws Exception {
        String mapName = ANNOTATIONS_MAP + ID_TYPE + ":doc";
        IMap<String,AnnotationMessage> annotationMap = annotationMap(mapName, 120, 30);

        consumer.accept(message(REMOTE_REGION, ID_TYPE, annotation("doc", "annotation")));

        verify(annotationMap).tryLock("annotation", 5000, TimeUnit.MILLISECONDS);
    }

    @Test
    void federationLockWaitRejectsInvalidDurations() {
        java.time.Duration[] invalidValues = {null, java.time.Duration.ZERO, java.time.Duration.ofNanos(-1), java.time.Duration.ofNanos(999_999)};
        for (java.time.Duration invalidValue : invalidValues) {
            AnnotationCacheProperties properties = new AnnotationCacheProperties();
            properties.setFederationLockWait(invalidValue);
            assertThrows(IllegalStateException.class, () -> new LoadCacheConsumer(hazelcastInstance, regionConfiguration, properties));
        }
    }

    @Test
    void federationLockWaitAcceptsOneMillisecond() throws Exception {
        AnnotationCacheProperties properties = new AnnotationCacheProperties();
        properties.setFederationLockWait(java.time.Duration.ofMillis(1));
        consumer = new LoadCacheConsumer(hazelcastInstance, regionConfiguration, properties).loadCache();
        String mapName = ANNOTATIONS_MAP + ID_TYPE + ":doc";
        IMap<String,AnnotationMessage> annotationMap = annotationMap(mapName, 120, 30);

        consumer.accept(message(REMOTE_REGION, ID_TYPE, annotation("doc", "annotation")));

        verify(annotationMap).tryLock("annotation", 1, TimeUnit.MILLISECONDS);
    }

    @Test
    void ignoresMessagesProducedInLocalRegion() {
        consumer.accept(message(LOCAL_REGION, ID_TYPE, annotation("doc", "annotation")));

        verify(hazelcastInstance, never()).getMap(any(String.class));
    }

    @Test
    void rejectsMessageWithoutSourceRegion() {
        // @formatter:off
        AnnotationMessage message = AnnotationMessage.newBuilder()
                        .putParameters(ID_TYPE_PARAMETER, ID_TYPE)
                        .addAnnotations(annotation("doc", "annotation"))
                        .build();
        // @formatter:on

        assertThrows(IllegalArgumentException.class, () -> consumer.accept(message));
        verify(hazelcastInstance, never()).getMap(any(String.class));
    }

    @Test
    void rejectsRemoteMessageWithoutIdentifierType() {
        // @formatter:off
        AnnotationMessage message = AnnotationMessage.newBuilder()
                        .putParameters(REGION_ID_PARAMETER, REMOTE_REGION)
                        .addAnnotations(annotation("doc", "annotation"))
                        .build();
        // @formatter:on

        assertThrows(IllegalArgumentException.class, () -> consumer.accept(message));
        verify(hazelcastInstance, never()).getMap(any(String.class));
    }

    @Test
    void acceptsRemoteMessageWithNoAnnotationsAsNoOp() {
        consumer.accept(message(REMOTE_REGION, ID_TYPE));

        verify(hazelcastInstance, never()).getMap(any(String.class));
    }

    @Test
    void rejectsAnnotationWithoutDocumentId() {
        Annotation annotation = annotation("", "annotation");

        assertThrows(IllegalArgumentException.class, () -> consumer.accept(message(REMOTE_REGION, ID_TYPE, annotation)));
        verify(hazelcastInstance, never()).getMap(any(String.class));
    }

    @Test
    void rejectsAnnotationWithoutAnnotationId() {
        Annotation annotation = annotation("doc", "");

        assertThrows(IllegalArgumentException.class, () -> consumer.accept(message(REMOTE_REGION, ID_TYPE, annotation)));
        verify(hazelcastInstance, never()).getMap(any(String.class));
    }

    @Test
    void storesRemoteAnnotationInDocumentMapWithoutInvokingMapStore() throws Exception {
        String mapName = ANNOTATIONS_MAP + ID_TYPE + ":doc";
        IMap<String,AnnotationMessage> annotationMap = annotationMap(mapName, 120, 30);
        AnnotationMessage message = message(REMOTE_REGION, ID_TYPE, annotation("doc", "annotation"));

        consumer.accept(message);

        verify(annotationMap).tryLock("annotation", 5000, TimeUnit.MILLISECONDS);
        verify(annotationMap).putTransient("annotation", message, 120, TimeUnit.SECONDS, 30, TimeUnit.SECONDS);
        verify(annotationMap).unlock("annotation");
        verify(annotationMap, never()).put(any(), any());
        verify(annotationMap, never()).set(any(), any());
        verify(documentIndex).executeOnKey(eq(ID_TYPE + ":doc"), any());
    }

    @Test
    void leavesExistingImmutableAnnotationUntouched() throws Exception {
        String mapName = ANNOTATIONS_MAP + ID_TYPE + ":doc";
        IMap<String,AnnotationMessage> annotationMap = annotationMap(mapName, 120, 30);
        when(annotationMap.containsKey("annotation")).thenReturn(true);

        consumer.accept(message(REMOTE_REGION, ID_TYPE, annotation("doc", "annotation")));

        verify(annotationMap).tryLock("annotation", 5000, TimeUnit.MILLISECONDS);
        verify(annotationMap).unlock("annotation");
        verify(documentIndex).executeOnKey(eq(ID_TYPE + ":doc"), any());
        verifyNoInteractions(config);
    }

    @Test
    void normalizesBatchedMessagesToOneAnnotationPerEntry() throws Exception {
        String mapName = ANNOTATIONS_MAP + ID_TYPE + ":doc";
        IMap<String,AnnotationMessage> annotationMap = annotationMap(mapName, 0, 0);
        Annotation first = annotation("doc", "first");
        Annotation second = annotation("doc", "second");
        consumer.accept(message(REMOTE_REGION, ID_TYPE, first, second));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<AnnotationMessage> valueCaptor = ArgumentCaptor.forClass(AnnotationMessage.class);
        // @formatter:off
        verify(annotationMap).putTransient(
                        eq("first"), valueCaptor.capture(), eq(0L), eq(TimeUnit.SECONDS), eq(0L), eq(TimeUnit.SECONDS));
        verify(annotationMap).putTransient(
                        eq("second"), valueCaptor.capture(), eq(0L), eq(TimeUnit.SECONDS), eq(0L), eq(TimeUnit.SECONDS));
        // @formatter:on

        assertEquals(1, valueCaptor.getAllValues().get(0).getAnnotationsCount());
        assertEquals(first, valueCaptor.getAllValues().get(0).getAnnotations(0));
        assertEquals(REMOTE_REGION, valueCaptor.getAllValues().get(0).getParametersOrThrow(REGION_ID_PARAMETER));
        assertEquals(PersistenceMode.WRITE_THROUGH.value(), valueCaptor.getAllValues().get(0).getParametersOrThrow(PERSISTENCE_MODE_PARAMETER));
        assertEquals(1, valueCaptor.getAllValues().get(1).getAnnotationsCount());
        assertEquals(second, valueCaptor.getAllValues().get(1).getAnnotations(0));
    }

    @Test
    void usesEachAnnotationsDocumentMapForBatchedMessages() throws Exception {
        IMap<String,AnnotationMessage> firstMap = annotationMap(ANNOTATIONS_MAP + ID_TYPE + ":first-doc", 0, 0);
        IMap<String,AnnotationMessage> secondMap = annotationMap(ANNOTATIONS_MAP + ID_TYPE + ":second-doc", 0, 0);

        consumer.accept(message(REMOTE_REGION, ID_TYPE, annotation("first-doc", "first"), annotation("second-doc", "second")));

        verify(firstMap).putTransient(eq("first"), any(AnnotationMessage.class), eq(0L), eq(TimeUnit.SECONDS), eq(0L), eq(TimeUnit.SECONDS));
        verify(secondMap).putTransient(eq("second"), any(AnnotationMessage.class), eq(0L), eq(TimeUnit.SECONDS), eq(0L), eq(TimeUnit.SECONDS));
        verify(documentIndex).executeOnKey(eq(ID_TYPE + ":first-doc"), any());
        verify(documentIndex).executeOnKey(eq(ID_TYPE + ":second-doc"), any());
    }

    @Test
    void retriesWhenDocumentIndexUpdateFails() throws Exception {
        String mapName = ANNOTATIONS_MAP + ID_TYPE + ":doc";
        IMap<String,AnnotationMessage> annotationMap = annotationMap(mapName, 120, 30);
        doThrow(new IllegalStateException("index failure")).when(documentIndex).executeOnKey(eq(ID_TYPE + ":doc"), any());

        assertThrows(IllegalStateException.class, () -> consumer.accept(message(REMOTE_REGION, ID_TYPE, annotation("doc", "annotation"))));
        verify(annotationMap).putTransient(eq("annotation"), any(AnnotationMessage.class), eq(120L), eq(TimeUnit.SECONDS), eq(30L), eq(TimeUnit.SECONDS));
    }

    @Test
    void repairsIndexForAnnotationAlreadyInCache() throws Exception {
        String mapName = ANNOTATIONS_MAP + ID_TYPE + ":doc";
        IMap<String,AnnotationMessage> annotationMap = annotationMap(mapName, 120, 30);
        when(annotationMap.containsKey("annotation")).thenReturn(true);

        consumer.accept(message(REMOTE_REGION, ID_TYPE, annotation("doc", "annotation")));

        verify(annotationMap, never()).putTransient(any(), any(), anyLong(), any(), anyLong(), any());
        verify(documentIndex).executeOnKey(eq(ID_TYPE + ":doc"), any());
    }

    @Test
    void failsAndLeavesLockUnreleasedWhenLockCannotBeAcquired() throws Exception {
        String mapName = ANNOTATIONS_MAP + ID_TYPE + ":doc";
        IMap<String,AnnotationMessage> annotationMap = annotationMap(mapName, 120, 30);
        AnnotationCacheProperties properties = new AnnotationCacheProperties();
        properties.setFederationLockWait(java.time.Duration.ofMillis(150));
        consumer = new LoadCacheConsumer(hazelcastInstance, regionConfiguration, properties).loadCache();
        when(annotationMap.tryLock("annotation", 150L, TimeUnit.MILLISECONDS)).thenReturn(false);

        assertThrows(AnnotationStorageException.class, () -> consumer.accept(message(REMOTE_REGION, ID_TYPE, annotation("doc", "annotation"))));
        verify(annotationMap, never()).unlock("annotation");
        verify(documentIndex, never()).executeOnKey(any(), any());
    }

    @Test
    void restoresInterruptWhenLockAcquisitionIsInterrupted() throws Exception {
        String mapName = ANNOTATIONS_MAP + ID_TYPE + ":doc";
        IMap<String,AnnotationMessage> annotationMap = annotationMap(mapName, 120, 30);
        when(annotationMap.tryLock("annotation", 5000L, TimeUnit.MILLISECONDS)).thenThrow(new InterruptedException());

        try {
            assertThrows(AnnotationStorageException.class, () -> consumer.accept(message(REMOTE_REGION, ID_TYPE, annotation("doc", "annotation"))));
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void unlocksEntryWhenTransientInsertionFails() throws Exception {
        String mapName = ANNOTATIONS_MAP + ID_TYPE + ":doc";
        IMap<String,AnnotationMessage> annotationMap = annotationMap(mapName, 120, 30);
        // @formatter:off
        doThrow(new IllegalStateException("failure")).when(annotationMap).putTransient(
                        any(), any(), eq(120L), eq(TimeUnit.SECONDS), eq(30L), eq(TimeUnit.SECONDS));
        // @formatter:on

        assertThrows(IllegalStateException.class, () -> consumer.accept(message(REMOTE_REGION, ID_TYPE, annotation("doc", "annotation"))));
        verify(annotationMap).tryLock("annotation", 5000, TimeUnit.MILLISECONDS);
        verify(annotationMap).unlock("annotation");
        verify(documentIndex, never()).executeOnKey(any(), any());
    }

    @SuppressWarnings("unchecked")
    private IMap<String,AnnotationMessage> annotationMap(String mapName, int ttlSeconds, int maxIdleSeconds) throws Exception {
        IMap<String,AnnotationMessage> annotationMap = mock(IMap.class);
        when(hazelcastInstance.<String,AnnotationMessage> getMap(mapName)).thenReturn(annotationMap);
        MapConfig mapConfig = new MapConfig(mapName).setTimeToLiveSeconds(ttlSeconds).setMaxIdleSeconds(maxIdleSeconds);
        when(config.findMapConfig(mapName)).thenReturn(mapConfig);
        doReturn(true).when(annotationMap).tryLock(any(), anyLong(), any());
        // add default explicit behavior to return false
        when(annotationMap.containsKey(any())).thenReturn(false);
        return annotationMap;
    }

    private Annotation annotation(String documentId, String annotationId) {
        return Annotation.newBuilder().setDocumentId(documentId).setAnnotationId(annotationId).build();
    }

    private AnnotationMessage message(String region, String idType, Annotation... annotations) {
        // @formatter:off
        return AnnotationMessage.newBuilder()
                        .putParameters(REGION_ID_PARAMETER, region)
                        .putParameters(ID_TYPE_PARAMETER, idType)
                        .putParameters(PERSISTENCE_MODE_PARAMETER, PersistenceMode.WRITE_THROUGH.value())
                        .addAllAnnotations(java.util.List.of(annotations))
                        .build();
        // @formatter:on
    }
}
