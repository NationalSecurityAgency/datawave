package datawave.microservice.annotationCache;

import static datawave.microservice.annotationCache.api.Constants.ANNOTATIONS_MAP;
import static datawave.microservice.annotationCache.api.Constants.ID_TYPE_PARAMETER;
import static datawave.microservice.annotationCache.api.Constants.PERSISTENCE_MODE_PARAMETER;
import static datawave.microservice.annotationCache.api.Constants.REGION_ID_PARAMETER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

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
import datawave.microservice.annotationCache.api.PersistenceMode;
import datawave.microservice.annotationCache.api.RegionConfiguration;

class LoadCacheConsumerTest {
    private static final String LOCAL_REGION = "local";
    private static final String REMOTE_REGION = "remote";
    private static final String ID_TYPE = "UUID";

    private HazelcastInstance hazelcastInstance;
    private Config config;
    private RegionConfiguration regionConfiguration;
    private Consumer<AnnotationMessage> consumer;

    @BeforeEach
    void setUp() {
        hazelcastInstance = mock(HazelcastInstance.class);
        config = mock(Config.class);
        when(hazelcastInstance.getConfig()).thenReturn(config);

        regionConfiguration = new RegionConfiguration();
        regionConfiguration.setName(LOCAL_REGION);
        consumer = new LoadCacheConsumer(hazelcastInstance, regionConfiguration).loadCache();
    }

    @Test
    void constructorRequiresRegionConfiguration() {
        assertThrows(IllegalStateException.class, () -> new LoadCacheConsumer(hazelcastInstance, null));

        RegionConfiguration missingName = new RegionConfiguration();
        assertThrows(IllegalStateException.class, () -> new LoadCacheConsumer(hazelcastInstance, missingName));

        missingName.setName("  ");
        assertThrows(IllegalStateException.class, () -> new LoadCacheConsumer(hazelcastInstance, missingName));
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
    void storesRemoteAnnotationInDocumentMapWithoutInvokingMapStore() {
        String mapName = ANNOTATIONS_MAP + ID_TYPE + ":doc";
        IMap<String,AnnotationMessage> annotationMap = annotationMap(mapName, 120, 30);
        AnnotationMessage message = message(REMOTE_REGION, ID_TYPE, annotation("doc", "annotation"));

        consumer.accept(message);

        verify(annotationMap).lock("annotation");
        verify(annotationMap).putTransient("annotation", message, 120, TimeUnit.SECONDS, 30, TimeUnit.SECONDS);
        verify(annotationMap).unlock("annotation");
        verify(annotationMap, never()).put(any(), any());
        verify(annotationMap, never()).set(any(), any());
    }

    @Test
    void leavesExistingImmutableAnnotationUntouched() {
        String mapName = ANNOTATIONS_MAP + ID_TYPE + ":doc";
        IMap<String,AnnotationMessage> annotationMap = annotationMap(mapName, 120, 30);
        when(annotationMap.containsKey("annotation")).thenReturn(true);

        consumer.accept(message(REMOTE_REGION, ID_TYPE, annotation("doc", "annotation")));

        verify(annotationMap).lock("annotation");
        verify(annotationMap, never()).putTransient(any(), any(), eq(120L), eq(TimeUnit.SECONDS), eq(30L), eq(TimeUnit.SECONDS));
        verify(annotationMap).unlock("annotation");
        verifyNoInteractions(config);
    }

    @Test
    void normalizesBatchedMessagesToOneAnnotationPerEntry() {
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
    void usesEachAnnotationsDocumentMapForBatchedMessages() {
        IMap<String,AnnotationMessage> firstMap = annotationMap(ANNOTATIONS_MAP + ID_TYPE + ":first-doc", 0, 0);
        IMap<String,AnnotationMessage> secondMap = annotationMap(ANNOTATIONS_MAP + ID_TYPE + ":second-doc", 0, 0);

        consumer.accept(message(REMOTE_REGION, ID_TYPE, annotation("first-doc", "first"), annotation("second-doc", "second")));

        verify(firstMap).putTransient(eq("first"), any(AnnotationMessage.class), eq(0L), eq(TimeUnit.SECONDS), eq(0L), eq(TimeUnit.SECONDS));
        verify(secondMap).putTransient(eq("second"), any(AnnotationMessage.class), eq(0L), eq(TimeUnit.SECONDS), eq(0L), eq(TimeUnit.SECONDS));
    }

    @Test
    void unlocksEntryWhenTransientInsertionFails() {
        String mapName = ANNOTATIONS_MAP + ID_TYPE + ":doc";
        IMap<String,AnnotationMessage> annotationMap = annotationMap(mapName, 120, 30);
        // @formatter:off
        doThrow(new IllegalStateException("failure")).when(annotationMap).putTransient(
                        any(), any(), eq(120L), eq(TimeUnit.SECONDS), eq(30L), eq(TimeUnit.SECONDS));
        // @formatter:on

        assertThrows(IllegalStateException.class, () -> consumer.accept(message(REMOTE_REGION, ID_TYPE, annotation("doc", "annotation"))));
        verify(annotationMap).unlock("annotation");
    }

    @SuppressWarnings("unchecked")
    private IMap<String,AnnotationMessage> annotationMap(String mapName, int ttlSeconds, int maxIdleSeconds) {
        IMap<String,AnnotationMessage> annotationMap = mock(IMap.class);
        when(hazelcastInstance.<String,AnnotationMessage> getMap(mapName)).thenReturn(annotationMap);
        MapConfig mapConfig = new MapConfig(mapName).setTimeToLiveSeconds(ttlSeconds).setMaxIdleSeconds(maxIdleSeconds);
        when(config.findMapConfig(mapName)).thenReturn(mapConfig);
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
