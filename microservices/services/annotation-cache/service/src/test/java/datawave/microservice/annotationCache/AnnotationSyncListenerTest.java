package datawave.microservice.annotationCache;

import static datawave.microservice.annotationCache.api.Constants.ANNOTATIONS_MAP;
import static datawave.microservice.annotationCache.api.Constants.DOC_ANNOTATIONS_MAP;
import static datawave.microservice.annotationCache.api.Constants.FETCH_MAP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.AbstractMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapEvent;

import datawave.annotation.protobuf.v1.AnnotationMessage;

class AnnotationSyncListenerTest {
    private static final String CACHE_KEY = "UUID:document";
    private static final String ANNOTATION_MAP = ANNOTATIONS_MAP + CACHE_KEY;
    private static final String ANNOTATION_ID = "annotation";

    private HazelcastInstance hazelcastInstance;
    private IMap<String,Set<String>> documentIndex;
    private IMap<Object,Object> fetchRecords;
    private AnnotationSyncListener listener;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        hazelcastInstance = mock(HazelcastInstance.class);
        documentIndex = mock(IMap.class);
        fetchRecords = mock(IMap.class);
        when(hazelcastInstance.<String,Set<String>> getMap(DOC_ANNOTATIONS_MAP)).thenReturn(documentIndex);
        when(hazelcastInstance.getMap(FETCH_MAP + CACHE_KEY)).thenReturn(fetchRecords);

        listener = new AnnotationSyncListener();
        listener.setHazelcastInstance(hazelcastInstance);
    }

    @Test
    void entryAddedAppendsAnnotationIdToDocumentIndex() {
        listener.entryAdded(entryEvent(ANNOTATION_MAP, ANNOTATION_ID, AnnotationMessage.getDefaultInstance()));

        EntryProcessor<String,Set<String>,Void> processor = capturedProcessor();
        Map.Entry<String,Set<String>> entry = new AbstractMap.SimpleEntry<>(CACHE_KEY, null);
        processor.process(entry);
        assertEquals(Set.of(ANNOTATION_ID), entry.getValue());
        verify(fetchRecords, never()).clear();
    }

    @Test
    void entryUpdatedRepairsDocumentIndexAndIsIdempotent() {
        listener.entryUpdated(entryEvent(ANNOTATION_MAP, ANNOTATION_ID, AnnotationMessage.getDefaultInstance()));

        EntryProcessor<String,Set<String>,Void> processor = capturedProcessor();
        Map.Entry<String,Set<String>> entry = new AbstractMap.SimpleEntry<>(CACHE_KEY, new HashSet<>(Set.of(ANNOTATION_ID)));
        processor.process(entry);
        assertEquals(Set.of(ANNOTATION_ID), entry.getValue());
        verify(fetchRecords, never()).clear();
    }

    @Test
    void addAndUpdateIgnoreUnexpectedValues() {
        listener.entryAdded(entryEvent(ANNOTATION_MAP, ANNOTATION_ID, "not an annotation message"));
        listener.entryUpdated(entryEvent(ANNOTATION_MAP, ANNOTATION_ID, null));

        verify(documentIndex, never()).executeOnKey(any(), any());
        verify(fetchRecords, never()).clear();
    }

    @Test
    void entryEventsIgnoreNonAnnotationAndIncompleteMapNames() {
        listener.entryAdded(entryEvent("other-map", ANNOTATION_ID, AnnotationMessage.getDefaultInstance()));
        listener.entryAdded(entryEvent(ANNOTATIONS_MAP, ANNOTATION_ID, AnnotationMessage.getDefaultInstance()));
        listener.entryAdded(entryEvent(null, ANNOTATION_ID, AnnotationMessage.getDefaultInstance()));

        verify(documentIndex, never()).executeOnKey(any(), any());
    }

    @Test
    void entryEventsIgnoreNullKeys() {
        listener.entryAdded(entryEvent(ANNOTATION_MAP, null, AnnotationMessage.getDefaultInstance()));
        listener.entryRemoved(entryEvent(ANNOTATION_MAP, null, null));

        verify(documentIndex, never()).executeOnKey(any(), any());
        verify(fetchRecords, never()).clear();
    }

    @Test
    void addDoesNotFailBeforeHazelcastInstanceIsInjected() {
        AnnotationSyncListener uninitialized = new AnnotationSyncListener();

        uninitialized.entryAdded(entryEvent(ANNOTATION_MAP, ANNOTATION_ID, AnnotationMessage.getDefaultInstance()));
    }

    @Test
    void allEntryRemovalEventsRemoveIdAndInvalidateFetchRecords() {
        EntryEvent<String,Object> event = entryEvent(ANNOTATION_MAP, ANNOTATION_ID, AnnotationMessage.getDefaultInstance());

        listener.entryRemoved(event);
        listener.entryEvicted(event);
        listener.entryExpired(event);

        verify(documentIndex, times(3)).executeOnKey(eq(CACHE_KEY), any());
        verify(fetchRecords, times(3)).clear();
    }

    @Test
    void removalProcessorPreservesOtherAnnotationIds() {
        listener.entryExpired(entryEvent(ANNOTATION_MAP, ANNOTATION_ID, AnnotationMessage.getDefaultInstance()));

        EntryProcessor<String,Set<String>,Void> processor = capturedProcessor();
        Map.Entry<String,Set<String>> entry = new AbstractMap.SimpleEntry<>(CACHE_KEY, Set.of(ANNOTATION_ID, "other"));
        processor.process(entry);
        assertEquals(Set.of("other"), entry.getValue());
    }

    @Test
    void removalProcessorDeletesEmptyDocumentIndex() {
        listener.entryRemoved(entryEvent(ANNOTATION_MAP, ANNOTATION_ID, null));

        EntryProcessor<String,Set<String>,Void> processor = capturedProcessor();
        Map.Entry<String,Set<String>> entry = new AbstractMap.SimpleEntry<>(CACHE_KEY, Set.of(ANNOTATION_ID));
        processor.process(entry);
        assertNull(entry.getValue());
    }

    @Test
    void removalProcessorLeavesMissingIndexUnchanged() {
        listener.entryEvicted(entryEvent(ANNOTATION_MAP, ANNOTATION_ID, null));

        EntryProcessor<String,Set<String>,Void> processor = capturedProcessor();
        Map.Entry<String,Set<String>> missingEntry = new AbstractMap.SimpleEntry<>(CACHE_KEY, null);
        processor.process(missingEntry);
        assertNull(missingEntry.getValue());

        Map.Entry<String,Set<String>> differentIdEntry = new AbstractMap.SimpleEntry<>(CACHE_KEY, Set.of("other"));
        processor.process(differentIdEntry);
        assertEquals(Set.of("other"), differentIdEntry.getValue());
    }

    @Test
    void mapClearedRemovesDocumentIndexAndInvalidatesFetchRecords() {
        listener.mapCleared(mapEvent(ANNOTATION_MAP));

        verify(documentIndex).remove(CACHE_KEY);
        verify(fetchRecords).clear();
    }

    @Test
    void mapEvictedRemovesDocumentIndexAndInvalidatesFetchRecords() {
        listener.mapEvicted(mapEvent(ANNOTATION_MAP));

        verify(documentIndex).remove(CACHE_KEY);
        verify(fetchRecords).clear();
    }

    @Test
    void mapEventsIgnoreNonAnnotationMaps() {
        listener.mapCleared(mapEvent("other-map"));
        listener.mapEvicted(mapEvent(ANNOTATIONS_MAP));

        verify(documentIndex, never()).remove(any());
        verify(fetchRecords, never()).clear();
    }

    @Test
    void removalDoesNotFailBeforeHazelcastInstanceIsInjected() {
        AnnotationSyncListener uninitialized = new AnnotationSyncListener();

        uninitialized.entryExpired(entryEvent(ANNOTATION_MAP, ANNOTATION_ID, null));
        uninitialized.mapCleared(mapEvent(ANNOTATION_MAP));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private EntryProcessor<String,Set<String>,Void> capturedProcessor() {
        ArgumentCaptor<EntryProcessor> captor = ArgumentCaptor.forClass(EntryProcessor.class);
        verify(documentIndex).executeOnKey(eq(CACHE_KEY), captor.capture());
        return captor.getValue();
    }

    @SuppressWarnings("unchecked")
    private EntryEvent<String,Object> entryEvent(String mapName, String key, Object value) {
        EntryEvent<String,Object> event = mock(EntryEvent.class);
        when(event.getName()).thenReturn(mapName);
        when(event.getKey()).thenReturn(key);
        when(event.getValue()).thenReturn(value);
        return event;
    }

    private MapEvent mapEvent(String mapName) {
        MapEvent event = mock(MapEvent.class);
        when(event.getName()).thenReturn(mapName);
        return event;
    }
}
