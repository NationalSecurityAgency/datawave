package datawave.microservice.annotationCache;

import static datawave.microservice.annotationCache.api.Constants.ANNOTATIONS_MAP;
import static datawave.microservice.annotationCache.api.Constants.FETCH_MAP;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapEvent;
import com.hazelcast.query.Predicate;

import datawave.microservice.annotationCache.api.AnnotationKey;
import datawave.microservice.annotationCache.api.FetchKey;
import datawave.microservice.annotationCache.api.FetchRecord;

class AnnotationSyncListenerTest {
    private static final AnnotationKey KEY = new AnnotationKey("UUID", "document", "annotation");

    private HazelcastInstance hazelcastInstance;
    private IMap<FetchKey,FetchRecord> fetchRecords;
    private AnnotationSyncListener listener;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        hazelcastInstance = mock(HazelcastInstance.class);
        fetchRecords = mock(IMap.class);
        when(hazelcastInstance.<FetchKey,FetchRecord> getMap(FETCH_MAP)).thenReturn(fetchRecords);

        listener = new AnnotationSyncListener();
        listener.setHazelcastInstance(hazelcastInstance);
    }

    @Test
    void entryRemovalEvictionAndExpirationRemoveMatchingDocumentFetchRecords() {
        EntryEvent<AnnotationKey,Object> event = entryEvent(ANNOTATIONS_MAP, KEY);

        listener.entryRemoved(event);
        listener.entryEvicted(event);
        listener.entryExpired(event);

        verify(fetchRecords, times(3)).removeAll(any(Predicate.class));
        verify(fetchRecords, never()).clear();
    }

    @Test
    void mapClearAndEvictInvalidateAllFetchRecords() {
        MapEvent event = mapEvent(ANNOTATIONS_MAP);

        listener.mapCleared(event);
        listener.mapEvicted(event);

        verify(fetchRecords, times(2)).clear();
    }

    @Test
    void ignoresNonAnnotationEventsAndEntriesWithoutAValidKey() {
        listener.entryRemoved(entryEvent("other-map", KEY));
        listener.entryExpired(entryEvent(ANNOTATIONS_MAP, null));
        listener.mapCleared(mapEvent("other-map"));
        listener.mapEvicted(mapEvent(FETCH_MAP));

        verify(hazelcastInstance, never()).getMap(FETCH_MAP);
    }

    @Test
    void removalDoesNotFailBeforeHazelcastInstanceIsInjected() {
        AnnotationSyncListener uninitialized = new AnnotationSyncListener();

        uninitialized.entryExpired(entryEvent(ANNOTATIONS_MAP, KEY));
        uninitialized.mapCleared(mapEvent(ANNOTATIONS_MAP));
    }

    @SuppressWarnings("unchecked")
    private EntryEvent<AnnotationKey,Object> entryEvent(String mapName, AnnotationKey key) {
        EntryEvent<AnnotationKey,Object> event = mock(EntryEvent.class);
        when(event.getName()).thenReturn(mapName);
        when(event.getKey()).thenReturn(key);
        return event;
    }

    private MapEvent mapEvent(String mapName) {
        MapEvent event = mock(MapEvent.class);
        when(event.getName()).thenReturn(mapName);
        return event;
    }
}
