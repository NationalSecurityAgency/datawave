package datawave.microservice.annotationCache;

import static datawave.microservice.annotationCache.api.Constants.ANNOTATIONS_MAP;
import static datawave.microservice.annotationCache.api.Constants.FETCH_MAP;
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

class AnnotationSyncListenerTest {
    private static final String CACHE_KEY = "UUID:document";
    private static final String ANNOTATION_MAP = ANNOTATIONS_MAP + CACHE_KEY;

    private HazelcastInstance hazelcastInstance;
    private IMap<Object,Object> fetchRecords;
    private AnnotationSyncListener listener;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        hazelcastInstance = mock(HazelcastInstance.class);
        fetchRecords = mock(IMap.class);
        when(hazelcastInstance.getMap(FETCH_MAP + CACHE_KEY)).thenReturn(fetchRecords);

        listener = new AnnotationSyncListener();
        listener.setHazelcastInstance(hazelcastInstance);
    }

    @Test
    void entryRemovalEvictionAndExpirationInvalidateDocumentFetchRecords() {
        EntryEvent<String,Object> event = entryEvent(ANNOTATION_MAP);

        listener.entryRemoved(event);
        listener.entryEvicted(event);
        listener.entryExpired(event);

        verify(fetchRecords, times(3)).clear();
    }

    @Test
    void mapClearAndEvictInvalidateDocumentFetchRecords() {
        MapEvent event = mapEvent(ANNOTATION_MAP);

        listener.mapCleared(event);
        listener.mapEvicted(event);

        verify(fetchRecords, times(2)).clear();
    }

    @Test
    void ignoresNonAnnotationMapsAndIncompleteAnnotationMapNames() {
        listener.entryRemoved(entryEvent("other-map"));
        listener.entryExpired(entryEvent(ANNOTATIONS_MAP));
        listener.mapCleared(mapEvent("other-map"));
        listener.mapEvicted(mapEvent(ANNOTATIONS_MAP));

        verify(hazelcastInstance, never()).getMap(FETCH_MAP + CACHE_KEY);
    }

    @Test
    void removalDoesNotFailBeforeHazelcastInstanceIsInjected() {
        AnnotationSyncListener uninitialized = new AnnotationSyncListener();

        uninitialized.entryExpired(entryEvent(ANNOTATION_MAP));
        uninitialized.mapCleared(mapEvent(ANNOTATION_MAP));
    }

    @SuppressWarnings("unchecked")
    private EntryEvent<String,Object> entryEvent(String mapName) {
        EntryEvent<String,Object> event = mock(EntryEvent.class);
        when(event.getName()).thenReturn(mapName);
        return event;
    }

    private MapEvent mapEvent(String mapName) {
        MapEvent event = mock(MapEvent.class);
        when(event.getName()).thenReturn(mapName);
        return event;
    }
}
