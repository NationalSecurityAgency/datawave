package datawave.microservice.annotationCache;

import static datawave.microservice.annotationCache.api.Constants.ANNOTATIONS_MAP;
import static datawave.microservice.annotationCache.api.Constants.DOC_ANNOTATIONS_MAP;
import static datawave.microservice.annotationCache.api.Constants.FETCH_MAP;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapEvent;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryEvictedListener;
import com.hazelcast.map.listener.EntryExpiredListener;
import com.hazelcast.map.listener.EntryRemovedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;
import com.hazelcast.map.listener.MapClearedListener;
import com.hazelcast.map.listener.MapEvictedListener;

import datawave.annotation.protobuf.v1.AnnotationMessage;
import datawave.microservice.annotationCache.api.entryProcessor.AppendAnnotationIdProcessor;
import datawave.microservice.annotationCache.api.entryProcessor.RemoveAnnotationIdProcessor;

/**
 * Keeps the document annotation index synchronized with the per-document annotation maps.
 *
 * <p>
 * Adding or updating an annotation ensures that its ID is present in the document index. Removing, evicting, or expiring an annotation removes its ID and
 * invalidates all fetch records for the document so that Sonicweb will consult permanent storage again. Clearing or evicting an annotation map removes the
 * complete document index and also invalidates its fetch records.
 * </p>
 *
 * <p>
 * The listener is registered only on maps named {@code annotations:*}. The suffix of the map name is the document cache key shared by the index and fetch maps.
 * </p>
 */
@Component
public class AnnotationSyncListener implements EntryAddedListener<String,Object>, EntryUpdatedListener<String,Object>, EntryRemovedListener<String,Object>,
                EntryEvictedListener<String,Object>, EntryExpiredListener<String,Object>, MapClearedListener, MapEvictedListener, HazelcastInstanceAware {
    private static final Logger log = LoggerFactory.getLogger(AnnotationSyncListener.class);

    private volatile HazelcastInstance instance;

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.instance = hazelcastInstance;
    }

    @Override
    public void entryAdded(EntryEvent<String,Object> event) {
        addToDocumentIndex(event);
    }

    @Override
    public void entryUpdated(EntryEvent<String,Object> event) {
        addToDocumentIndex(event);
    }

    @Override
    public void entryRemoved(EntryEvent<String,Object> event) {
        removeFromDocumentIndex(event);
    }

    @Override
    public void entryEvicted(EntryEvent<String,Object> event) {
        removeFromDocumentIndex(event);
    }

    @Override
    public void entryExpired(EntryEvent<String,Object> event) {
        removeFromDocumentIndex(event);
    }

    @Override
    public void mapCleared(MapEvent event) {
        removeDocumentIndex(event.getName());
    }

    @Override
    public void mapEvicted(MapEvent event) {
        removeDocumentIndex(event.getName());
    }

    private void addToDocumentIndex(EntryEvent<String,Object> event) {
        String cacheKey = cacheKey(event.getName());
        if (cacheKey == null || event.getKey() == null) {
            return;
        }

        Object value = event.getValue();
        if (!(value instanceof AnnotationMessage)) {
            log.warn("Ignoring unexpected value in annotation map {} for key {}: {}", event.getName(), event.getKey(),
                            value == null ? "null" : value.getClass().getName());
            return;
        }

        HazelcastInstance hazelcastInstance = instance;
        if (hazelcastInstance == null) {
            log.warn("Cannot synchronize annotation {} from {} because the Hazelcast instance has not been injected", event.getKey(), event.getName());
            return;
        }

        IMap<String,Set<String>> documentIndex = hazelcastInstance.getMap(DOC_ANNOTATIONS_MAP);
        documentIndex.executeOnKey(cacheKey, new AppendAnnotationIdProcessor(event.getKey()));
        log.debug("Added annotation {} to document index {}", event.getKey(), cacheKey);
    }

    private void removeFromDocumentIndex(EntryEvent<String,Object> event) {
        String cacheKey = cacheKey(event.getName());
        if (cacheKey == null || event.getKey() == null) {
            return;
        }

        HazelcastInstance hazelcastInstance = instance;
        if (hazelcastInstance == null) {
            log.warn("Cannot synchronize removal of annotation {} from {} because the Hazelcast instance has not been injected", event.getKey(),
                            event.getName());
            return;
        }

        IMap<String,Set<String>> documentIndex = hazelcastInstance.getMap(DOC_ANNOTATIONS_MAP);
        documentIndex.executeOnKey(cacheKey, new RemoveAnnotationIdProcessor(event.getKey()));
        invalidateFetchRecords(hazelcastInstance, cacheKey);
        log.debug("Removed annotation {} from document index {} and invalidated its fetch records", event.getKey(), cacheKey);
    }

    private void removeDocumentIndex(String mapName) {
        String cacheKey = cacheKey(mapName);
        if (cacheKey == null) {
            return;
        }

        HazelcastInstance hazelcastInstance = instance;
        if (hazelcastInstance == null) {
            log.warn("Cannot synchronize removal of annotation map {} because the Hazelcast instance has not been injected", mapName);
            return;
        }

        hazelcastInstance.getMap(DOC_ANNOTATIONS_MAP).remove(cacheKey);
        invalidateFetchRecords(hazelcastInstance, cacheKey);
        log.debug("Removed document index {} and invalidated its fetch records", cacheKey);
    }

    private void invalidateFetchRecords(HazelcastInstance hazelcastInstance, String cacheKey) {
        hazelcastInstance.getMap(FETCH_MAP + cacheKey).clear();
    }

    private String cacheKey(String mapName) {
        if (mapName == null || !mapName.startsWith(ANNOTATIONS_MAP) || mapName.length() == ANNOTATIONS_MAP.length()) {
            log.debug("Ignoring event from non-annotation map {}", mapName);
            return null;
        }
        return mapName.substring(ANNOTATIONS_MAP.length());
    }
}
