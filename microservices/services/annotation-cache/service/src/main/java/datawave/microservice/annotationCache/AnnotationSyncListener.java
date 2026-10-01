package datawave.microservice.annotationCache;

import static datawave.microservice.annotationCache.api.Constants.ANNOTATIONS_MAP;
import static datawave.microservice.annotationCache.api.Constants.FETCH_MAP;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapEvent;
import com.hazelcast.map.listener.EntryEvictedListener;
import com.hazelcast.map.listener.EntryExpiredListener;
import com.hazelcast.map.listener.EntryRemovedListener;
import com.hazelcast.map.listener.MapClearedListener;
import com.hazelcast.map.listener.MapEvictedListener;

/** Invalidates a document's fetch records when cached annotation data is removed. */
@Component
public class AnnotationSyncListener implements EntryRemovedListener<String,Object>, EntryEvictedListener<String,Object>, EntryExpiredListener<String,Object>,
                MapClearedListener, MapEvictedListener, HazelcastInstanceAware {
    private static final Logger log = LoggerFactory.getLogger(AnnotationSyncListener.class);

    private volatile HazelcastInstance instance;

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.instance = hazelcastInstance;
    }

    @Override
    public void entryRemoved(EntryEvent<String,Object> event) {
        invalidateFetchRecords(event.getName());
    }

    @Override
    public void entryEvicted(EntryEvent<String,Object> event) {
        invalidateFetchRecords(event.getName());
    }

    @Override
    public void entryExpired(EntryEvent<String,Object> event) {
        invalidateFetchRecords(event.getName());
    }

    @Override
    public void mapCleared(MapEvent event) {
        invalidateFetchRecords(event.getName());
    }

    @Override
    public void mapEvicted(MapEvent event) {
        invalidateFetchRecords(event.getName());
    }

    private void invalidateFetchRecords(String annotationMapName) {
        String cacheKey = cacheKey(annotationMapName);
        if (cacheKey == null) {
            return;
        }

        HazelcastInstance hazelcastInstance = instance;
        if (hazelcastInstance == null) {
            log.warn("Cannot invalidate fetch records for {} because the Hazelcast instance has not been injected", annotationMapName);
            return;
        }

        AnnotationMapCleanup.withDocumentLock(hazelcastInstance, cacheKey, () -> {
            IMap<String,Object> fetchRecords = hazelcastInstance.getMap(FETCH_MAP + cacheKey);
            fetchRecords.clear();
            return null;
        });
        log.debug("Invalidated fetch records for {} after annotation cache removal", cacheKey);
    }

    private String cacheKey(String mapName) {
        if (mapName == null || !mapName.startsWith(ANNOTATIONS_MAP) || mapName.length() == ANNOTATIONS_MAP.length()) {
            log.debug("Ignoring removal event from non-annotation map {}", mapName);
            return null;
        }
        return mapName.substring(ANNOTATIONS_MAP.length());
    }
}
