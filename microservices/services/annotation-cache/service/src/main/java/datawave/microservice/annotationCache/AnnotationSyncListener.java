package datawave.microservice.annotationCache;

import static datawave.microservice.annotationCache.api.Constants.ANNOTATIONS_MAP;
import static datawave.microservice.annotationCache.api.Constants.DOCUMENT_ID_KEY_ATTRIBUTE;
import static datawave.microservice.annotationCache.api.Constants.FETCH_MAP;
import static datawave.microservice.annotationCache.api.Constants.ID_TYPE_KEY_ATTRIBUTE;

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
import com.hazelcast.query.Predicates;

import datawave.microservice.annotationCache.api.AnnotationKey;
import datawave.microservice.annotationCache.api.FetchKey;
import datawave.microservice.annotationCache.api.FetchRecord;

/** Invalidates fetch records when cached annotation data is removed. */
@Component
public class AnnotationSyncListener implements EntryRemovedListener<AnnotationKey,Object>, EntryEvictedListener<AnnotationKey,Object>,
                EntryExpiredListener<AnnotationKey,Object>, MapClearedListener, MapEvictedListener, HazelcastInstanceAware {
    private static final Logger log = LoggerFactory.getLogger(AnnotationSyncListener.class);

    private volatile HazelcastInstance instance;

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.instance = hazelcastInstance;
    }

    @Override
    public void entryRemoved(EntryEvent<AnnotationKey,Object> event) {
        invalidateDocument(event.getName(), event.getKey());
    }

    @Override
    public void entryEvicted(EntryEvent<AnnotationKey,Object> event) {
        invalidateDocument(event.getName(), event.getKey());
    }

    @Override
    public void entryExpired(EntryEvent<AnnotationKey,Object> event) {
        invalidateDocument(event.getName(), event.getKey());
    }

    @Override
    public void mapCleared(MapEvent event) {
        invalidateAll(event.getName());
    }

    @Override
    public void mapEvicted(MapEvent event) {
        invalidateAll(event.getName());
    }

    private void invalidateDocument(String annotationMapName, AnnotationKey key) {
        if (!ANNOTATIONS_MAP.equals(annotationMapName) || key == null) {
            log.debug("Ignoring invalidation event from non-annotation map or with invalid key {}", annotationMapName);
            return;
        }

        HazelcastInstance hazelcastInstance = instance;
        if (hazelcastInstance == null) {
            log.warn("Cannot invalidate fetch records for {} because the Hazelcast instance has not been injected", key);
            return;
        }

        IMap<FetchKey,FetchRecord> fetchRecords = hazelcastInstance.getMap(FETCH_MAP);
        fetchRecords.removeAll(Predicates.and(Predicates.equal(ID_TYPE_KEY_ATTRIBUTE, key.getIdType()),
                        Predicates.equal(DOCUMENT_ID_KEY_ATTRIBUTE, key.getDocumentId())));
        log.debug("Invalidated fetch records for idType={} documentId={} after annotation entry removal", key.getIdType(), key.getDocumentId());
    }

    private void invalidateAll(String annotationMapName) {
        if (!ANNOTATIONS_MAP.equals(annotationMapName)) {
            log.debug("Ignoring map-wide invalidation event from non-annotation map {}", annotationMapName);
            return;
        }

        HazelcastInstance hazelcastInstance = instance;
        if (hazelcastInstance == null) {
            log.warn("Cannot invalidate all fetch records because the Hazelcast instance has not been injected");
            return;
        }

        hazelcastInstance.<FetchKey,FetchRecord> getMap(FETCH_MAP).clear();
        log.debug("Invalidated all fetch records after annotation map clear or eviction");
    }
}
