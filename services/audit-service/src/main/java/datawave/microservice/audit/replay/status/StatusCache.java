package datawave.microservice.audit.replay.status;

import datawave.microservice.cached.CacheInspector;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.CachePut;

import java.util.Date;
import java.util.List;

import static datawave.microservice.audit.replay.status.StatusCache.CACHE_NAME;

/**
 * The StatusCache is used to cache the status of every audit replay. By default, this will operate as a local concurrent map cache, but if the Hazelcast client
 * is enabled, this cache will be shared across multiple audit-service instances.
 */
@CacheConfig(cacheNames = CACHE_NAME)
public class StatusCache {
    public static final String CACHE_NAME = "auditReplay";
    
    private final CacheInspector cacheInspector;
    
    public StatusCache(CacheInspector cacheInspector) {
        this.cacheInspector = cacheInspector;
    }
    
    @CachePut(key = "#id")
    public Status create(String id, String path, String fileUri, long sendRate, boolean replayUnfinished) {
        Status status = new Status();
        status.setId(id);
        status.setState(Status.ReplayState.CREATED);
        status.setPath(path);
        status.setFileUri(fileUri);
        status.setSendRate(sendRate);
        status.setLastUpdated(new Date());
        status.setReplayUnfinishedFiles(replayUnfinished);
        return status;
    }
    
    public Status retrieve(String id) {
        return cacheInspector.list(CACHE_NAME, Status.class, id);
    }
    
    public List<Status> retrieveAll() {
        return (List<Status>) cacheInspector.listAll(CACHE_NAME, Status.class);
    }
    
    @CachePut(key = "#status.getId()")
    public Status update(Status status) {
        status.setLastUpdated(new Date());
        return status;
    }
    
    @CacheEvict(key = "#id")
    public String delete(String id) {
        return "Evicted " + id;
    }
    
    @CacheEvict(allEntries = true)
    public String deleteAll() {
        return "Evicted all entries";
    }
    
}
