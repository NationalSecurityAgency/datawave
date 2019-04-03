package datawave.microservice.audit.replay.status;

import datawave.microservice.cached.LockableCacheInspector;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.CachePut;

import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static datawave.microservice.audit.replay.status.StatusCache.CACHE_NAME;

/**
 * The StatusCache is used to cache the status of every audit replay. By default, this will operate as a local concurrent map cache, but if the Hazelcast client
 * is enabled, this cache will be shared across multiple audit-service instances.
 */
@CacheConfig(cacheNames = CACHE_NAME)
public class StatusCache {
    public static final String CACHE_NAME = "auditReplay";
    
    private final LockableCacheInspector cacheInspector;
    
    public StatusCache(LockableCacheInspector cacheInspector) {
        this.cacheInspector = cacheInspector;
    }
    
    @CachePut(key = "#id")
    public Status create(String id, String path, long sendRate, boolean replayUnfinished) {
        Status status = new Status();
        status.setId(id);
        status.setState(Status.ReplayState.CREATED);
        status.setPathUri(path);
        status.setSendRate(sendRate);
        status.setLastUpdated(new Date());
        status.setReplayUnfinishedFiles(replayUnfinished);
        return status;
    }
    
    public Status retrieve(String id) {
        return cacheInspector.list(CACHE_NAME, Status.class, id);
    }
    
    public List<String> retrieveAllIds() {
        return cacheInspector.listAll(CACHE_NAME, Status.class).stream().map(Status::getId).collect(Collectors.toList());
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
    
    public void lock(String id) {
        cacheInspector.lock(CACHE_NAME, id);
    }
    
    public void lock(String id, long leaseTimeMillis) {
        cacheInspector.lock(CACHE_NAME, id, leaseTimeMillis, TimeUnit.MILLISECONDS);
    }
    
    public boolean tryLock(String id) {
        return cacheInspector.tryLock(CACHE_NAME, id);
    }
    
    public boolean tryLock(String id, long waitTimeMillis) {
        try {
            return cacheInspector.tryLock(CACHE_NAME, id, waitTimeMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return false;
        }
    }
    
    public boolean tryLock(String id, long waitTimeMillis, long leaseTimeMillis) {
        try {
            return cacheInspector.tryLock(CACHE_NAME, id, waitTimeMillis, TimeUnit.MILLISECONDS, leaseTimeMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return false;
        }
    }
    
    public void unlock(String id) {
        cacheInspector.unlock(CACHE_NAME, id);
    }
    
    public void forceUnlock(String id) {
        cacheInspector.forceUnlock(CACHE_NAME, id);
    }
    
}
