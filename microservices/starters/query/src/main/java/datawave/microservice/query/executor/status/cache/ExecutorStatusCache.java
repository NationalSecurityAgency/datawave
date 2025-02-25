package datawave.microservice.query.executor.status.cache;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.CachePut;

import datawave.microservice.cached.LockableCacheInspector;

@CacheConfig(cacheNames = ExecutorStatusCache.CACHE_NAME)
public class ExecutorStatusCache extends LockableCache<ExecutorPoolStatus> {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    public static final String CACHE_NAME = "ExecutorStatusCache";
    
    public ExecutorStatusCache(LockableCacheInspector cacheInspector) {
        super(cacheInspector, CACHE_NAME);
    }
    
    @Override
    @CachePut(key = "#poolName")
    public ExecutorPoolStatus update(String poolName, ExecutorPoolStatus executorPoolStatus) {
        log.debug("Updating executor pool status for pool: {}", executorPoolStatus.getPoolName());
        return executorPoolStatus;
    }
    
    @CacheEvict(key = "#poolName")
    public void delete(String poolName) {
        log.debug("Deleting executor pool status for pool: {}", poolName);
    }
    
    @CacheEvict(allEntries = true)
    public void evictAll() {
        log.debug("Evicting all executor pool status");
    }
    
    @Override
    public ExecutorPoolStatus get(String poolName) {
        ExecutorPoolStatus poolStatus = null;
        try {
            poolStatus = cacheInspector.list(CACHE_NAME, ExecutorPoolStatus.class, poolName);
        } catch (Exception e) {
            log.error("Failed to retrieve executor pool status for {}", poolName);
            throw e;
        }
        return poolStatus;
    }
    
    public Map<String,ExecutorPoolStatus> getPoolStatusMap() {
        List<? extends ExecutorPoolStatus> executorPoolStatusList = cacheInspector.listAll(CACHE_NAME, ExecutorPoolStatus.class);
        Map<String,ExecutorPoolStatus> statusByPool = new LinkedHashMap<>();
        executorPoolStatusList.forEach(status -> statusByPool.put(status.getPoolName(), status));
        return statusByPool;
    }
}
