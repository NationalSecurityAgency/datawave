package datawave.microservice.query.mapreduce.status.cache;

import static datawave.microservice.query.mapreduce.status.cache.MapReduceQueryIdByJobIdCache.CACHE_NAME;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.CachePut;

import datawave.microservice.cached.LockableCacheInspector;

@CacheConfig(cacheNames = CACHE_NAME)
public class MapReduceQueryIdByJobIdCache extends LockableCache<String> {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    public static final String CACHE_NAME = "mapReduceQueryIdByJobIdCache";
    
    public MapReduceQueryIdByJobIdCache(LockableCacheInspector cacheInspector) {
        super(cacheInspector, CACHE_NAME);
    }
    
    @Override
    public String get(String jobId) {
        return cacheInspector.list(CACHE_NAME, String.class, jobId);
    }
    
    @Override
    @CachePut(key = "#jobId")
    public String update(String jobId, String id) {
        if (log.isDebugEnabled()) {
            log.debug("Updating mrQueryId for job id {}", jobId);
        }
        return id;
    }
    
    @CacheEvict(key = "#jobId")
    public void remove(String jobId) {
        if (log.isDebugEnabled()) {
            log.debug("Evicting job id {}", jobId);
        }
    }
}
