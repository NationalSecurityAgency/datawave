package datawave.microservice.query.cachedresults.status.cache;

import static datawave.microservice.query.cachedresults.status.cache.DefinedQueryIdByViewCache.CACHE_NAME;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.CachePut;

import datawave.microservice.cached.LockableCacheInspector;

@CacheConfig(cacheNames = CACHE_NAME)
public class DefinedQueryIdByViewCache extends LockableCache<String> {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    public static final String CACHE_NAME = "definedQueryIdByViewCache";
    
    public DefinedQueryIdByViewCache(LockableCacheInspector cacheInspector) {
        super(cacheInspector, CACHE_NAME);
    }
    
    @Override
    public String get(String view) {
        return cacheInspector.list(CACHE_NAME, String.class, view);
    }
    
    @Override
    @CachePut(key = "#view")
    public String update(String view, String queryId) {
        return queryId;
    }
    
    @CacheEvict(key = "#view")
    public void remove(String view) {
        if (log.isDebugEnabled()) {
            log.debug("Evicting view {}", view);
        }
    }
}
