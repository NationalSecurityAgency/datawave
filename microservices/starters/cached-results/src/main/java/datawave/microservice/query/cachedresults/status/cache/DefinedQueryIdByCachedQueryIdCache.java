package datawave.microservice.query.cachedresults.status.cache;

import static datawave.microservice.query.cachedresults.status.cache.DefinedQueryIdByAliasCache.CACHE_NAME;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.CachePut;

import datawave.microservice.cached.LockableCacheInspector;

@CacheConfig(cacheNames = CACHE_NAME)
public class DefinedQueryIdByCachedQueryIdCache extends LockableCache<String> {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    public static final String CACHE_NAME = "definedQueryIdByCachedQueryIdCache";
    
    public DefinedQueryIdByCachedQueryIdCache(LockableCacheInspector cacheInspector) {
        super(cacheInspector, CACHE_NAME);
    }
    
    @Override
    public String get(String cachedQueryId) {
        return cacheInspector.list(CACHE_NAME, String.class, cachedQueryId);
    }
    
    @Override
    @CachePut(key = "#cachedQueryId")
    public String update(String cachedQueryId, String queryId) {
        return queryId;
    }
    
    @CacheEvict(key = "#cachedQueryId")
    public void remove(String cachedQueryId) {
        if (log.isDebugEnabled()) {
            log.debug("Evicting cachedQueryId {}", cachedQueryId);
        }
    }
}
