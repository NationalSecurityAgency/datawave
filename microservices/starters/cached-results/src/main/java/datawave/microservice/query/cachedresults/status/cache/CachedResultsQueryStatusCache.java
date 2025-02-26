package datawave.microservice.query.cachedresults.status.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.CachePut;

import datawave.microservice.cached.LockableCacheInspector;
import datawave.microservice.query.cachedresults.status.CachedResultsQueryStatus;
import datawave.security.authorization.ProxiedUserDetails;

@CacheConfig(cacheNames = CachedResultsQueryStatusCache.CACHE_NAME)
public class CachedResultsQueryStatusCache extends LockableCache<CachedResultsQueryStatus> {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    public static final String CACHE_NAME = "cachedResultsQueryStatusCache";
    
    public CachedResultsQueryStatusCache(LockableCacheInspector cacheInspector) {
        super(cacheInspector, CACHE_NAME);
    }
    
    @CachePut(key = "#definedQueryId")
    public CachedResultsQueryStatus create(String definedQueryId, String cachedQueryId, String alias, ProxiedUserDetails currentUser) {
        return new CachedResultsQueryStatus(definedQueryId, cachedQueryId, alias, currentUser);
    }
    
    @Override
    public CachedResultsQueryStatus get(String definedQueryId) {
        return cacheInspector.list(CACHE_NAME, CachedResultsQueryStatus.class, definedQueryId);
    }
    
    @Override
    @CachePut(key = "#definedQueryId")
    public CachedResultsQueryStatus update(String definedQueryId, CachedResultsQueryStatus cachedResultsQueryStatus) {
        cachedResultsQueryStatus.setLastUpdatedMillis(System.currentTimeMillis());
        return cachedResultsQueryStatus;
    }
    
    @CacheEvict(key = "#definedQueryId")
    public void remove(String definedQueryId) {
        if (log.isDebugEnabled()) {
            log.debug("Evicting queryId {}", definedQueryId);
        }
    }
}
