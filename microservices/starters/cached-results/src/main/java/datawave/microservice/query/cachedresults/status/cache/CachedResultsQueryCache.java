package datawave.microservice.query.cachedresults.status.cache;

import datawave.microservice.query.cachedresults.config.CachedResultsQueryProperties;
import datawave.microservice.query.cachedresults.status.CachedResultsQueryStatus;
import datawave.microservice.query.cachedresults.status.cache.util.CacheUpdater;
import datawave.microservice.query.cachedresults.status.cache.util.LockedCacheUpdateUtil;
import datawave.security.authorization.ProxiedUserDetails;
import datawave.webservice.query.exception.QueryException;

public class CachedResultsQueryCache {
    private final CachedResultsQueryProperties cachedResultsQueryProperties;
    private final CachedResultsQueryStatusCache queryStatusCache;
    private final DefinedQueryIdByCachedQueryIdCache definedQueryIdByCachedQueryIdCache;
    private final DefinedQueryIdByAliasCache definedQueryIdByAliasCache;
    private final DefinedQueryIdByViewCache definedQueryIdByViewCache;
    
    private LockedCacheUpdateUtil<CachedResultsQueryStatus> queryStatusLockedCacheUpdateUtil;
    
    public CachedResultsQueryCache(CachedResultsQueryProperties cachedResultsQueryProperties, CachedResultsQueryStatusCache queryStatusCache,
                    DefinedQueryIdByCachedQueryIdCache definedQueryIdByCachedQueryIdCache, DefinedQueryIdByAliasCache definedQueryIdByAliasCache,
                    DefinedQueryIdByViewCache definedQueryIdByViewCache) {
        this.cachedResultsQueryProperties = cachedResultsQueryProperties;
        this.queryStatusCache = queryStatusCache;
        this.definedQueryIdByCachedQueryIdCache = definedQueryIdByCachedQueryIdCache;
        this.definedQueryIdByAliasCache = definedQueryIdByAliasCache;
        this.definedQueryIdByViewCache = definedQueryIdByViewCache;
        this.queryStatusLockedCacheUpdateUtil = new LockedCacheUpdateUtil<>(queryStatusCache);
    }
    
    public CachedResultsQueryStatus createQuery(String definedQueryId, String cachedQueryId, String alias, ProxiedUserDetails currentUser) {
        return queryStatusCache.create(definedQueryId, cachedQueryId, alias, currentUser);
    }
    
    public CachedResultsQueryStatus lookupQueryStatus(String key) {
        CachedResultsQueryStatus cachedResultsQueryStatus = getQueryStatus(key);
        
        if (cachedResultsQueryStatus == null) {
            cachedResultsQueryStatus = getQueryStatusByCachedQueryId(key);
            
            if (cachedResultsQueryStatus == null) {
                cachedResultsQueryStatus = getQueryStatusByAlias(key);
                
                if (cachedResultsQueryStatus == null) {
                    cachedResultsQueryStatus = getQueryStatusByView(key);
                }
            }
        }
        
        return cachedResultsQueryStatus;
    }
    
    public CachedResultsQueryStatus getQueryStatus(String definedQueryId) {
        return queryStatusCache.get(definedQueryId);
    }
    
    public CachedResultsQueryStatus update(String definedQueryId, CachedResultsQueryStatus cachedResultsQueryStatus)
                    throws QueryException, InterruptedException {
        return queryStatusCache.update(definedQueryId, cachedResultsQueryStatus);
    }
    
    public CachedResultsQueryStatus lockedUpdate(String definedQueryId, CacheUpdater<CachedResultsQueryStatus> updater)
                    throws QueryException, InterruptedException {
        return queryStatusLockedCacheUpdateUtil.lockedUpdate(definedQueryId, updater, cachedResultsQueryProperties.getLockWaitTimeMillis(),
                        cachedResultsQueryProperties.getLockLeaseTimeMillis());
    }
    
    public CachedResultsQueryStatus lockedUpdate(String definedQueryId, CacheUpdater<CachedResultsQueryStatus> updater, long waitTimeMillis,
                    long leaseTimeMillis) throws QueryException, InterruptedException {
        return queryStatusLockedCacheUpdateUtil.lockedUpdate(definedQueryId, updater, waitTimeMillis, leaseTimeMillis);
    }
    
    public void lockQueryStatus(String definedQueryId) {
        queryStatusCache.lock(definedQueryId);
    }
    
    public void lockQueryStatus(String definedQueryId, long leaseTimeMillis) {
        queryStatusCache.lock(definedQueryId, leaseTimeMillis);
    }
    
    public boolean tryLockQueryStatus(String definedQueryId) {
        return queryStatusCache.tryLock(definedQueryId);
    }
    
    public boolean tryLockQueryStatus(String definedQueryId, long waitTimeMillis) {
        return queryStatusCache.tryLock(definedQueryId, waitTimeMillis);
    }
    
    public boolean tryLockQueryStatus(String definedQueryId, long waitTimeMillis, long leaseTimeMillis) throws InterruptedException {
        return queryStatusCache.tryLock(definedQueryId, waitTimeMillis, leaseTimeMillis);
    }
    
    public void unlockQueryStatus(String definedQueryId) {
        queryStatusCache.unlock(definedQueryId);
    }
    
    public void forceUnlockQueryStatus(String definedQueryId) {
        queryStatusCache.forceUnlock(definedQueryId);
    }
    
    public CachedResultsQueryStatus removeQueryStatus(String definedQueryId) {
        CachedResultsQueryStatus cachedResultsQueryStatus = queryStatusCache.get(definedQueryId);
        queryStatusCache.remove(definedQueryId);
        removeQueryIdByCachedQueryIdLookup(cachedResultsQueryStatus.getCachedQueryId());
        removeQueryIdByViewLookup(cachedResultsQueryStatus.getView());
        removeQueryIdByAliasLookup(cachedResultsQueryStatus.getAlias());
        return cachedResultsQueryStatus;
    }
    
    public String lookupQueryId(String key) {
        String definedQueryId = lookupQueryIdByCachedQueryId(key);
        if (definedQueryId == null) {
            definedQueryId = lookupQueryIdByAlias(key);
            if (definedQueryId == null) {
                definedQueryId = lookupQueryIdByView(key);
            }
        }
        return definedQueryId;
    }
    
    private CachedResultsQueryStatus getQueryStatusByCachedQueryId(String cachedQueryId) {
        String definedQueryId = lookupQueryIdByCachedQueryId(cachedQueryId);
        return (definedQueryId != null) ? queryStatusCache.get(definedQueryId) : null;
    }
    
    public String putQueryIdByCachedQueryIdLookup(String cachedQueryId, String definedQueryId) {
        if (cachedQueryId != null) {
            return definedQueryIdByCachedQueryIdCache.update(cachedQueryId, definedQueryId);
        }
        return definedQueryId;
    }
    
    public String lookupQueryIdByCachedQueryId(String cachedQueryId) {
        return definedQueryIdByCachedQueryIdCache.get(cachedQueryId);
    }
    
    public void removeQueryIdByCachedQueryIdLookup(String cachedQueryId) {
        definedQueryIdByCachedQueryIdCache.remove(cachedQueryId);
    }
    
    private CachedResultsQueryStatus getQueryStatusByAlias(String alias) {
        String definedQueryId = lookupQueryIdByAlias(alias);
        return (definedQueryId != null) ? queryStatusCache.get(definedQueryId) : null;
    }
    
    public String putQueryIdByAliasLookup(String alias, String definedQueryId) {
        if (alias != null) {
            return definedQueryIdByAliasCache.update(alias, definedQueryId);
        }
        return definedQueryId;
    }
    
    public String lookupQueryIdByAlias(String alias) {
        return definedQueryIdByAliasCache.get(alias);
    }
    
    public void removeQueryIdByAliasLookup(String alias) {
        definedQueryIdByAliasCache.remove(alias);
    }
    
    private CachedResultsQueryStatus getQueryStatusByView(String view) {
        String definedQueryId = lookupQueryIdByView(view);
        return (definedQueryId != null) ? queryStatusCache.get(definedQueryId) : null;
    }
    
    public String putQueryIdByViewLookup(String view, String definedQueryId) {
        if (view != null) {
            return definedQueryIdByViewCache.update(view, definedQueryId);
        }
        return definedQueryId;
    }
    
    public String lookupQueryIdByView(String view) {
        return definedQueryIdByViewCache.get(view);
    }
    
    public void removeQueryIdByViewLookup(String view) {
        definedQueryIdByViewCache.remove(view);
    }
}
