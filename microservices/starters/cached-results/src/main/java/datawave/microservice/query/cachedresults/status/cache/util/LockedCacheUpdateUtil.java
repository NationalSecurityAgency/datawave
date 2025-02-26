package datawave.microservice.query.cachedresults.status.cache.util;

import datawave.microservice.query.cachedresults.status.cache.LockableCache;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.NotFoundQueryException;
import datawave.webservice.query.exception.QueryException;

public class LockedCacheUpdateUtil<T> {
    private final LockableCache<T> lockableCache;
    
    public LockedCacheUpdateUtil(LockableCache<T> lockableCache) {
        this.lockableCache = lockableCache;
    }
    
    public T lockedUpdate(String key, CacheUpdater<T> updater, long waitTimeMillis, long leaseTimeMillis) throws QueryException, InterruptedException {
        T cacheEntry = null;
        if (lockableCache.tryLock(key, waitTimeMillis, leaseTimeMillis)) {
            try {
                cacheEntry = lockableCache.get(key);
                if (cacheEntry != null) {
                    updater.apply(cacheEntry);
                    lockableCache.update(key, cacheEntry);
                } else {
                    throw new NotFoundQueryException(DatawaveErrorCode.NO_QUERY_OBJECT_MATCH, "Unable to find entry in cache.");
                }
            } finally {
                lockableCache.unlock(key);
            }
        } else {
            updater.onLockFailed();
        }
        return cacheEntry;
    }
}
