package datawave.microservice.query.executor.status.cache.util;

import datawave.microservice.query.executor.status.cache.LockableCache;

public class LockedCacheUpdateUtil<T> {
    private final LockableCache<T> lockableCache;
    
    public LockedCacheUpdateUtil(LockableCache<T> lockableCache) {
        this.lockableCache = lockableCache;
    }
    
    public T lockedUpdate(String key, CacheUpdater<T> updater, long waitTimeMillis, long leaseTimeMillis) throws Exception, InterruptedException {
        T cacheEntry = null;
        if (lockableCache.tryLock(key, waitTimeMillis, leaseTimeMillis)) {
            try {
                cacheEntry = lockableCache.get(key);
                if (cacheEntry != null) {
                    updater.apply(cacheEntry);
                    lockableCache.update(key, cacheEntry);
                } else {
                    throw new Exception("Cache entry not found for key: " + key);
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
