package datawave.microservice.cached;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This cache inspector offers universal locking functionality for any given CacheInspector. This should not be used for distributed caches.
 */
public class UniversalLockableCacheInspector implements CacheInspector, LockableCacheInspector {
    private Map<String,ReentrantLock> lockMap = new HashMap<>();
    private CacheInspector cacheInspector;
    
    public UniversalLockableCacheInspector(CacheInspector cacheInspector) {
        this.cacheInspector = cacheInspector;
    }
    
    @Override
    public <T> T list(String cacheName, Class<T> cacheObjectType, String key) {
        return cacheInspector.list(cacheName, cacheObjectType, key);
    }
    
    @Override
    public <T> List<? extends T> listAll(String cacheName, Class<T> cacheObjectType) {
        return cacheInspector.listAll(cacheName, cacheObjectType);
    }
    
    @Override
    public <T> List<? extends T> listMatching(String cacheName, Class<T> cacheObjectType, String substring) {
        return cacheInspector.listMatching(cacheName, cacheObjectType, substring);
    }
    
    @Override
    public <T> int evictMatching(String cacheName, Class<T> cacheObjectType, String substring) {
        return cacheInspector.evictMatching(cacheName, cacheObjectType, substring);
    }
    
    @Override
    public void lock(String cacheName, String key) {
        getLock(cacheName).lock();
    }
    
    @Override
    public void lock(String cacheName, String key, long leaseTime, TimeUnit leaseTimeUnit) {
        getLock(cacheName).lock();
    }
    
    @Override
    public boolean tryLock(String cacheName, String key) {
        return getLock(cacheName).tryLock();
    }
    
    @Override
    public boolean tryLock(String cacheName, String key, long waitTime, TimeUnit waitTimeUnit) throws InterruptedException {
        return getLock(cacheName).tryLock(waitTime, waitTimeUnit);
    }
    
    @Override
    public boolean tryLock(String cacheName, String key, long waitTime, TimeUnit waitTimeUnit, long leaseTime, TimeUnit leaseTimeUnit)
                    throws InterruptedException {
        return getLock(cacheName).tryLock(waitTime, waitTimeUnit);
    }
    
    @Override
    public void unlock(String cacheName, String key) {
        getLock(cacheName).unlock();
    }
    
    @Override
    public void forceUnlock(String cacheName, String key) {
        getLock(cacheName).unlock();
    }
    
    private ReentrantLock getLock(String cacheName) {
        if (lockMap.get(cacheName) == null)
            lockMap.put(cacheName, new ReentrantLock(true));
        return lockMap.get(cacheName);
    }
}
