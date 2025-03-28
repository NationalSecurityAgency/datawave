package datawave.microservice.query.mapreduce.status.cache;

import java.util.concurrent.TimeUnit;

import datawave.microservice.cached.LockableCacheInspector;

public abstract class LockableCache<T> {
    protected final LockableCacheInspector cacheInspector;
    private final String cacheName;
    
    protected LockableCache(LockableCacheInspector cacheInspector, String cacheName) {
        this.cacheInspector = cacheInspector;
        this.cacheName = cacheName;
    }
    
    public abstract T get(String key);
    
    public abstract T update(String key, T entry);
    
    public void lock(String key) {
        cacheInspector.lock(cacheName, key);
    }
    
    public void lock(String key, long leaseTimeMillis) {
        cacheInspector.lock(cacheName, key, leaseTimeMillis, TimeUnit.MILLISECONDS);
    }
    
    public boolean tryLock(String key) {
        return cacheInspector.tryLock(cacheName, key);
    }
    
    public boolean tryLock(String key, long waitTimeMillis) {
        try {
            return cacheInspector.tryLock(cacheName, key, waitTimeMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return false;
        }
    }
    
    public boolean tryLock(String key, long waitTimeMillis, long leaseTimeMillis) throws InterruptedException {
        return cacheInspector.tryLock(cacheName, key, waitTimeMillis, TimeUnit.MILLISECONDS, leaseTimeMillis, TimeUnit.MILLISECONDS);
    }
    
    public void unlock(String key) {
        cacheInspector.unlock(cacheName, key);
    }
    
    public void forceUnlock(String key) {
        cacheInspector.forceUnlock(cacheName, key);
    }
    
}
