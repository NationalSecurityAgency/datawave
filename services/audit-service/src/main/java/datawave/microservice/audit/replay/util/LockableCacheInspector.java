package datawave.microservice.audit.replay.util;

import datawave.microservice.cached.CacheInspector;

import java.util.concurrent.TimeUnit;

public interface LockableCacheInspector extends CacheInspector {
    
    void lock(String cacheName, String key);
    
    void lock(String cacheName, String key, long leaseTime, TimeUnit leaseTimeUnit);
    
    boolean tryLock(String cacheName, String key);
    
    boolean tryLock(String cacheName, String key, long waitTime, TimeUnit waitTimeUnit) throws InterruptedException;
    
    boolean tryLock(String cacheName, String key, long waitTime, TimeUnit waitTimeUnit, long leaseTime, TimeUnit leaseTimeUnit) throws InterruptedException;
    
    void unlock(String cacheName, String key);
    
    void forceUnlock(String cacheName, String key);
}
