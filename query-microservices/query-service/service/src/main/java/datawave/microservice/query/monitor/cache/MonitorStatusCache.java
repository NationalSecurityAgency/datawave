package datawave.microservice.query.monitor.cache;

import datawave.microservice.cached.LockableCacheInspector;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.CachePut;

import java.util.concurrent.TimeUnit;

@CacheConfig(cacheNames = MonitorStatusCache.CACHE_NAME)
public class MonitorStatusCache {
    private final LockableCacheInspector cacheInspector;
    
    public static final String CACHE_NAME = "QueryMonitorCache";
    public static final String CACHE_KEY = "MonitorStatus";
    
    public MonitorStatusCache(LockableCacheInspector cacheInspector) {
        this.cacheInspector = cacheInspector;
    }
    
    /**
     * Get the query monitor status
     * 
     * @return
     */
    public MonitorStatus getStatus() {
        return cacheInspector.list(CACHE_NAME, MonitorStatus.class, CACHE_KEY);
    }
    
    /**
     * Store the query monitor status
     * 
     * @param monitorStatus
     * @return
     */
    @CachePut(key = CACHE_KEY)
    public MonitorStatus setStatus(MonitorStatus monitorStatus) {
        return monitorStatus;
    }
    
    /**
     * Deletes the query monitor status
     */
    @CacheEvict(key = CACHE_KEY)
    public void deleteStatus() {
        
    }
    
    public void lock() {
        cacheInspector.lock(CACHE_NAME, CACHE_KEY);
    }
    
    public void lock(long leaseTime, TimeUnit leaseTimeUnit) {
        cacheInspector.lock(CACHE_NAME, CACHE_KEY, leaseTime, leaseTimeUnit);
    }
    
    public boolean tryLock() {
        return cacheInspector.tryLock(CACHE_NAME, CACHE_KEY);
    }
    
    public boolean tryLock(long waitTime, TimeUnit waitTimeUnit) throws InterruptedException {
        return cacheInspector.tryLock(CACHE_NAME, CACHE_KEY, waitTime, waitTimeUnit);
    }
    
    public boolean tryLock(long waitTime, TimeUnit waitTimeUnit, long leaseTime, TimeUnit leaseTimeUnit) throws InterruptedException {
        return cacheInspector.tryLock(CACHE_NAME, CACHE_KEY, waitTime, waitTimeUnit, leaseTime, leaseTimeUnit);
    }
    
    public void unlock() {
        cacheInspector.unlock(CACHE_NAME, CACHE_KEY);
    }
    
    public void forceUnlock() {
        cacheInspector.forceUnlock(CACHE_NAME, CACHE_KEY);
    }
    
    public boolean isLocked() {
        return cacheInspector.isLocked(CACHE_NAME, CACHE_KEY);
    }
}
