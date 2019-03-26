package datawave.microservice.cached;

import com.hazelcast.core.IMap;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;

import java.util.concurrent.TimeUnit;

/**
 * This cache inspector extends the default HazelcastCacheInspector, and adds a locking mechanism.
 */
public class LockableHazelcastCacheInspector extends HazelcastCacheInspector implements LockableCacheInspector {
    public LockableHazelcastCacheInspector(CacheManager cacheManager) {
        super(cacheManager);
    }
    
    @Override
    public void lock(String cacheName, String key) {
        Cache cache = cacheManager.getCache(cacheName);
        if (cache.getNativeCache() instanceof IMap) {
            @SuppressWarnings("unchecked")
            IMap<Object,Object> imap = (IMap<Object,Object>) cache.getNativeCache();
            imap.lock(key);
        }
    }
    
    @Override
    public void lock(String cacheName, String key, long leaseTime, TimeUnit leaseTimeUnit) {
        Cache cache = cacheManager.getCache(cacheName);
        if (cache.getNativeCache() instanceof IMap) {
            @SuppressWarnings("unchecked")
            IMap<Object,Object> imap = (IMap<Object,Object>) cache.getNativeCache();
            imap.lock(key, leaseTime, leaseTimeUnit);
        }
    }
    
    @Override
    public boolean tryLock(String cacheName, String key) {
        Cache cache = cacheManager.getCache(cacheName);
        if (cache.getNativeCache() instanceof IMap) {
            @SuppressWarnings("unchecked")
            IMap<Object,Object> imap = (IMap<Object,Object>) cache.getNativeCache();
            return imap.tryLock(key);
        }
        return false;
    }
    
    @Override
    public boolean tryLock(String cacheName, String key, long waitTime, TimeUnit waitTimeUnit) throws InterruptedException {
        Cache cache = cacheManager.getCache(cacheName);
        if (cache.getNativeCache() instanceof IMap) {
            @SuppressWarnings("unchecked")
            IMap<Object,Object> imap = (IMap<Object,Object>) cache.getNativeCache();
            return imap.tryLock(key, waitTime, waitTimeUnit);
        }
        return false;
    }
    
    @Override
    public boolean tryLock(String cacheName, String key, long waitTime, TimeUnit waitTimeUnit, long leaseTime, TimeUnit leaseTimeUnit)
                    throws InterruptedException {
        Cache cache = cacheManager.getCache(cacheName);
        if (cache.getNativeCache() instanceof IMap) {
            @SuppressWarnings("unchecked")
            IMap<Object,Object> imap = (IMap<Object,Object>) cache.getNativeCache();
            return imap.tryLock(key, waitTime, waitTimeUnit, leaseTime, leaseTimeUnit);
        }
        return false;
    }
    
    @Override
    public void unlock(String cacheName, String key) {
        Cache cache = cacheManager.getCache(cacheName);
        if (cache.getNativeCache() instanceof IMap) {
            @SuppressWarnings("unchecked")
            IMap<Object,Object> imap = (IMap<Object,Object>) cache.getNativeCache();
            imap.unlock(key);
        }
    }
    
    @Override
    public void forceUnlock(String cacheName, String key) {
        Cache cache = cacheManager.getCache(cacheName);
        if (cache.getNativeCache() instanceof IMap) {
            @SuppressWarnings("unchecked")
            IMap<Object,Object> imap = (IMap<Object,Object>) cache.getNativeCache();
            imap.forceUnlock(key);
        }
    }
}
