package datawave.microservice.audit.replay.util;

import datawave.microservice.cached.CacheInspector;
import org.springframework.cache.concurrent.ConcurrentMapCache;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * This cache inspector can be used when using Spring's simple (i.e. default) cache implementation.
 */
public class ConcurrentMapCacheInspector implements CacheInspector {
    
    private ConcurrentMapCacheManager cacheManager;
    
    public ConcurrentMapCacheInspector(ConcurrentMapCacheManager cacheManager) {
        this.cacheManager = cacheManager;
    }
    
    @Override
    public <T> T list(String cacheName, Class<T> cacheObjectType, String key) {
        return cacheManager.getCache(cacheName).get(key, cacheObjectType);
    }
    
    @Override
    public <T> List<? extends T> listAll(String cacheName, Class<T> cacheObjectType) {
        ConcurrentMapCache mapCache = ((ConcurrentMapCache) cacheManager.getCache(cacheName));
        if (mapCache != null)
            return mapCache.getNativeCache().values().stream().map(cacheObjectType::cast).collect(Collectors.toList());
        else
            return Collections.emptyList();
    }
    
    @Override
    public <T> List<? extends T> listMatching(String cacheName, Class<T> cacheObjectType, String substring) {
        ConcurrentMapCache mapCache = ((ConcurrentMapCache) cacheManager.getCache(cacheName));
        if (mapCache != null)
            return mapCache.getNativeCache().entrySet().stream().filter(e -> e.getKey().toString().contains(substring)).map(Map.Entry::getValue)
                            .map(cacheObjectType::cast).collect(Collectors.toList());
        else
            return Collections.emptyList();
    }
    
    @Override
    public <T> int evictMatching(String cacheName, Class<T> cacheObjectType, String substring) {
        ConcurrentMapCache mapCache = ((ConcurrentMapCache) cacheManager.getCache(cacheName));
        if (mapCache != null) {
            ConcurrentMap<Object,Object> map = mapCache.getNativeCache();
            Set<Object> keysToRemove = map.keySet().stream().filter(k -> k.toString().contains(substring)).collect(Collectors.toSet());
            keysToRemove.forEach(map::remove);
            return keysToRemove.size();
        } else {
            return 0;
        }
    }
}
