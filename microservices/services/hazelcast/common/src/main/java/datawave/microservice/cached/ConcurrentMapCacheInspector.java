package datawave.microservice.cached;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;

/**
 * A {@link CacheInspector} that is capable of inspecting a {@link org.springframework.cache.concurrent.ConcurrentMapCache}.
 */
public class ConcurrentMapCacheInspector implements CacheInspector {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final CacheManager cacheManager;
    
    public ConcurrentMapCacheInspector(CacheManager cacheManager) {
        this.cacheManager = cacheManager;
    }
    
    @Override
    public <T> T list(String cacheName, Class<T> cacheObjectType, String key) {
        Cache cache = cacheManager.getCache(cacheName);
        return cache.get(key, cacheObjectType);
    }
    
    @Override
    public <T> List<? extends T> listAll(String cacheName, Class<T> cacheObjectType) {
        Cache cache = cacheManager.getCache(cacheName);
        if (cache.getNativeCache() instanceof ConcurrentMap) {
            @SuppressWarnings("unchecked")
            ConcurrentMap<Object,Object> map = (ConcurrentMap<Object,Object>) cache.getNativeCache();
            // @formatter:off
            return map.values().stream()
                    .map(cacheObjectType::cast)
                    .collect(Collectors.toList());
            // @formatter:on
        } else {
            logger.error("Native cache should be a ConcurrentMap Cache, but instead is {}.", cache.getNativeCache().getClass());
            return Collections.emptyList();
        }
    }
    
    @Override
    public <T> List<? extends T> listMatching(String cacheName, Class<T> cacheObjectType, String substring) {
        Cache cache = cacheManager.getCache(cacheName);
        if (cache.getNativeCache() instanceof ConcurrentMap) {
            @SuppressWarnings("unchecked")
            ConcurrentMap<Object,Object> map = (ConcurrentMap<Object,Object>) cache.getNativeCache();
            // @formatter:off
            return map.entrySet().stream()
                    .filter(e -> String.valueOf(e.getKey()).contains(substring))
                    .map(Map.Entry::getValue)
                    .map(cacheObjectType::cast)
                    .collect(Collectors.toList());
            // @formatter:on
        } else {
            logger.error("Native cache should be a ConcurrentMap Cache, but instead is {}.", cache.getNativeCache().getClass());
            return Collections.emptyList();
        }
    }
    
    @Override
    public <T> int evictMatching(String cacheName, Class<T> cacheObjectType, String substring) {
        org.springframework.cache.Cache cache = cacheManager.getCache(cacheName);
        if (cache.getNativeCache() instanceof ConcurrentMap) {
            @SuppressWarnings("unchecked")
            ConcurrentMap<Object,Object> map = (ConcurrentMap<Object,Object>) cache.getNativeCache();
            int[] removed = new int[1];
            map.keySet().removeIf(k -> {
                if (String.valueOf(k).contains(substring)) {
                    removed[0]++;
                    return true;
                } else {
                    return false;
                }
            });
            return removed[0];
        } else {
            logger.error("Native cache should be a ConcurrentMap Cache, but instead is {}.", cache.getNativeCache().getClass());
            return 0;
        }
    }
}
