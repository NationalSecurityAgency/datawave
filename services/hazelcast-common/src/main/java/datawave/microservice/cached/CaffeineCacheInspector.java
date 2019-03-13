package datawave.microservice.cached;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.CacheManager;

import com.github.benmanes.caffeine.cache.Cache;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A {@link CacheInspector} that is capable of inspecting a {@link org.springframework.cache.caffeine.CaffeineCache}.
 */
public class CaffeineCacheInspector implements CacheInspector {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final CacheManager cacheManager;
    
    public CaffeineCacheInspector(CacheManager cacheManager) {
        this.cacheManager = cacheManager;
    }
    
    @Override
    public <T> T list(String cacheName, Class<T> cacheObjectType, String key) {
        org.springframework.cache.Cache cache = cacheManager.getCache(cacheName);
        return cache.get(key, cacheObjectType);
    }
    
    @Override
    public <T> List<? extends T> listAll(String cacheName, Class<T> cacheObjectType) {
        org.springframework.cache.Cache cache = cacheManager.getCache(cacheName);
        if (cache.getNativeCache() instanceof Cache) {
            @SuppressWarnings("unchecked")
            Cache<Object,Object> map = (Cache<Object,Object>) cache.getNativeCache();
            // @formatter:off
            return map.asMap().values().stream()
                    .map(cacheObjectType::cast)
                    .collect(Collectors.toList());
            // @formatter:on
        } else {
            logger.error("Native cache should be an caffeine Cache, but instead is {}.", cache.getNativeCache().getClass());
            return Collections.emptyList();
        }
    }
    
    @Override
    public <T> List<? extends T> listMatching(String cacheName, Class<T> cacheObjectType, String substring) {
        org.springframework.cache.Cache cache = cacheManager.getCache(cacheName);
        if (cache.getNativeCache() instanceof Cache) {
            @SuppressWarnings("unchecked")
            Cache<Object,Object> map = (Cache<Object,Object>) cache.getNativeCache();
            // @formatter:off
            return map.asMap().entrySet().stream()
                    .filter(e -> String.valueOf(e.getKey()).contains(substring))
                    .map(Map.Entry::getValue)
                    .map(cacheObjectType::cast)
                    .collect(Collectors.toList());
            // @formatter:on
        } else {
            logger.error("Native cache should be an caffeine Cache, but instead is {}.", cache.getNativeCache().getClass());
            return Collections.emptyList();
        }
    }
    
    @Override
    public <T> int evictMatching(String cacheName, Class<T> cacheObjectType, String substring) {
        org.springframework.cache.Cache cache = cacheManager.getCache(cacheName);
        if (cache.getNativeCache() instanceof Cache) {
            @SuppressWarnings("unchecked")
            Cache<Object,Object> map = (Cache<Object,Object>) cache.getNativeCache();
            int[] removed = new int[1];
            map.asMap().keySet().removeIf(k -> {
                if (String.valueOf(k).contains(substring)) {
                    removed[0]++;
                    return true;
                } else {
                    return false;
                }
            });
            return removed[0];
        } else {
            logger.error("Native cache should be an caffeine Cache, but instead is {}.", cache.getNativeCache().getClass());
            return 0;
        }
    }
}
