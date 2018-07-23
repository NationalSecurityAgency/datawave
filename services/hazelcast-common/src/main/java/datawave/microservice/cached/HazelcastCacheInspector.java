package datawave.microservice.cached;

import com.hazelcast.core.IMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A {@link CacheInspector} that is capable of inspecting a {@link com.hazelcast.spring.cache.HazelcastCache}.
 */
class HazelcastCacheInspector implements CacheInspector {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final CacheManager cacheManager;
    
    public HazelcastCacheInspector(CacheManager cacheManager) {
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
        if (cache.getNativeCache() instanceof IMap) {
            @SuppressWarnings("unchecked")
            IMap<Object,Object> imap = (IMap<Object,Object>) cache.getNativeCache();
            // @formatter:off
            return imap.values().stream()
                    .map(cacheObjectType::cast)
                    .collect(Collectors.toList());
            // @formatter:on
        } else {
            logger.error("Native cache should be an IMap, but instead is {}.", cache.getNativeCache().getClass());
            return Collections.emptyList();
        }
    }
    
    @Override
    public <T> List<? extends T> listMatching(String cacheName, Class<T> cacheObjectType, String substring) {
        Cache cache = cacheManager.getCache(cacheName);
        if (cache.getNativeCache() instanceof IMap) {
            @SuppressWarnings("unchecked")
            IMap<Object,Object> imap = (IMap<Object,Object>) cache.getNativeCache();
            // @formatter:off
            return imap.values(e -> String.valueOf(e.getKey()).contains(substring)).stream()
                    .map(cacheObjectType::cast)
                    .collect(Collectors.toList());
            // @formatter:on
        } else {
            logger.error("Native cache should be an IMap, but instead is {}.", cache.getNativeCache().getClass());
            return Collections.emptyList();
        }
    }
    
    @Override
    public <T> int evictMatching(String cacheName, Class<T> cacheObjectType, String substring) {
        Cache cache = cacheManager.getCache(cacheName);
        if (cache.getNativeCache() instanceof IMap) {
            @SuppressWarnings("unchecked")
            IMap<Object,Object> imap = (IMap<Object,Object>) cache.getNativeCache();
            Set<Object> keysToRemove = imap.keySet(e -> String.valueOf(e.getKey()).contains(substring));
            keysToRemove.forEach(imap::remove);
            return keysToRemove.size();
        } else {
            logger.error("Native cache should be an IMap, but instead is {}.", cache.getNativeCache().getClass());
            return 0;
        }
    }
}
