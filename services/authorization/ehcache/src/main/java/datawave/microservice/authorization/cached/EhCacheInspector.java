package datawave.microservice.authorization.cached;

import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A {@link CacheInspector} that is capable of inspecting an {@link Ehcache}. This bridges the Spring cache configuration API to use native Ehcache methods to
 * list the entries stored in the cache.
 */
class EhCacheInspector implements CacheInspector {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    
    @Autowired
    private CacheManager cacheManager;
    
    @Override
    public <T> T list(String cacheName, Class<T> cacheObjectType, String key) {
        Cache cache = cacheManager.getCache(cacheName);
        return cache.get(key, cacheObjectType);
    }
    
    @Override
    public <T> List<? extends T> listAll(String cacheName, Class<T> cacheObjectType, boolean inMemoryOnly) {
        // @formatter:off
        return listElements(cacheName, inMemoryOnly)
            .map(Element::getObjectValue)
            .map(cacheObjectType::cast)
            .collect(Collectors.toList());
        // @formatter:on
    }
    
    @Override
    public <T> List<? extends T> listMatching(String cacheName, Class<T> cacheObjectType, String substring, boolean inMemoryOnly) {
        // @formatter:off
        return listElements(cacheName, inMemoryOnly)
            .filter(e -> String.valueOf(e.getObjectKey()).contains(substring))
            .map(Element::getObjectValue)
            .map(cacheObjectType::cast)
            .collect(Collectors.toList());
        // @formatter:on
    }
    
    @Override
    public <T> int evictMatching(String cacheName, Class<T> cacheObjectType, String substring) {
        Cache cache = cacheManager.getCache(cacheName);
        if (cache.getNativeCache() instanceof Ehcache) {
            Ehcache ehcache = (Ehcache) cache.getNativeCache();
            @SuppressWarnings("unchecked")
            List<Object> keys = ehcache.getKeys();
            // @formatter:off
            List<Object> keysToRemove = keys.parallelStream()
                    .map(String::valueOf)
                    .filter(s -> s.contains(substring))
                    .collect(Collectors.toList());
            // @formatter:on
            ehcache.removeAll(keysToRemove);
            return keysToRemove.size();
        } else {
            logger.error("Native cache should be Ehcache, but instead is {}", cache.getNativeCache().getClass());
            return 0;
        }
    }
    
    private Stream<Element> listElements(String cacheName, boolean inMemoryOnly) {
        Cache cache = cacheManager.getCache(cacheName);
        if (cache.getNativeCache() instanceof Ehcache) {
            Ehcache ehcache = (Ehcache) cache.getNativeCache();
            @SuppressWarnings("unchecked")
            List<Object> allKeys = ehcache.getKeys();
            List<Object> keysInMemory = allKeys;
            if (inMemoryOnly) {
                keysInMemory = allKeys.parallelStream().filter(ehcache::isElementInMemory).collect(Collectors.toList());
            }
            Map<Object,Element> cachedObjects = ehcache.getAll(keysInMemory);
            return cachedObjects.values().parallelStream().filter(e -> !e.isExpired());
        } else {
            logger.error("Native cache should be Ehcache, but instead is {}", cache.getNativeCache().getClass());
            return Stream.empty();
        }
    }
}
