package datawave.query.util;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Component;

/**
 * A utility to help with evicting entries from the caches used by {@link MetadataHelper}, {@link TypeMetadataHelper},
 * {@link datawave.query.composite.CompositeMetadataHelper}, and {@link AllFieldMetadataHelper}. Normally this could be handled with the
 * {@link org.springframework.cache.annotation.CacheEvict} annotation on a method. However, we want to be sure that all caches in the cache manager are cleared.
 */
@Component
public class MetadataCacheManager {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final CacheManager cacheManager;
    
    public MetadataCacheManager(@Qualifier("metadataHelperCacheManager") CacheManager cacheManager) {
        this.cacheManager = cacheManager;
    }
    
    /**
     * Evicts all entries from all caches in the metadata helper {@link CacheManager}.
     */
    public void evictCaches() {
        cacheManager.getCacheNames().forEach(cacheName -> {
            Cache cache = cacheManager.getCache(cacheName);
            if (cache != null) {
                log.debug("Clearing metadata cache {}.");
                cache.clear();
            }
        });
    }
    
    /**
     * Dump all entries in the metadata helper {@link CacheManager}'s caches.
     * 
     * @param log
     *            the logger to use when printing entries
     * @param prefix
     *            a prefix string to prepend on the log messages
     */
    public void showMeDaCache(Logger log, String prefix) {
        for (String cacheName : cacheManager.getCacheNames()) {
            log.trace(prefix + " got " + cacheName);
            Cache cache = cacheManager.getCache(cacheName);
            if (cache != null) {
                Object nativeCache = cache.getNativeCache();
                log.trace("nativeCache is a " + nativeCache);
                if (nativeCache instanceof com.github.benmanes.caffeine.cache.Cache) {
                    com.github.benmanes.caffeine.cache.Cache caffeineCache = (com.github.benmanes.caffeine.cache.Cache) nativeCache;
                    Map map = caffeineCache.asMap();
                    log.trace("cache map is " + map);
                    log.trace("cache map size is " + map.size());
                    for (Object key : map.keySet()) {
                        log.trace("value for " + key + " is :" + map.get(key));
                    }
                } else {
                    log.warn("Expected native cache to be a " + com.github.benmanes.caffeine.cache.Cache.class + " but it was a " + nativeCache.getClass());
                }
            }
        }
    }
}
