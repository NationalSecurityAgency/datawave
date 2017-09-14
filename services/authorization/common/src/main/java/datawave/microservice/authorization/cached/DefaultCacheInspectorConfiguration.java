package datawave.microservice.authorization.cached;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.cache.concurrent.ConcurrentMapCache;
import org.springframework.cache.guava.GuavaCache;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * A default {@link CacheInspector} to use when no other inspector is specified on the classpath. Normally, a {@link CacheInspector} bean would be defined to go
 * with the selected cache provider.
 */
@Configuration
@ConditionalOnMissingBean(CacheInspector.class)
public class DefaultCacheInspectorConfiguration {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    
    @Bean
    @Autowired
    public CacheInspector cacheInspector(final CacheManager cacheManager) {
        return new CacheInspector() {
            @Override
            public <T> T list(String cacheName, Class<T> cacheObjectType, String key) {
                Cache cache = cacheManager.getCache(cacheName);
                return cache.get(key, cacheObjectType);
            }
            
            @Override
            public <T> List<? extends T> listAll(String cacheName, Class<T> cacheObjectType, boolean inMemoryOnly) {
                ConcurrentMap<Object,Object> map = getNativeCache(cacheName);
                if (map != null) {
                    return map.values().stream().map(cacheObjectType::cast).collect(Collectors.toList());
                } else {
                    logger.error("listAll not supported with default configuration. Returning empty list.");
                    return Collections.emptyList();
                }
            }
            
            @Override
            public <T> List<? extends T> listMatching(String cacheName, Class<T> cacheObjectType, String substring, boolean inMemoryOnly) {
                ConcurrentMap<Object,Object> map = getNativeCache(cacheName);
                if (map != null) {
                    // @formatter:off
                    return map.entrySet().stream()
                            .filter(e -> String.valueOf(e.getKey()).contains(substring))
                            .map(Map.Entry::getValue)
                            .map(cacheObjectType::cast)
                            .collect(Collectors.toList());
                    // @formatter:on
                } else {
                    logger.error("listMatching not supported with default configuration. Returning empty list.");
                    return Collections.emptyList();
                }
            }
            
            @Override
            public <T> int evictMatching(String cacheName, Class<T> cacheObjectType, String substring) {
                ConcurrentMap<Object,Object> map = getNativeCache(cacheName);
                if (map != null) {
                    int origSize = map.size();
                    map.entrySet().removeIf(e -> String.valueOf(e.getKey()).contains(substring));
                    return origSize - map.size();
                } else {
                    logger.error("evictMatching not supported with default configuration. No entries were evicted.");
                    return 0;
                }
            }
            
            private ConcurrentMap<Object,Object> getNativeCache(String cacheName) {
                Cache cache = cacheManager.getCache(cacheName);
                if (cache instanceof ConcurrentMapCache) {
                    return ((ConcurrentMapCache) cache).getNativeCache();
                } else if (cache instanceof GuavaCache) {
                    return ((GuavaCache) cache).getNativeCache().asMap();
                } else {
                    return null;
                }
            }
        };
    }
}
