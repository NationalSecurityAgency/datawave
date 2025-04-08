package datawave.autoconfigure;

import static org.springframework.util.Assert.state;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;

import org.springframework.boot.autoconfigure.cache.CacheType;

/**
 * Maps supported {@link CacheType} values to/from the associated configuration classes that configure the named cache type. This is effectively a copy of
 * org.springframework.boot.autoconfigure.cache.CacheConfigurations that maps to our own configuration classes that ensure the created bean is a
 * {@link org.springframework.context.annotation.Primary} bean. We need this since we create other cache manager instances.
 */
final class CacheConfigurations {
    
    private static final Map<CacheType,Class<?>> MAPPINGS;
    
    static {
        Map<CacheType,Class<?>> mappings = new EnumMap<>(CacheType.class);
        mappings.put(CacheType.GENERIC, GenericCacheConfiguration.class);
        mappings.put(CacheType.EHCACHE, EhCacheCacheConfiguration.class);
        mappings.put(CacheType.HAZELCAST, HazelcastCacheConfiguration.class);
        mappings.put(CacheType.CACHE2K, Cache2kCacheConfiguration.class);
        mappings.put(CacheType.CAFFEINE, CaffeineCacheConfiguration.class);
        mappings.put(CacheType.SIMPLE, SimpleCacheConfiguration.class);
        mappings.put(CacheType.NONE, NoOpCacheConfiguration.class);
        MAPPINGS = Collections.unmodifiableMap(mappings);
    }
    
    private CacheConfigurations() {}
    
    public static String getConfigurationClass(CacheType cacheType) {
        Class<?> configurationClass = MAPPINGS.get(cacheType);
        state(configurationClass != null, () -> "Unknown cache type " + cacheType);
        return configurationClass.getName();
    }
    
    public static CacheType getType(String configurationClassName) {
        for (Map.Entry<CacheType,Class<?>> entry : MAPPINGS.entrySet()) {
            if (entry.getValue().getName().equals(configurationClassName)) {
                return entry.getKey();
            }
        }
        throw new IllegalStateException("Unknown configuration class " + configurationClassName);
    }
}
