package datawave.query.util.cache;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import datawave.query.attributes.Attributes;
import datawave.query.attributes.TypeAttribute;

/**
 * Cache for Class instances.
 * <p>
 * Used by {@link TypeAttribute} and {@link Attributes}
 */
public class ClassCache {

    //  @formatter:off
    private final LoadingCache<String,Class<?>> clazzCache = CacheBuilder.newBuilder()
            .maximumSize(128)
            .expireAfterAccess(1, TimeUnit.HOURS)
            .build(new CacheLoader<>() {
                @Override
                public Class<?> load(String clazz) throws Exception {
                    return Class.forName(clazz);
                }
            });
    //  @formatter:on

    public ClassCache() {}

    public Class<?> get(String clazz) throws ClassNotFoundException {
        try {
            return clazzCache.get(clazz);
        } catch (ExecutionException e) {
            throw new ClassNotFoundException(clazz, e);
        }
    }
}
