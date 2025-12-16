package datawave.core.cache;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * A guava {@link LoadingCache} implementation of a {@link ClassCache}
 */
public class GuavaClassCache implements ClassCache {

    private final LoadingCache<String,Class<?>> cache;

    public GuavaClassCache() {
        this(DEFAULT_SIZE);
    }

    public GuavaClassCache(int size) {
        //  @formatter:off
        cache = CacheBuilder.newBuilder()
                .maximumSize(size)
                .expireAfterWrite(1, TimeUnit.HOURS)
                .build(new CacheLoader<>() {
                    @Override
                    public Class<?> load(String name)throws ClassNotFoundException {
                        return Class.forName(name);
                    }
                });
        //  @formatter:on
    }

    @Override
    public String name() {
        return "Guava";
    }

    @Override
    public Class<?> get(String name) throws ClassNotFoundException {
        try {
            return cache.get(name);
        } catch (ExecutionException e) {
            throw new ClassNotFoundException("Invalid Class name", e);
        }
    }
}
