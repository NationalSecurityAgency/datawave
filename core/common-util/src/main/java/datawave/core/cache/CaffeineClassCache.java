package datawave.core.cache;

import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

/**
 * A {@link Caffeine} implementation of a {@link ClassCache}
 */
public class CaffeineClassCache implements ClassCache {

    private final LoadingCache<String,Class<?>> cache;

    public CaffeineClassCache() {
        this(DEFAULT_SIZE);
    }

    public CaffeineClassCache(int size) {
        //  @formatter:off
        cache = Caffeine.newBuilder()
                .maximumSize(size)
                .expireAfterAccess(1, TimeUnit.HOURS)
                .build(Class::forName);
        //  @formatter:on
    }

    @Override
    public String name() {
        return "Caffeine";
    }

    @Override
    public Class<?> get(String name) throws ClassNotFoundException {
        try {
            return cache.get(name);
        } catch (CompletionException e) {
            throw new ClassNotFoundException(name, e);
        }
    }
}
