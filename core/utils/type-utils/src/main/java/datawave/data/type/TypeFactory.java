package datawave.data.type;

import java.util.concurrent.TimeUnit;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

/**
 * TypeFactory that uses an internal loading cache to limit new Type objects
 */
public class TypeFactory {

    private static final int DEFAULT_SIZE = 32;
    private static final int DEFAULT_TIMEOUT_MINUTES = 15;

    private final LoadingCache<String,Type<?>> typeCache;

    /**
     * Constructor that uses the default size and timeout
     */
    public TypeFactory() {
        this(DEFAULT_SIZE, DEFAULT_TIMEOUT_MINUTES);
    }

    /**
     * Constructor that uses custom size and timeout arguments
     *
     * @param size
     *            the cache size
     * @param timeout
     *            the timeout in minutes
     */
    public TypeFactory(int size, int timeout) {
        //  @formatter:off
        typeCache = Caffeine.newBuilder()
                        .maximumSize(size)
                        .expireAfterWrite(timeout, TimeUnit.MINUTES)
                        .build(className-> {
                            Class<?> clazz = Class.forName(className);
                            return (Type<?>) clazz.getDeclaredConstructor().newInstance();
                        });
        //  @formatter:on
    }

    /**
     * Create a {@link Type} for the given class name
     *
     * @param className
     *            the class name
     * @return the Type
     */
    public Type<?> createType(String className) {
        try {
            return typeCache.get(className);
        } catch (Exception e) {
            throw new IllegalStateException("Error creating instance of class " + className);
        }
    }

    /**
     * Expose current cache size
     *
     * @return the current cache size
     */
    public long getCacheSize() {
        return typeCache.estimatedSize();
    }

    /**
     * Perform pending maintenance tasks, which tasks are performed are implementation specific.
     * <p>
     * <b>Note:</b> there should be no need to call this method in production code. This is useful for unit tests.
     */
    public void cleanup() {
        typeCache.cleanUp();
    }
}
