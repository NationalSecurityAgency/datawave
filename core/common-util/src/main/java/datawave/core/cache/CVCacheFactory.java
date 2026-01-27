package datawave.core.cache;

/**
 * Factory for creating various implementations of a {@link CVCache}.
 * <p>
 * The default implementation is the {@link CaffeineCVCache}.
 */
public class CVCacheFactory {

    public static CVCache create() {
        return new CaffeineCVCache();
    }

    public static CVCache createCaffeineCVCache() {
        return new CaffeineCVCache();
    }

    public static CVCache createLRUMapCVCache() {
        return new LRUMapCVCache();
    }

    public static CVCache createGuavaCVCache() {
        return new GuavaCVCache();
    }
}
