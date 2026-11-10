package datawave.accumulo.inmemory;

import java.time.Duration;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * A cache of {@link InMemoryAccumulo} instances keyed by instance name. Callers should use the shared, JVM-wide instance returned by {@link #getDefault()}.
 * <p>
 * Every caller that asks the same cache for the same name gets the same instance, so separate clients (for example a mock bulk ingest job and the code that
 * reads its results) see the same tables and data. Tests that use named instances should still clean up after themselves with {@link #remove(String)} or
 * {@link #clear()}, but as a safety net an instance in the default cache is evicted automatically once it has gone {@link #EXPIRE_AFTER_ACCESS} without being
 * looked up. Eviction only drops the cache's reference: callers that still hold the instance keep using it, and the next lookup by that name gets a new, empty
 * instance.
 */
public final class InMemoryAccumuloCache {

    /**
     * How long an instance in the default cache can go without being looked up before it is evicted. Every lookup resets the clock, and instances are normally
     * shared within one test class or job, so an hour leaves plenty of room while still reclaiming instances that are never removed.
     */
    public static final Duration EXPIRE_AFTER_ACCESS = Duration.ofHours(1);

    private static final InMemoryAccumuloCache DEFAULT = new InMemoryAccumuloCache(Caffeine.newBuilder().expireAfterAccess(EXPIRE_AFTER_ACCESS).build());

    private final Cache<String,InMemoryAccumulo> cache;

    /**
     * Package-private so tests can inject their own cache, for example one with a custom ticker.
     *
     * @param cache
     *            the backing cache
     */
    InMemoryAccumuloCache(Cache<String,InMemoryAccumulo> cache) {
        this.cache = cache;
    }

    /**
     * @return the shared, JVM-wide cache, whose instances expire after going {@link #EXPIRE_AFTER_ACCESS} without being looked up
     */
    public static InMemoryAccumuloCache getDefault() {
        return DEFAULT;
    }

    /**
     * Gets the InMemoryAccumulo instance with the given name from this cache, creating and caching a new instance if there is none.
     *
     * @param instanceName
     *            the instance name
     * @return the cached InMemoryAccumulo for the given name
     */
    public InMemoryAccumulo getOrCreate(String instanceName) {
        return cache.get(instanceName, InMemoryAccumulo::new);
    }

    /**
     * Removes the instance with the given name from this cache.
     *
     * @param instanceName
     *            the instance name
     * @return the removed instance, or null if there was none
     */
    public InMemoryAccumulo remove(String instanceName) {
        return cache.asMap().remove(instanceName);
    }

    /**
     * Removes every instance from this cache.
     */
    public void clear() {
        cache.invalidateAll();
    }

    /**
     * @return the approximate number of cached instances, which may still count expired instances that have not been cleaned up yet
     */
    public int size() {
        return Math.toIntExact(cache.estimatedSize());
    }
}
