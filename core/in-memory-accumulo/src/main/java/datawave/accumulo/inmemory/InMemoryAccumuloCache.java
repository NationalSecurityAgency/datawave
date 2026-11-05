package datawave.accumulo.inmemory;

import java.time.Duration;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Ticker;

/**
 * A shared, JVM-wide cache of {@link InMemoryAccumulo} instances keyed by instance name.
 * <p>
 * Every caller that asks for the same name gets the same instance, so separate clients (for example a mock bulk ingest job and the code that reads its results)
 * see the same tables and data. Tests that use named instances should still clean up after themselves with {@link #remove(String)} or {@link #clear()}, but as
 * a safety net an instance is evicted automatically once it has gone {@link #EXPIRE_AFTER_ACCESS} without being looked up. Eviction only drops the cache's
 * reference: callers that still hold the instance keep using it, and the next lookup by that name gets a new, empty instance.
 */
public final class InMemoryAccumuloCache {

    /**
     * How long an instance can go without being looked up before it is evicted. Every lookup resets the clock, and instances are normally shared within one
     * test class or job, so an hour leaves plenty of room while still reclaiming instances that are never removed.
     */
    public static final Duration EXPIRE_AFTER_ACCESS = Duration.ofHours(1);

    private static final Cache<String,InMemoryAccumulo> instances = newCache(EXPIRE_AFTER_ACCESS, Ticker.systemTicker());

    private InMemoryAccumuloCache() {
        throw new UnsupportedOperationException("Utility class");
    }

    /**
     * Builds the backing cache. Package-private so tests can check expiry with their own ticker.
     *
     * @param expireAfterAccess
     *            how long an entry can go without being read or written before it is evicted
     * @param ticker
     *            the time source
     * @return a new, empty cache
     */
    static Cache<String,InMemoryAccumulo> newCache(Duration expireAfterAccess, Ticker ticker) {
        return Caffeine.newBuilder().expireAfterAccess(expireAfterAccess).ticker(ticker).build();
    }

    /**
     * Gets the InMemoryAccumulo instance with the given name from the shared cache, creating and caching a new instance if there is none.
     *
     * @param instanceName
     *            the instance name
     * @return the cached InMemoryAccumulo for the given name
     */
    public static InMemoryAccumulo getOrCreate(String instanceName) {
        return instances.get(instanceName, InMemoryAccumulo::new);
    }

    /**
     * Removes the instance with the given name from the shared cache.
     *
     * @param instanceName
     *            the instance name
     * @return the removed instance, or null if there was none
     */
    public static InMemoryAccumulo remove(String instanceName) {
        return instances.asMap().remove(instanceName);
    }

    /**
     * Removes every instance from the shared cache.
     */
    public static void clear() {
        instances.invalidateAll();
    }

    /**
     * @return the number of cached instances, after any pending evictions have been processed
     */
    public static int size() {
        instances.cleanUp();
        return Math.toIntExact(instances.estimatedSize());
    }
}
