package datawave.query.jexl.lookups.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.BeforeEach;

import datawave.query.jexl.lookups.cache.LookupFailureCache.LookupCacheKey;

/**
 * Base class for {@link LookupFailureCache} tests. Extending classes must provide their own implementation of cache and key creation
 */
public abstract class LookupFailureCacheTest {

    private static final int DEFAULT_CACHE_SIZE = 5;

    protected LookupFailureCache cache;

    @BeforeEach
    public void setup() {
        cache = null; // ensure fresh cache for each test
    }

    protected void createCache() {
        createCache(DEFAULT_CACHE_SIZE);
    }

    protected abstract void createCache(int size);

    protected abstract LookupCacheKey getKey(String nodeKey);

    protected void recordFailure(LookupCacheKey key) {
        cache.recordFailure(key);
    }

    protected void assertCacheHitsAndMisses(int expectedHits, int expectedMisses) {
        assertEquals(expectedHits, cache.stats().hitCount(), "hits did not match expectation");
        assertEquals(expectedMisses, cache.stats().missCount(), "misses did not match expectation");
        cache.logStats();
    }
}
