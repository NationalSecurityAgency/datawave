package datawave.query.jexl.lookups.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.query.jexl.lookups.cache.BoundedRangeLookupCache.BoundedRangeCacheKey;
import datawave.query.jexl.lookups.cache.LookupFailureCache.LookupCacheKey;

public class BoundedRangeLookupCacheTest extends LookupFailureCacheTest {

    @BeforeEach
    public void setup() {
        super.setup();
    }

    @Test
    public void testRangeExpansionSuccess() {
        createCache();
        LookupCacheKey key = getKey("F > 1 && F < 2");

        assertCacheHitsAndMisses(0, 0);

        assertFalse(cache.lookupFailed(key));
        assertCacheHitsAndMisses(0, 1);
    }

    @Test
    public void testRangeExpansionFailure() {
        createCache();
        LookupCacheKey key = getKey("F > 1 && F < 2");

        recordFailure(key);
        assertCacheHitsAndMisses(0, 0);

        assertTrue(cache.lookupFailed(key));
        assertCacheHitsAndMisses(1, 0);
    }

    @Test
    public void testRangeExpansionCyclesCache() {
        createCache(1);

        LookupCacheKey keyA = getKey("F > 1 && F < 2");
        LookupCacheKey keyB = getKey("F > 2 && F < 3");
        LookupCacheKey keyC = getKey("F > 3 && F < 4");

        recordFailure(keyA);
        recordFailure(keyB);
        recordFailure(keyC);

        // internal operations are async, flush operations before assertions
        cache.cleanup();

        assertFalse(cache.lookupFailed(keyA));
        assertFalse(cache.lookupFailed(keyB));
        assertTrue(cache.lookupFailed(keyC));
        assertCacheHitsAndMisses(1, 2);
    }

    @Test
    public void testRepeatedHitsAndMisses() {
        createCache();

        LookupCacheKey keyA = getKey("F > 1 && F < 2");
        LookupCacheKey keyB = getKey("F > 2 && F < 3");

        cache.cleanup();

        int max = 15;
        for (int i = 0; i < max; i++) {
            // key A always fails to expand and thus records a failure
            // key B always expands
            cache.recordFailure(keyA);

            assertTrue(cache.lookupFailed(keyA));
            assertFalse(cache.lookupFailed(keyB));
        }

        assertCacheHitsAndMisses(15, 15);
    }

    @Test
    public void testCacheKeyEquality() {
        LookupCacheKey left = getKey("F > 1 && F < 2");
        LookupCacheKey right = getKey("F > 1 && F < 2");
        assertEquals(left, right);
    }

    protected void createCache(int size) {
        cache = new BoundedRangeLookupCache(size, 1, 1);
    }

    /**
     * Create a cache key with default values
     *
     * @param range
     *            a literal range
     * @return the cache key
     */
    protected LookupCacheKey getKey(String range) {
        return new BoundedRangeCacheKey(range, "20241120", "20241125", Set.of("type-a", "type-b"));
    }

}
