package datawave.query.jexl.lookups.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.query.jexl.lookups.cache.FieldNameLookupCache.FieldNameCacheKey;
import datawave.query.jexl.lookups.cache.LookupCache.LookupCacheKey;

public class FieldNameLookupCacheTest extends LookupCacheTest {

    @BeforeEach
    public void setup() {
        super.setup();
    }

    @Test
    public void testRangeExpansionSuccess() {
        createCache();
        LookupCacheKey key = getKey("_ANYFIELD_ == 'bar'");

        recordSuccess(key);
        assertCacheHitsAndMisses(0, 0);

        assertTrue(cache.get(key));
        assertCacheHitsAndMisses(1, 0);
    }

    @Test
    public void testRangeExpansionFailure() {
        createCache();
        LookupCacheKey key = getKey("_ANYFIELD_ == 'bar'");

        recordFailure(key);
        assertCacheHitsAndMisses(0, 0);

        assertFalse(cache.get(key));
        assertCacheHitsAndMisses(1, 0);
    }

    @Test
    public void testRangeExpansionCyclesCache() {
        createCache(1);

        LookupCacheKey keyA = getKey("_ANYFIELD_ == 'foo'");
        LookupCacheKey keyB = getKey("_ANYFIELD_ == 'bar'");
        LookupCacheKey keyC = getKey("_ANYFIELD_ == 'baz'");

        recordFailure(keyA);
        recordFailure(keyB);
        recordFailure(keyC);

        // internal operations are async, flush operations before assertions
        cache.cleanup();

        assertTrue(cache.get(keyA));
        assertTrue(cache.get(keyB));
        assertFalse(cache.get(keyC));
        assertCacheHitsAndMisses(1, 2);
    }

    @Test
    public void testRepeatedHitsAndMisses() {
        createCache();

        LookupCacheKey keyA = getKey("_ANYFIELD_ == 'bar'");
        LookupCacheKey keyB = getKey("_ANYFIELD_ == 'baz'");

        recordSuccess(keyA);
        cache.cleanup();

        int max = 15;
        for (int i = 0; i < max; i++) {
            assertTrue(cache.get(keyA));
            assertTrue(cache.get(keyB));
        }

        assertCacheHitsAndMisses(15, 15);
    }

    @Test
    public void testCacheKeyEquality() {
        LookupCacheKey left = getKey("_ANYFIELD_ == 'bar'");
        LookupCacheKey right = getKey("_ANYFIELD_ == 'bar'");
        assertEquals(left, right);
    }

    @Override
    protected void createCache(int size) {
        cache = new FieldNameLookupCache(size, 1, 1);
    }

    /**
     * Create a cache key with default values
     *
     * @param nodeKey
     *            the Jexl node string
     * @return the cache key
     */
    @Override
    protected LookupCacheKey getKey(String nodeKey) {
        return new FieldNameCacheKey(nodeKey, "20241120", "20241125", Set.of("type-a", "type-b"));
    }
}
