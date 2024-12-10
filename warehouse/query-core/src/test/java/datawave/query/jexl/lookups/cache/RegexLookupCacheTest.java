package datawave.query.jexl.lookups.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.query.jexl.lookups.cache.LookupFailureCache.LookupCacheKey;
import datawave.query.jexl.lookups.cache.RegexLookupCache.RegexCacheKey;

public class RegexLookupCacheTest extends LookupFailureCacheTest {

    @BeforeEach
    public void setup() {
        super.setup();
    }

    @Test
    public void testRegexExpansionSuccess() {
        createCache();
        LookupCacheKey key = getKey("FIELD =~ 'aa.*'");

        assertCacheHitsAndMisses(0, 0);

        assertFalse(cache.lookupFailed(key));
        assertCacheHitsAndMisses(0, 1);
    }

    @Test
    public void testRegexExpansionFailure() {
        createCache();
        LookupCacheKey key = getKey("FIELD =~ 'aa.*'");

        recordFailure(key);
        assertCacheHitsAndMisses(0, 0);

        assertTrue(cache.lookupFailed(key));
        assertCacheHitsAndMisses(1, 0);
    }

    @Test
    public void testRegexExpansionCyclesCache() {
        createCache(1);
        LookupCacheKey keyA = getKey("FIELD =~ 'aa.*'");
        LookupCacheKey keyB = getKey("FIELD =~ 'bb.*'");
        LookupCacheKey keyC = getKey("FIELD =~ 'cc.*'");

        recordFailure(keyA);
        recordFailure(keyB);
        recordFailure(keyC);

        cache.cleanup(); // flush cache operations

        assertFalse(cache.lookupFailed(keyA));
        assertFalse(cache.lookupFailed(keyB));
        assertTrue(cache.lookupFailed(keyC));

        assertCacheHitsAndMisses(1, 2);
    }

    @Test
    public void testRepeatedHitsAndMisses() {
        createCache();

        LookupCacheKey keyA = getKey("FIELD =~ 'aa.*'");
        LookupCacheKey keyB = getKey("FIELD =~ 'bb.*'");

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
        LookupCacheKey left = getKey("FIELD =~ 'aa.*'");
        LookupCacheKey right = getKey("FIELD =~ 'aa.*'");
        assertEquals(left, right);
    }

    @Override
    protected void createCache(int size) {
        cache = new RegexLookupCache(size, 1, 1);
    }

    @Override
    protected LookupCacheKey getKey(String regex) {
        return new RegexCacheKey(regex, false, "20241120", "20241125", Set.of("type-a", "type-b"));
    }
}
