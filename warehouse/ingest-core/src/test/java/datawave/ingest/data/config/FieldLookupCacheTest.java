package datawave.ingest.data.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import datawave.ingest.data.config.FieldLookupCache.OverflowPolicy;

class FieldLookupCacheTest {

    private static final String DATA_TYPE_NAME = "test";
    private static final String PREFIX = ".data.field.type.cache";
    private static final String MAX_SIZE_PROPERTY = DATA_TYPE_NAME + PREFIX + FieldLookupCache.MAX_SIZE_SUFFIX;
    private static final String OVERFLOW_POLICY_PROPERTY = DATA_TYPE_NAME + PREFIX + FieldLookupCache.OVERFLOW_POLICY_SUFFIX;
    private static final String ALL_MAX_SIZE_PROPERTY = "all" + PREFIX + FieldLookupCache.MAX_SIZE_SUFFIX;
    private static final String ALL_OVERFLOW_POLICY_PROPERTY = "all" + PREFIX + FieldLookupCache.OVERFLOW_POLICY_SUFFIX;

    private Configuration config;

    @BeforeEach
    void setUp() {
        config = new Configuration(false);
    }

    /**
     * A mapping function that counts how many times it was invoked, so a test can assert a hit never calls it.
     */
    private static final class CountingFunction implements Function<String,String> {
        int calls;

        @Override
        public String apply(String key) {
            calls++;
            return key + "-resolved";
        }
    }

    @Nested
    class UnboundedTests {

        /**
         * Verify that the default constructor never evicts, however many distinct keys are cached.
         */
        @Test
        void neverEvicts() {
            FieldLookupCache<String,String> cache = new FieldLookupCache<>();

            for (int i = 0; i < 500; i++) {
                cache.computeIfAbsent("KEY_" + i, k -> k + "-value");
            }

            assertEquals(500, cache.size());
            assertEquals(FieldLookupCache.UNBOUNDED, cache.getMaxSize());
        }

        /**
         * Verify that size grows by one per distinct key and is unaffected by repeat lookups of the same key.
         */
        @Test
        void sizeGrowsWithDistinctKeys() {
            FieldLookupCache<String,String> cache = new FieldLookupCache<>();

            cache.computeIfAbsent("A", k -> "a");
            cache.computeIfAbsent("A", k -> "a-again");
            cache.computeIfAbsent("B", k -> "b");

            assertEquals(2, cache.size());
        }
    }

    /**
     * Verify that a hit never invokes the mapping function, regardless of overflow policy.
     */
    @Test
    void hitNeverCallsTheMappingFunction() {
        FieldLookupCache<String,String> cache = new FieldLookupCache<>(2, OverflowPolicy.BYPASS);
        CountingFunction fn = new CountingFunction();

        cache.computeIfAbsent("A", fn);
        cache.computeIfAbsent("A", fn);
        cache.computeIfAbsent("A", fn);

        assertEquals(1, fn.calls);
    }

    @Nested
    class BypassTests {

        /**
         * Verify that once the cache is full, a miss resolves and returns the value without growing the cache, size stays at the bound, the originally cached
         * keys remain, and the mapping function is invoked again the next time the bypassed key is looked up.
         */
        @Test
        void bypassesWithoutGrowingPastBound() {
            FieldLookupCache<String,String> cache = new FieldLookupCache<>(2, OverflowPolicy.BYPASS);
            CountingFunction fn = new CountingFunction();

            cache.computeIfAbsent("A", fn);
            cache.computeIfAbsent("B", fn);
            assertEquals(2, cache.size());

            String value = cache.computeIfAbsent("C", fn);
            assertEquals("C-resolved", value);
            assertEquals(2, cache.size());
            assertTrue(cache.asMap().containsKey("A"));
            assertTrue(cache.asMap().containsKey("B"));
            assertFalse(cache.asMap().containsKey("C"));

            int callsBefore = fn.calls;
            cache.computeIfAbsent("C", fn);
            assertEquals(callsBefore + 1, fn.calls, "a bypassed key must be resolved again on its next lookup");
        }
    }

    @Nested
    class ClearTests {

        /**
         * Verify that once the cache is full, a miss clears it and stores only the new entry.
         */
        @Test
        void clearsAndStoresTheNewEntry() {
            FieldLookupCache<String,String> cache = new FieldLookupCache<>(2, OverflowPolicy.CLEAR);

            cache.computeIfAbsent("A", k -> "a");
            cache.computeIfAbsent("B", k -> "b");
            assertEquals(2, cache.size());

            cache.computeIfAbsent("C", k -> "c");

            assertEquals(1, cache.size());
            assertTrue(cache.asMap().containsKey("C"));
            assertFalse(cache.asMap().containsKey("A"));
            assertFalse(cache.asMap().containsKey("B"));
        }
    }

    @Nested
    class ConstructorTests {

        /**
         * Verify that a non-positive size is rejected by the two argument constructor.
         */
        @Test
        void nonPositiveMaxSizeThrows() {
            assertThrows(IllegalArgumentException.class, () -> new FieldLookupCache<String,String>(0, OverflowPolicy.BYPASS));
            assertThrows(IllegalArgumentException.class, () -> new FieldLookupCache<String,String>(-1, OverflowPolicy.CLEAR));
        }
    }

    @Nested
    class ParseTests {

        /**
         * Verify that an unconfigured datatype gets the historical unbounded cache.
         */
        @Test
        void givenNothingSetThenUnbounded() {
            FieldLookupCache<String,String> cache = FieldLookupCache.parse(config, DATA_TYPE_NAME, PREFIX);

            assertEquals(FieldLookupCache.UNBOUNDED, cache.getMaxSize());
        }

        /**
         * Verify that setting only the size bounds the cache and defaults the overflow policy to BYPASS.
         */
        @Test
        void givenOnlyMaxSizeThenPolicyDefaultsToBypass() {
            config.set(MAX_SIZE_PROPERTY, "5");

            FieldLookupCache<String,String> cache = FieldLookupCache.parse(config, DATA_TYPE_NAME, PREFIX);

            assertEquals(5, cache.getMaxSize());
            assertEquals(OverflowPolicy.BYPASS, cache.getOverflowPolicy());
        }

        /**
         * Verify that the datatype specific max size wins over the {@code all} one.
         */
        @Test
        void givenDataTypeAndAllMaxSizeThenDataTypeWins() {
            config.set(ALL_MAX_SIZE_PROPERTY, "10");
            config.set(MAX_SIZE_PROPERTY, "20");

            FieldLookupCache<String,String> cache = FieldLookupCache.parse(config, DATA_TYPE_NAME, PREFIX);

            assertEquals(20, cache.getMaxSize());
        }

        /**
         * Verify that the {@code all} max size applies when the datatype declares none of its own.
         */
        @Test
        void givenOnlyAllMaxSizeThenItApplies() {
            config.set(ALL_MAX_SIZE_PROPERTY, "10");

            FieldLookupCache<String,String> cache = FieldLookupCache.parse(config, DATA_TYPE_NAME, PREFIX);

            assertEquals(10, cache.getMaxSize());
        }

        /**
         * Verify that the datatype specific overflow policy wins over the {@code all} one.
         */
        @Test
        void givenDataTypeAndAllOverflowPolicyThenDataTypeWins() {
            config.set(MAX_SIZE_PROPERTY, "5");
            config.set(ALL_OVERFLOW_POLICY_PROPERTY, "BYPASS");
            config.set(OVERFLOW_POLICY_PROPERTY, "CLEAR");

            FieldLookupCache<String,String> cache = FieldLookupCache.parse(config, DATA_TYPE_NAME, PREFIX);

            assertEquals(OverflowPolicy.CLEAR, cache.getOverflowPolicy());
        }

        /**
         * Verify that the {@code all} overflow policy applies when the datatype declares none of its own.
         */
        @Test
        void givenOnlyAllOverflowPolicyThenItApplies() {
            config.set(MAX_SIZE_PROPERTY, "5");
            config.set(ALL_OVERFLOW_POLICY_PROPERTY, "CLEAR");

            FieldLookupCache<String,String> cache = FieldLookupCache.parse(config, DATA_TYPE_NAME, PREFIX);

            assertEquals(OverflowPolicy.CLEAR, cache.getOverflowPolicy());
        }

        /**
         * Verify that the max size and overflow policy resolve independently, so {@code all} can set one while the datatype sets the other.
         */
        @Test
        void maxSizeAndOverflowPolicyResolveIndependently() {
            config.set(ALL_MAX_SIZE_PROPERTY, "5");
            config.set(OVERFLOW_POLICY_PROPERTY, "CLEAR");

            FieldLookupCache<String,String> cache = FieldLookupCache.parse(config, DATA_TYPE_NAME, PREFIX);

            assertEquals(5, cache.getMaxSize());
            assertEquals(OverflowPolicy.CLEAR, cache.getOverflowPolicy());
        }

        /**
         * Verify that overflow policy parsing tolerates surrounding whitespace and any letter case.
         */
        @Test
        void overflowPolicyParsingIsCaseAndWhitespaceInsensitive() {
            config.set(MAX_SIZE_PROPERTY, "5");
            config.set(OVERFLOW_POLICY_PROPERTY, " clear ");

            FieldLookupCache<String,String> cache = FieldLookupCache.parse(config, DATA_TYPE_NAME, PREFIX);

            assertEquals(OverflowPolicy.CLEAR, cache.getOverflowPolicy());
        }

        /**
         * Verify that a non-integer max size fails, naming the property that carried it.
         */
        @Test
        void nonIntegerMaxSizeThrows() {
            config.set(MAX_SIZE_PROPERTY, "many");

            IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> FieldLookupCache.parse(config, DATA_TYPE_NAME, PREFIX));
            assertTrue(e.getMessage().contains(MAX_SIZE_PROPERTY), e.getMessage());
        }

        /**
         * Verify that a non-positive max size fails, naming the property that carried it.
         */
        @Test
        void nonPositiveMaxSizeThrows() {
            config.set(MAX_SIZE_PROPERTY, "0");

            IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> FieldLookupCache.parse(config, DATA_TYPE_NAME, PREFIX));
            assertTrue(e.getMessage().contains(MAX_SIZE_PROPERTY), e.getMessage());
        }

        /**
         * Verify that an unrecognized overflow policy fails, naming the property that carried it.
         */
        @Test
        void unknownOverflowPolicyThrows() {
            config.set(MAX_SIZE_PROPERTY, "5");
            config.set(OVERFLOW_POLICY_PROPERTY, "BOGUS");

            IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> FieldLookupCache.parse(config, DATA_TYPE_NAME, PREFIX));
            assertTrue(e.getMessage().contains(OVERFLOW_POLICY_PROPERTY), e.getMessage());
            assertTrue(e.getMessage().contains("BOGUS"), e.getMessage());
        }

        /**
         * Verify that a bad max size under {@code all} is reported against the {@code all} property, not the datatype one.
         */
        @Test
        void badAllMaxSizeReportsAllProperty() {
            config.set(ALL_MAX_SIZE_PROPERTY, "many");

            IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> FieldLookupCache.parse(config, DATA_TYPE_NAME, PREFIX));
            assertTrue(e.getMessage().contains(ALL_MAX_SIZE_PROPERTY), e.getMessage());
        }
    }

    /**
     * Verify that {@link FieldLookupCache#asMap()} reflects the cache contents but cannot be used to mutate it.
     */
    @Test
    void asMapIsAnUnmodifiableView() {
        FieldLookupCache<String,String> cache = new FieldLookupCache<>();
        cache.computeIfAbsent("A", k -> "a");

        Set<String> keys = new HashSet<>(cache.asMap().keySet());
        assertEquals(Set.of("A"), keys);
        assertThrows(UnsupportedOperationException.class, () -> cache.asMap().put("B", "b"));
    }
}
