package datawave.iterators.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import datawave.iterators.filter.ageoff.AgeOffPeriod;
import datawave.iterators.filter.ageoff.AppliedRule;
import datawave.iterators.filter.ageoff.FilterOptions;

/**
 * Common methods useful for ColumnVisibilityFilter tests
 */
public class VisibilityFilterTest {

    protected AgeOffPeriod period;
    protected AppliedRule filter;
    protected AppliedRule filterWithCache;

    protected final Value value = new Value();

    /**
     * Create a {@link Key} with the specified visibility and timestamp
     *
     * @param visibility
     *            the visibility
     * @param timestamp
     *            the timestamp
     * @return a Key
     */
    protected Key createKey(String visibility, long timestamp) {
        return new Key("row", "cf", "cq", visibility, timestamp);
    }

    /**
     * Create an {@link AgeOffPeriod} using a starting timestamp and ttl in milliseconds
     *
     * @param start
     *            the starting timestamp
     * @param ttl
     *            the ttl in milliseconds
     */
    protected void createAgeOffPeriod(long start, long ttl) {
        period = new AgeOffPeriod(start, ttl, "ms");
    }

    /**
     * Create a {@link FilterOptions} with a pattern and TTL
     *
     * @param pattern
     *            the pattern
     * @param ttl
     *            the ttl
     * @return an instance of FilterOptions
     */
    protected FilterOptions createOptions(String pattern, long ttl) {
        return createOptions(pattern, ttl, false);
    }

    /**
     * Create a {@link FilterOptions} with a pattern, ttl, and column visibility cache
     *
     * @param pattern
     *            the pattern
     * @param ttl
     *            the ttl
     * @param enableCache
     *            enable the column visibility cache
     * @return an instance of FilterOptions
     */
    protected FilterOptions createOptions(String pattern, long ttl, boolean enableCache) {
        FilterOptions filterOptions = new FilterOptions();
        filterOptions.setOption(AgeOffConfigParams.MATCHPATTERN, pattern);
        filterOptions.setTTL(ttl);
        filterOptions.setTTLUnits(AgeOffTtlUnits.MILLISECONDS);
        filterOptions.setOption(AgeOffConfigParams.COLUMN_VISIBILITY_CACHE_ENABLED, String.valueOf(enableCache));
        return filterOptions;
    }

    /**
     * Asserts that a key is accepted by a filter with and without column visibility caching enabled
     *
     * @param k
     *            a key
     */
    protected void assertKeyAccepted(Key k) {
        assertTrue(filter.accept(period, k, value));
        assertTrue(filterWithCache.accept(period, k, value));
    }

    /**
     * Asserts that a key is rejected by a filter with and without column visibility caching enabled
     *
     * @param k
     *            a key
     */
    protected void assertKeyRejected(Key k) {
        assertFalse(filter.accept(period, k, value));
        assertFalse(filterWithCache.accept(period, k, value));
    }

    /**
     * Asserts that the filter rule is applied (or not), with and without column visibility caching enabled
     *
     * @param state
     *            the expected boolean state
     */
    protected void assertRuleAppliedState(boolean state) {
        assertEquals(state, filter.isFilterRuleApplied());
        assertEquals(state, filterWithCache.isFilterRuleApplied());
    }

    /**
     * Record how long it takes to apply a filter to a list of keys. Default number of iterations is 10,000.
     *
     * @param filter
     *            the filter
     * @param keys
     *            the list of keys
     * @return the time in milliseconds it took to traverse the keys
     */
    protected long throughput(AppliedRule filter, List<Key> keys) {
        return throughput(filter, keys, 1_000);
    }

    /**
     * Record how long it takes to apply a filter to a list of keys for the provided number of iterations
     *
     * @param filter
     *            the filter
     * @param keys
     *            the list of keys
     * @param max
     *            the number of iterations
     * @return the time in milliseconds it took to traverse the keys
     */
    protected long throughput(AppliedRule filter, List<Key> keys, int max) {
        long start = System.currentTimeMillis();
        for (int i = 0; i < max; i++) {
            for (Key key : keys) {
                filter.accept(period, key, value);
            }
        }
        return System.currentTimeMillis() - start;
    }

    /**
     * Build a list of keys using the provided visibility and timestamp. For testing filter throughput.
     *
     * @param size
     *            the number of keys to create
     * @param visibility
     *            the visibility of the keys
     * @param timestamp
     *            the timestamp of the keys
     * @return a list of keys
     */
    protected List<Key> buildListOfSize(int size, String visibility, long timestamp) {
        List<Key> keys = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            keys.add(createKey(visibility, timestamp));
        }
        return keys;
    }

}
