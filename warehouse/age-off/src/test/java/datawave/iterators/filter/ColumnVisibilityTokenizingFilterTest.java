package datawave.iterators.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.Before;
import org.junit.Test;

import datawave.iterators.filter.ageoff.AgeOffPeriod;
import datawave.iterators.filter.ageoff.FilterOptions;

public class ColumnVisibilityTokenizingFilterTest {

    private ColumnVisibilityTokenizingFilter filter;
    private ColumnVisibilityTokenizingFilter filterWithCache;
    private AgeOffPeriod period;
    private final Value value = new Value();

    @Before
    public void before() {
        period = new AgeOffPeriod(100L, 50L, "ms");
    }

    private Key createKey(String viz, long ts) {
        return new Key("row", "cf", "cq", viz, ts);
    }

    private void createFilter(String pattern, long ttl) {
        FilterOptions options = new FilterOptions();
        options.setOption(AgeOffConfigParams.MATCHPATTERN, pattern);
        options.setTTL(ttl);
        options.setTTLUnits(AgeOffTtlUnits.MILLISECONDS);

        filter = new ColumnVisibilityTokenizingFilter();
        filter.init(options);
    }

    private void createFilter(String pattern, long ttl, int size) {
        FilterOptions options = new FilterOptions();
        options.setOption(AgeOffConfigParams.MATCHPATTERN, pattern);
        options.setTTL(ttl);
        options.setTTLUnits(AgeOffTtlUnits.MILLISECONDS);
        options.setOption(AgeOffConfigParams.COLUMN_VISIBILITY_CACHE_SIZE, String.valueOf(size));

        filterWithCache = new ColumnVisibilityTokenizingFilter();
        filterWithCache.init(options);
    }

    @Test
    public void testMatchingVisibilityAndTimeStamp() {
        Key key = createKey("(A)", 75L);
        createFilter("\"A\": 50ms", 50L);
        createFilter("\"A\": 50ms", 50L, 25);

        assertKeyAccepted(key);
        assertRuleAppliedState(true);
    }

    @Test
    public void testMatchingVisibilityAndExcludeTimeStamp() {
        Key key = createKey("(A)", 25L);
        createFilter("\"A\": 50ms", 50L);
        createFilter("\"A\": 50ms", 50L, 25);

        assertKeyRejected(key);
        assertRuleAppliedState(true);
    }

    @Test
    public void testExclusionVisibilityAndMatchingTimeStamp() {
        Key key = createKey("(X)", 75L);
        createFilter("\"A\": 50ms", 50L);
        createFilter("\"A\": 50ms", 50L, 25);

        assertKeyAccepted(key);
        assertRuleAppliedState(false);
    }

    @Test
    public void testExclusionVisibilityAndTimeStamp() {
        Key key = createKey("(X)", 25L);
        createFilter("\"A\": 50ms", 50L);
        createFilter("\"A\": 50ms", 50L, 25);

        assertKeyAccepted(key);
        assertRuleAppliedState(false);
    }

    private void assertKeyAccepted(Key key) {
        assertTrue(filter.accept(period, key, value));
        assertTrue(filterWithCache.accept(period, key, value));
    }

    private void assertKeyRejected(Key key) {
        assertFalse(filter.accept(period, key, value));
        assertFalse(filterWithCache.accept(period, key, value));
    }

    private void assertRuleAppliedState(boolean state) {
        assertEquals(state, filter.isFilterRuleApplied());
        assertEquals(state, filterWithCache.isFilterRuleApplied());
    }

}
