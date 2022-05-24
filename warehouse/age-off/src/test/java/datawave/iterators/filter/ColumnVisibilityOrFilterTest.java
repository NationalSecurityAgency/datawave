package datawave.iterators.filter;

import datawave.iterators.filter.ageoff.AgeOffPeriod;
import datawave.iterators.filter.ageoff.FilterOptions;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test for the {@link ColumnVisibilityOrFilter}
 */
public class ColumnVisibilityOrFilterTest {

    private ColumnVisibilityOrFilter filter;

    private AgeOffPeriod period;

    private Key keyA;
    private Key keyAandB;
    private Key keyAorB;
    private Key keyX;
    private Key keyXorY;

    private Value value = new Value();

    private long start;

    @Before
    public void setup() {
        start = System.currentTimeMillis();

        FilterOptions filterOptions = new FilterOptions();
        filterOptions.setOption(AgeOffConfigParams.MATCHPATTERN, "A");
        filterOptions.setTTL(60L);
        filterOptions.setTTLUnits(AgeOffTtlUnits.MILLISECONDS);

        filter = new ColumnVisibilityOrFilter();
        filter.init(filterOptions);

        period = new AgeOffPeriod(start, 60, "ms");
    }

    private void createKeysWithTimeStamp(long ts) {
        keyA = new Key("row", "cf", "cq", "(A)", ts);
        keyAandB = new Key("row", "cf", "cq", "(A&B)", ts);
        keyAorB = new Key("row", "cf", "cq", "(A|B)", ts);

        keyX = new Key("row", "cf", "cq", "(X)", ts);
        keyXorY = new Key("row", "cf", "cq", "(X|Y)", ts);
    }

    @Test
    public void testKeysWithinAgeOffPeriod() {
        // for keys within the AgeOffPeriod
        createKeysWithTimeStamp(start - 45);

        // rule is applied to key that has the target column visibility and falls within the time range
        assertTrue(filter.accept(period, keyA, value));
        assertTrue(filter.isFilterRuleApplied());

        assertTrue(filter.accept(period, keyAandB, value));
        assertTrue(filter.isFilterRuleApplied());

        assertTrue(filter.accept(period, keyAorB, value));
        assertTrue(filter.isFilterRuleApplied());

        // rule is NOT applied to key that has different column visibility and falls within the time range
        assertTrue(filter.accept(period, keyX, value));
        assertFalse(filter.isFilterRuleApplied());

        assertTrue(filter.accept(period, keyXorY, value));
        assertFalse(filter.isFilterRuleApplied());
    }

    @Test
    public void testKeysOutsideOfAgeOffPeriod() {
        // for keys that fall outside the AgeOffPeriod
        createKeysWithTimeStamp(start - 85);

        // rule is applied to key that has the target column visibility and falls within the time range
        assertFalse(filter.accept(period, keyA, value));
        assertTrue(filter.isFilterRuleApplied());

        assertFalse(filter.accept(period, keyAandB, value));
        assertTrue(filter.isFilterRuleApplied());

        assertFalse(filter.accept(period, keyAorB, value));
        assertTrue(filter.isFilterRuleApplied());

        // rule is NOT applied to key that has different column visibility and falls within the time range
        assertTrue(filter.accept(period, keyX, value));
        assertFalse(filter.isFilterRuleApplied());

        assertTrue(filter.accept(period, keyXorY, value));
        assertFalse(filter.isFilterRuleApplied());
    }

}
