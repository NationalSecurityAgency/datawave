package datawave.iterators.filter;

import datawave.iterators.filter.ageoff.AgeOffPeriod;
import datawave.iterators.filter.ageoff.FilterOptions;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test for the {@link ColumnVisibilityOrFilter}
 */
public class ColumnVisibilityOrFilterTest {

    private ColumnVisibilityOrFilter filter;

    private AgeOffPeriod period;
    private final Value value = new Value();
    private long start;

    private Key keyA;
    private Key keyAandB;
    private Key keyAorB;
    private Key keyX;
    private Key keyXorY;

    @Before
    public void setup() {
        start = System.currentTimeMillis();
        period = new AgeOffPeriod(start, 60, "ms");
    }

    private void createKeysWithTimeStamp(long ts) {
        keyA = createKey("(A)", ts);
        keyAandB = createKey("(A&B)", ts);
        keyAorB = createKey("(A|B)", ts);

        keyX = createKey("(X)", ts);
        keyXorY = createKey("(X|Y)", ts);
    }

    private Key createKey(String viz, long ts) {
        return new Key("row", "cf", "cq", viz, ts);
    }

    private void createFilter(String pattern, long ttl) {
        FilterOptions filterOptions = new FilterOptions();
        filterOptions.setOption(AgeOffConfigParams.MATCHPATTERN, pattern);
        filterOptions.setTTL(ttl);
        filterOptions.setTTLUnits(AgeOffTtlUnits.MILLISECONDS);

        filter = new ColumnVisibilityOrFilter();
        filter.init(filterOptions);
    }

    @Test
    public void testKeysWithinAgeOffPeriod() {
        // for keys within the AgeOffPeriod
        createKeysWithTimeStamp(start - 45);
        createFilter("A", 60);

        // rule is applied to key that has the target column visibility and falls within the time range
        assertKeyAccepted(keyA);
        assertRuleAppliedState(true);

        assertKeyAccepted(keyAandB);
        assertRuleAppliedState(true);

        assertKeyAccepted(keyAorB);
        assertRuleAppliedState(true);

        // rule is NOT applied to key that has different column visibility and falls within the time range
        assertKeyAccepted(keyX);
        assertRuleAppliedState(false);

        assertKeyAccepted(keyXorY);
        assertRuleAppliedState(false);
    }

    @Test
    public void testKeysOutsideOfAgeOffPeriod() {
        // for keys that fall outside the AgeOffPeriod
        createKeysWithTimeStamp(start - 85);
        createFilter("A", 60);

        // rule is applied to key that has the target column visibility and falls within the time range
        assertKeyRejected(keyA);
        assertRuleAppliedState(true);

        assertKeyRejected(keyAandB);
        assertRuleAppliedState(true);

        assertKeyRejected(keyAorB);
        assertRuleAppliedState(true);

        // rule is NOT applied to key that has different column visibility and falls within the time range
        assertKeyAccepted(keyX);
        assertRuleAppliedState(false);

        assertKeyAccepted(keyXorY);
        assertRuleAppliedState(false);
    }

    @Test
    public void testMultiFilterWithinAgeOffPeriod() {
        // for keys within the AgeOffPeriod
        createKeysWithTimeStamp(start - 45);
        createFilter("A,Y", 60);

        // rule is applied to key that has the target column visibility and falls within the time range
        assertKeyAccepted(keyA);
        assertRuleAppliedState(true);

        assertKeyAccepted(keyAandB);
        assertRuleAppliedState(true);

        assertKeyAccepted(keyAorB);
        assertRuleAppliedState(true);

        // rule is NOT applied to key that has different column visibility and falls within the time range
        assertKeyAccepted(keyX);
        assertRuleAppliedState(false);

        assertKeyAccepted(keyXorY);
        assertRuleAppliedState(true); // Y portion matches
    }

    private void assertKeyAccepted(Key k) {
        assertTrue(filter.accept(period, k, value));
    }

    private void assertKeyRejected(Key k) {
        assertFalse(filter.accept(period, k, value));
    }

    private void assertRuleAppliedState(boolean state) {
        assertEquals(state, filter.isFilterRuleApplied());
    }

}
