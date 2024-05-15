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
 * Test for the {@link ColumnVisibilityAndFilter}
 */
public class ColumnVisibilityAndFilterTest {

    private ColumnVisibilityAndFilter filter;
    private AgeOffPeriod period;

    private final Value value = new Value();

    private long start;

    private Key a;
    private Key b;
    private Key c;
    private Key AandB;
    private Key BandC;

    private Key AorBandBorC;

    @Before
    public void setup() {
        start = System.currentTimeMillis();
        period = new AgeOffPeriod(start, 60, "ms");
    }

    private void createKeysWithTimeStamp(long ts) {
        a = createKey("(A)", ts);
        b = createKey("(B)", ts);
        c = createKey("(C)", ts);

        AandB = createKey("(A&B)", ts);
        BandC = createKey("(B&C)", ts);

        AorBandBorC = createKey("(A|B)&(B|C)", ts);
    }

    private Key createKey(String viz, long ts) {
        return new Key("row", "cf", "cq", viz, ts);
    }

    private void createFilter(String pattern, long ttl) {
        FilterOptions filterOptions = new FilterOptions();
        filterOptions.setOption(AgeOffConfigParams.MATCHPATTERN, pattern);
        filterOptions.setTTL(ttl);
        filterOptions.setTTLUnits(AgeOffTtlUnits.MILLISECONDS);

        filter = new ColumnVisibilityAndFilter();
        filter.init(filterOptions);
    }

    @Test
    public void testSingleMatchPattern() {
        createFilter("A", 60);
        createKeysWithTimeStamp(start - 45);

        assertKeyAccepted(a);
        assertRuleAppliedState(true);

        assertKeyAccepted(b);
        assertRuleAppliedState(false);

        assertKeyAccepted(c);
        assertRuleAppliedState(false);

        assertKeyAccepted(AandB);
        assertRuleAppliedState(true);

        assertKeyAccepted(BandC);
        assertRuleAppliedState(false);

        assertKeyAccepted(AorBandBorC);
        assertRuleAppliedState(true);
    }

    @Test
    public void testMultiPattern() {
        createFilter("A,B", 60);
        createKeysWithTimeStamp(start - 45);

        assertKeyAccepted(a);
        assertRuleAppliedState(false);

        assertKeyAccepted(b);
        assertRuleAppliedState(false);

        assertKeyAccepted(c);
        assertRuleAppliedState(false);

        assertKeyAccepted(AandB);
        assertRuleAppliedState(true);

        assertKeyAccepted(BandC);
        assertRuleAppliedState(false);

        assertKeyAccepted(AorBandBorC);
        assertRuleAppliedState(true);
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
