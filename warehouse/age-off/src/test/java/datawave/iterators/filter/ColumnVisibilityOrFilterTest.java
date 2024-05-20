package datawave.iterators.filter;

import org.apache.accumulo.core.data.Key;
import org.junit.Before;
import org.junit.Test;

/**
 * Test for the {@link ColumnVisibilityOrFilter}
 */
public class ColumnVisibilityOrFilterTest extends VisibilityFilterTest {

    private final int max = 5;

    @Before
    public void setup() {
        createAgeOffPeriod(100L, 50L);
        createFilters("A,B", 50L, 25);
    }

    private void createFilters(String pattern, long ttl, int size) {
        filter = new ColumnVisibilityOrFilter();
        filter.init(createOptions(pattern, ttl));

        filterWithCache = new ColumnVisibilityOrFilter();
        filterWithCache.init(createOptions(pattern, ttl, size));
    }

    @Test
    public void testVisibilityFullMatchTimeStampMatch() {
        Key key = createKey("(A|B)", 75L);

        for (int i = 0; i < max; i++) {
            assertKeyAccepted(key);
            assertRuleAppliedState(true);
        }
    }

    @Test
    public void testVisibilityPartialMatchTimeStampMatch() {
        Key key = createKey("(A)", 75L);

        for (int i = 0; i < max; i++) {
            assertKeyAccepted(key);
            assertRuleAppliedState(true);
        }
    }

    @Test
    public void testVisibilityFullMatchTimeStampMiss() {
        Key key = createKey("(A|B)", 25L);

        for (int i = 0; i < max; i++) {
            assertKeyRejected(key);
            assertRuleAppliedState(true);
        }
    }

    @Test
    public void testVisibilityMissTimeStampMatch() {
        Key key = createKey("(X&Y)", 75L);

        for (int i = 0; i < max; i++) {
            assertKeyAccepted(key);
            assertRuleAppliedState(false);
        }
    }

    @Test
    public void testVisibilityMissTimeStampMiss() {
        Key key = createKey("(X&Y)", 25L);

        for (int i = 0; i < max; i++) {
            assertKeyAccepted(key);
            assertRuleAppliedState(false);
        }
    }

    @Test
    public void testFilterWithDifferentTimeStampsAndDifferentCacheSizes() {
        Key first = createKey("(A)", 75L);
        Key second = createKey("(A)", 25L);

        assertKeyAccepted(first);
        assertRuleAppliedState(true);
        assertKeyRejected(second);
        assertRuleAppliedState(true);
    }

}
