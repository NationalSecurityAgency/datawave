package datawave.iterators.filter;

import java.util.List;

import org.apache.accumulo.core.data.Key;
import org.junit.Before;
import org.junit.Test;

/**
 * Test for the {@link ColumnVisibilityAndFilter}
 */
public class ColumnVisibilityAndFilterTest extends VisibilityFilterTest {

    private final int max = 5;

    @Before
    public void setup() {
        createAgeOffPeriod(100L, 50L);
        createFilters("A,B", 50L, 25);
    }

    private void createFilters(String pattern, long ttl, int size) {
        filter = new ColumnVisibilityAndFilter();
        filter.init(createOptions(pattern, ttl));

        filterWithCache = new ColumnVisibilityAndFilter();
        filterWithCache.init(createOptions(pattern, ttl, size));
    }

    @Test
    public void testVisibilityFullMatchTimeStampMatch() {
        Key key = createKey("(A&B)", 75L);

        for (int i = 0; i < max; i++) {
            assertKeyAccepted(key);
            assertRuleAppliedState(true);
        }
    }

    @Test
    public void testVisibilityPartialMatchTimeStampMatch() {
        Key key = createKey("(A|C)", 75L);

        for (int i = 0; i < max; i++) {
            assertKeyAccepted(key);
            assertRuleAppliedState(false);
        }
    }

    @Test
    public void testVisibilityFullMatchTimeStampMiss() {
        Key key = createKey("(A&B)", 25L);

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
}
