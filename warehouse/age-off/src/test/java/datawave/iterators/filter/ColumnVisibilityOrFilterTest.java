package datawave.iterators.filter;

import org.apache.accumulo.core.data.Key;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

/**
 * Test for the {@link ColumnVisibilityOrFilter}
 */
public class ColumnVisibilityOrFilterTest extends VisibilityFilterTest {

    private final static Logger log = Logger.getLogger(ColumnVisibilityOrFilterTest.class);
    private final int max = 5;

    @Before
    public void setup() {
        createAgeOffPeriod(100L, 50L);
        createFilters("A,B", 50L);
    }

    private void createFilters(String pattern, long ttl) {
        filter = new ColumnVisibilityOrFilter();
        filter.init(createOptions(pattern, ttl));

        filterWithCache = new ColumnVisibilityOrFilter();
        filterWithCache.init(createOptions(pattern, ttl, true));
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

    @Test
    public void testThroughput() {
        createFilters("A,B", 50L);
        long time = throughput(filter, buildListOfSize(500, "(A&B)", 75L));
        long cacheTime = throughput(filterWithCache, buildListOfSize(500, "(A&B)", 75L));
        log.info("viz match, time match (orig) : " + time);
        log.info("viz match, time match (cache): " + cacheTime);

        createFilters("A,B", 50);
        time = throughput(filter, buildListOfSize(500, "(A&B)", 75L));
        cacheTime = throughput(filterWithCache, buildListOfSize(500, "(A&B)", 75L));
        log.info("viz match, time match (orig) : " + time);
        log.info("viz match, time match (cache): " + cacheTime);

        createFilters("A,B", 50L);
        time = throughput(filter, buildListOfSize(500, "(C&D&E)", 75L));
        cacheTime = throughput(filterWithCache, buildListOfSize(500, "(A&B)", 75L));
        log.info("viz match, time match (orig) : " + time);
        log.info("viz match, time match (cache): " + cacheTime);

        createFilters("A,B", 50L);
        time = throughput(filter, buildListOfSize(500, "(C&D&E)", 75L));
        cacheTime = throughput(filterWithCache, buildListOfSize(500, "(A&B)", 75L));
        log.info("viz match, time match (orig) : " + time);
        log.info("viz match, time match (cache): " + cacheTime);
    }
}
