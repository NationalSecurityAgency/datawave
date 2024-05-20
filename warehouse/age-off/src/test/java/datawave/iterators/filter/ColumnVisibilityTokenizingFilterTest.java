package datawave.iterators.filter;

import org.apache.accumulo.core.data.Key;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

public class ColumnVisibilityTokenizingFilterTest extends VisibilityFilterTest {

    private final static Logger log = Logger.getLogger(ColumnVisibilityTokenizingFilterTest.class);
    private final int max = 5;

    @Before
    public void before() {
        createAgeOffPeriod(100L, 50L);
        createFilters("\"A\": 50ms", 50L);
    }

    private void createFilters(String pattern, long ttl) {
        filter = new ColumnVisibilityTokenizingFilter();
        filter.init(createOptions(pattern, ttl));

        filterWithCache = new ColumnVisibilityTokenizingFilter();
        filterWithCache.init(createOptions(pattern, ttl, true));
    }

    @Test
    public void testVisibilityFullMatchTimeStampMatch() {
        Key key = createKey("(A)", 75L);

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
            assertRuleAppliedState(true);
        }
    }

    @Test
    public void testVisibilityFullMatchTimeStampMiss() {
        Key key = createKey("(A)", 25L);

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
    public void testThroughput() {
        createFilters("\"A\": 50ms", 50L);

        long time = throughput(filter, buildListOfSize(500, "(A&B)", 75L));
        long cacheTime = throughput(filterWithCache, buildListOfSize(500, "(A&B)", 75L));
        log.info("viz match, time match (orig) : " + time);
        log.info("viz match, time match (cache): " + cacheTime);

        createFilters("\"A\": 50ms", 50L);
        time = throughput(filter, buildListOfSize(500, "(A&B)", 75L));
        cacheTime = throughput(filterWithCache, buildListOfSize(500, "(A&B)", 75L));
        log.info("viz match, time match (orig) : " + time);
        log.info("viz match, time match (cache): " + cacheTime);

        createFilters("\"A\": 50ms", 50L);
        time = throughput(filter, buildListOfSize(500, "(C&D&E)", 75L));
        cacheTime = throughput(filterWithCache, buildListOfSize(500, "(A&B)", 75L));
        log.info("viz match, time match (orig) : " + time);
        log.info("viz match, time match (cache): " + cacheTime);

        createFilters("\"A\": 50ms", 50L);
        time = throughput(filter, buildListOfSize(500, "(C&D&E)", 75L));
        cacheTime = throughput(filterWithCache, buildListOfSize(500, "(A&B)", 75L));
        log.info("viz match, time match (orig) : " + time);
        log.info("viz match, time match (cache): " + cacheTime);
    }
}
