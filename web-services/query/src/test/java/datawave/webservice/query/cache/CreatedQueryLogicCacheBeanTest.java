package datawave.webservice.query.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.Sets;

import datawave.core.query.logic.QueryLogic;
import datawave.webservice.query.cache.CreatedQueryLogicCacheBean.Triple;

public class CreatedQueryLogicCacheBeanTest {

    protected CreatedQueryLogicCacheBean qlCache = null;
    protected ConcurrentHashMap<Pair<String,Long>,Triple> internalCache = null;
    protected QueryLogic<?> queryLogic;
    protected AccumuloClient client;

    private final String USER_A = "userA";

    private final String QUERY_ID_A = UUID.randomUUID().toString();
    private final String QUERY_ID_B = UUID.randomUUID().toString();
    private final String QUERY_ID_C = UUID.randomUUID().toString();

    // control time of cache operations
    private final Clock mockClock = mock(Clock.class);

    @BeforeEach
    public void beforeEach() throws IllegalAccessException, SecurityException, NoSuchMethodException {
        qlCache = new CreatedQueryLogicCacheBean();
        queryLogic = mock(QueryLogic.class);
        client = mock(AccumuloClient.class);

        // set the mock clock on the cache to control timing of cache operations
        qlCache.setClock(mockClock);

        // inject the map so the test can verify internal state without a reflection call per test
        internalCache = new ConcurrentHashMap<>();
        ReflectionTestUtils.setField(qlCache, "cache", internalCache);
    }

    @Test
    public void testFirstInsertion() {
        boolean value = qlCache.add(USER_A, QUERY_ID_A, queryLogic, client);
        assertFirstCacheAdd(value);
    }

    @Test
    public void testDuplicateKeyInsertion() {
        setClockTime(1L);
        boolean value = qlCache.add(QUERY_ID_A, USER_A, queryLogic, client);
        assertFirstCacheAdd(value);
        assertInternalCacheSize(1);

        setClockTime(2L);
        value = qlCache.add(QUERY_ID_A, USER_A, queryLogic, client);
        assertTrue(value, "New timestamp should have caused duplicate entry");
        assertInternalCacheSize(2);
    }

    @Test
    public void testRemoval() {
        setClockTime(1L);
        boolean value = qlCache.add(QUERY_ID_A, USER_A, queryLogic, client);
        assertFirstCacheAdd(value);

        qlCache.poll(QUERY_ID_A);
        assertInternalCacheSize(0);
    }

    @Test
    public void testDuplicateRemoval() {
        setClockTime(1L);
        boolean ret1 = qlCache.add(QUERY_ID_A, USER_A, queryLogic, client);
        assertFirstCacheAdd(ret1);

        setClockTime(2L);
        boolean ret2 = qlCache.add(QUERY_ID_A, USER_A, queryLogic, client);
        qlCache.poll(QUERY_ID_A);
        assertFirstCacheAdd(ret2);

        // We should never be allowing collisions of query-ids, as such, if multiple are present
        // from different times, we should not be trying to assert anything on order
        assertInternalCacheSize(1);
    }

    @Test
    public void testRemovalOfOldEntries() {
        setClockTime(1L);
        boolean ret1 = qlCache.add(QUERY_ID_A, USER_A, queryLogic, client);

        setClockTime(2L);
        boolean ret2 = qlCache.add(QUERY_ID_B, USER_A, queryLogic, client);

        setClockTime(3L);
        boolean ret3 = qlCache.add(QUERY_ID_C, USER_A, queryLogic, client);

        Map<String,Pair<QueryLogic<?>,AccumuloClient>> oldEntries = qlCache.entriesOlderThan(5L, 2L);
        Map<String,Pair<QueryLogic<?>,AccumuloClient>> olderEntries = qlCache.entriesOlderThan(5L, 3L);
        Map<String,Pair<QueryLogic<?>,AccumuloClient>> noEntries = qlCache.entriesOlderThan(5L, 4L);

        // We should never be allowing collisions of query-ids, as such, if multiple are present
        // from different times, we should not be trying to assert anything on order
        assertTrue(ret1, "Expected the cache add to return true for first");
        assertTrue(ret2, "Expected the cache add to return true for second");
        assertTrue(ret3, "Expected the cache add to return true for third");

        assertEquals(2, oldEntries.size());
        assertEquals(Sets.newHashSet(QUERY_ID_A, QUERY_ID_B), oldEntries.keySet());

        assertEquals(1, olderEntries.size());
        assertEquals(Collections.singleton(QUERY_ID_A), olderEntries.keySet());

        assertEquals(0, noEntries.size());
    }

    @Test
    public void testPollByUser() {
        final String USER_B = "userB";

        setClockTime(1L);
        boolean addQueryIdA = qlCache.add(QUERY_ID_A, USER_A, queryLogic, client);
        assertFirstCacheAdd(addQueryIdA);

        setClockTime(2L);
        boolean addQueryIdB = qlCache.add(QUERY_ID_B, USER_B, queryLogic, client);
        assertFirstCacheAdd(addQueryIdB);

        // initial state
        assertInternalCacheSize(2);

        // verify that only the owner can poll a query id
        Pair<QueryLogic<?>,AccumuloClient> user2FetchQuery1 = qlCache.pollIfOwnedBy(QUERY_ID_A, USER_B);
        Pair<QueryLogic<?>,AccumuloClient> user1FetchQuery2 = qlCache.pollIfOwnedBy(QUERY_ID_B, USER_A);
        assertNull(user2FetchQuery1);
        assertNull(user1FetchQuery2);

        // correct owner polls query id
        Pair<QueryLogic<?>,AccumuloClient> user1FetchQuery1 = qlCache.pollIfOwnedBy(QUERY_ID_A, USER_A);
        Pair<QueryLogic<?>,AccumuloClient> user2FetchQuery2 = qlCache.pollIfOwnedBy(QUERY_ID_B, USER_B);
        assertNotNull(user1FetchQuery1);
        assertNotNull(user2FetchQuery2);

        // final state
        assertInternalCacheSize(0);
    }

    @Test
    public void testSnapshot() {
        setClockTime(1L);
        boolean addQueryIdA = qlCache.add(QUERY_ID_A, USER_A, queryLogic, client);
        assertFirstCacheAdd(addQueryIdA);
        assertInternalCacheSize(1);

        Map<String,Pair<QueryLogic<?>,AccumuloClient>> snapshot = qlCache.snapshot();
        assertEquals(1, snapshot.size());

        boolean addQueryIdB = qlCache.add(QUERY_ID_B, USER_A, queryLogic, client);
        assertFirstCacheAdd(addQueryIdB);

        assertInternalCacheSize(2);
        assertEquals(1, snapshot.size());
    }

    /**
     * Set the internal time for the mock clock
     * @param time the time
     */
    private void setClockTime(long time) {
        when(mockClock.millis()).thenReturn(time);
    }

    /**
     * The cache returns null on the first add operation. Verify that this was a first add.
     *
     * @param value
     *            the return value of a cache add operation
     */
    private void assertFirstCacheAdd(boolean value) {
        assertTrue(value, "Expected the cache add to return true");
    }

    /**
     * Assert the size of the internal cache
     *
     * @param expected
     *            the expected size
     */
    private void assertInternalCacheSize(int expected) {
        assertEquals(expected, internalCache.size());
    }

}
