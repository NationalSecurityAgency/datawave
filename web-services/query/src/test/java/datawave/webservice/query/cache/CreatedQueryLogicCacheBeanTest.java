package datawave.webservice.query.cache;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import datawave.webservice.query.cache.CreatedQueryLogicCacheBean.Triple;
import datawave.webservice.query.logic.QueryLogic;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.util.Pair;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.common.collect.Sets;

/**
 *
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({CreatedQueryLogicCacheBean.class})
// <-- Have to do this to make the static System mock work
public class CreatedQueryLogicCacheBeanTest {

    protected CreatedQueryLogicCacheBean qlCache = null;
    protected ConcurrentHashMap<Pair<String,Long>,Triple> internalCache = null;
    protected QueryLogic<?> queryLogic;
    protected AccumuloClient client;

    @Before
    public void setupCacheBean() throws IllegalAccessException, SecurityException, NoSuchMethodException {
        qlCache = new CreatedQueryLogicCacheBean();
        queryLogic = PowerMock.createMock(QueryLogic.class);
        client = PowerMock.createMock(AccumuloClient.class);
        internalCache = new ConcurrentHashMap<>();

        PowerMock.field(CreatedQueryLogicCacheBean.class, "cache").set(qlCache, internalCache);

        PowerMock.mockStatic(System.class, System.class.getMethod("currentTimeMillis"));
    }

    @Test
    public void testFirstInsertion() throws Exception {
        String queryId = "12345";
        String userId = "me";
        long timestamp = 1l;

        EasyMock.expect(System.currentTimeMillis()).andReturn(timestamp);

        PowerMock.replayAll();

        boolean ret = qlCache.add(queryId, userId, queryLogic, client);

        PowerMock.verifyAll();

        Assert.assertTrue("Expected the cache add to return true", ret);
    }

    @Test
    public void testDuplicateKeyInsertion() throws Exception {
        String queryId = "12345";
        String userId = "me";
        long timestamp = 1l;

        EasyMock.expect(System.currentTimeMillis()).andReturn(timestamp);

        PowerMock.replayAll();

        boolean ret = qlCache.add(queryId, userId, queryLogic, client);

        PowerMock.verifyAll();

        Assert.assertTrue("Expected the cache add to return true", ret);
        Assert.assertEquals(1, internalCache.size());

        PowerMock.resetAll();

        long timestamp2 = 2l;

        EasyMock.expect(System.currentTimeMillis()).andReturn(timestamp2);

        PowerMock.replayAll();

        ret = qlCache.add(queryId, userId, queryLogic, client);

        PowerMock.verifyAll();

        Assert.assertTrue("New timestamp should have caused duplicate entry", ret);
        Assert.assertEquals(2, internalCache.size());
    }

    @Test
    public void testRemoval() throws Exception {

        String queryId = "12345";
        String userId = "me";
        long timestamp = 1l;

        EasyMock.expect(System.currentTimeMillis()).andReturn(timestamp);

        PowerMock.replayAll();

        boolean ret = qlCache.add(queryId, userId, queryLogic, client);
        qlCache.poll(queryId);

        PowerMock.verifyAll();

        Assert.assertTrue("Expected the cache add to return true", ret);
        Assert.assertEquals(0, internalCache.size());
    }

    @Test
    public void testDuplicateRemoval() throws Exception {

        String queryId = "12345";
        String userId = "me";
        long timestamp1 = 1l, timestamp2 = 2l;

        EasyMock.expect(System.currentTimeMillis()).andReturn(timestamp1);
        EasyMock.expect(System.currentTimeMillis()).andReturn(timestamp2);

        PowerMock.replayAll();

        boolean ret1 = qlCache.add(queryId, userId, queryLogic, client);
        boolean ret2 = qlCache.add(queryId, userId, queryLogic, client);
        qlCache.poll(queryId);

        PowerMock.verifyAll();

        // We should never be allowing collisions of query-ids, as such, if multiple are present
        // from different times, we should not be trying to assert anything on order
        Assert.assertTrue("Expected the cache add to return true for first", ret1);
        Assert.assertTrue("Expected the cache add to return true for second", ret2);
        Assert.assertEquals(1, internalCache.size());
    }

    @Test
    public void testRemovalOfOldEntries() throws Exception {
        String queryId1 = "12345", queryId2 = "123456", queryId3 = "1234567";
        String userId = "me";
        long timestamp1 = 1l, timestamp2 = 2l, timestamp3 = 3l;

        EasyMock.expect(System.currentTimeMillis()).andReturn(timestamp1);
        EasyMock.expect(System.currentTimeMillis()).andReturn(timestamp2);
        EasyMock.expect(System.currentTimeMillis()).andReturn(timestamp3);

        PowerMock.replayAll();

        boolean ret1 = qlCache.add(queryId1, userId, queryLogic, client);
        boolean ret2 = qlCache.add(queryId2, userId, queryLogic, client);
        boolean ret3 = qlCache.add(queryId3, userId, queryLogic, client);

        Map<String,Pair<QueryLogic<?>,AccumuloClient>> oldEntries = qlCache.entriesOlderThan(5l, 2l), olderEntries = qlCache.entriesOlderThan(5l, 3l),
                        noEntries = qlCache.entriesOlderThan(5l, 4l);

        PowerMock.verifyAll();

        // We should never be allowing collisions of query-ids, as such, if multiple are present
        // from different times, we should not be trying to assert anything on order
        Assert.assertTrue("Expected the cache add to return true for first", ret1);
        Assert.assertTrue("Expected the cache add to return true for second", ret2);
        Assert.assertTrue("Expected the cache add to return true for third", ret3);

        Assert.assertEquals(2, oldEntries.size());
        Assert.assertEquals(Sets.newHashSet(queryId1, queryId2), oldEntries.keySet());

        Assert.assertEquals(1, olderEntries.size());
        Assert.assertEquals(Collections.singleton(queryId1), olderEntries.keySet());

        Assert.assertEquals(0, noEntries.size());
    }

    @Test
    public void testPollByUser() throws Exception {
        String queryId1 = "12345", queryId2 = "123456";
        String user1 = "me", user2 = "you";
        long timestamp1 = 1l, timestamp2 = 2l;

        EasyMock.expect(System.currentTimeMillis()).andReturn(timestamp1);
        EasyMock.expect(System.currentTimeMillis()).andReturn(timestamp2);

        PowerMock.replayAll();

        boolean ret1 = qlCache.add(queryId1, user1, queryLogic, client);
        boolean ret2 = qlCache.add(queryId2, user2, queryLogic, client);

        Pair<QueryLogic<?>,AccumuloClient> user2FetchQuery1 = qlCache.pollIfOwnedBy(queryId1, user2), user1FetchQuery2 = qlCache.pollIfOwnedBy(queryId2, user1);

        PowerMock.verifyAll();

        Assert.assertTrue("Did not successfully insert record 1", ret1);
        Assert.assertTrue("Did not successfully insert record 2", ret2);

        Assert.assertNull(user1FetchQuery2);
        Assert.assertNull(user2FetchQuery1);

        Assert.assertEquals(2, internalCache.size());

        PowerMock.resetAll();

        Pair<QueryLogic<?>,AccumuloClient> user1FetchQuery1 = qlCache.pollIfOwnedBy(queryId1, user1), user2FetchQuery2 = qlCache.pollIfOwnedBy(queryId2, user2);

        Assert.assertNotNull(user1FetchQuery1);
        Assert.assertNotNull(user2FetchQuery2);

        Assert.assertEquals(0, internalCache.size());
    }

    @Test
    public void testSnapshot() throws Exception {
        String queryId1 = "12345", queryId2 = "123456";
        String userId = "me";
        long timestamp = 1l;

        EasyMock.expect(System.currentTimeMillis()).andReturn(timestamp);

        PowerMock.replayAll();

        boolean ret1 = qlCache.add(queryId1, userId, queryLogic, client);

        Map<String,Pair<QueryLogic<?>,AccumuloClient>> snapshot = qlCache.snapshot();

        PowerMock.verifyAll();

        Assert.assertTrue("Expected the cache add to return true", ret1);
        Assert.assertEquals(1, internalCache.size());
        Assert.assertEquals(1, snapshot.size());

        PowerMock.resetAll();

        EasyMock.expect(System.currentTimeMillis()).andReturn(timestamp);

        PowerMock.replayAll();

        boolean ret2 = qlCache.add(queryId2, userId, queryLogic, client);

        PowerMock.verifyAll();

        Assert.assertTrue("Expected the cache add to return true", ret2);
        Assert.assertEquals(2, internalCache.size());
        Assert.assertEquals(1, snapshot.size());
    }

}
