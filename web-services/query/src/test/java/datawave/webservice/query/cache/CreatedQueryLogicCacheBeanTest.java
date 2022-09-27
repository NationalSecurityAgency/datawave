package datawave.webservice.query.cache;

import com.google.common.collect.Sets;
import datawave.webservice.query.cache.CreatedQueryLogicCacheBean.Triple;
import datawave.webservice.query.logic.QueryLogic;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.util.Pair;
import org.easymock.Mock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.mockito.Mockito.when;

/**
 * 
 */
@Disabled
@ExtendWith(MockitoExtension.class)
public class CreatedQueryLogicCacheBeanTest {
    
    protected CreatedQueryLogicCacheBean qlCache;
    protected ConcurrentHashMap<Pair<String,Long>,Triple> internalCache;
    @Mock
    protected QueryLogic<?> queryLogic;
    @Mock
    protected Connector conn;
    
    @BeforeEach
    public void setupCacheBean() throws IllegalAccessException, SecurityException, NoSuchMethodException {
        qlCache = new CreatedQueryLogicCacheBean();
        internalCache = new ConcurrentHashMap<>();
        
        // PowerMock.field(CreatedQueryLogicCacheBean.class, "cache").set(qlCache, internalCache);
        //
        // PowerMock.mockStatic(System.class, System.class.getMethod("currentTimeMillis"));
    }
    
    @Test
    public void testFirstInsertion() throws Exception {
        String queryId = "12345";
        String userId = "me";
        long timestamp = 1l;
        
        when(System.currentTimeMillis()).thenReturn(timestamp);
        
        boolean ret = qlCache.add(queryId, userId, queryLogic, conn);
        
        Assertions.assertTrue(ret, "Expected the cache add to return true");
    }
    
    @Test
    public void testDuplicateKeyInsertion() throws Exception {
        String queryId = "12345";
        String userId = "me";
        long timestamp = 1l;
        
        when(System.currentTimeMillis()).thenReturn(timestamp);
        
        boolean ret = qlCache.add(queryId, userId, queryLogic, conn);
        
        Assertions.assertTrue(ret, "Expected the cache add to return true");
        Assertions.assertEquals(1, internalCache.size());
        
        long timestamp2 = 2l;
        
        when(System.currentTimeMillis()).thenReturn(timestamp2);
        
        ret = qlCache.add(queryId, userId, queryLogic, conn);
        
        Assertions.assertTrue(ret, "New timestamp should have caused duplicate entry");
        Assertions.assertEquals(2, internalCache.size());
    }
    
    @Test
    public void testRemoval() throws Exception {
        
        String queryId = "12345";
        String userId = "me";
        long timestamp = 1l;
        
        when(System.currentTimeMillis()).thenReturn(timestamp);
        
        boolean ret = qlCache.add(queryId, userId, queryLogic, conn);
        qlCache.poll(queryId);
        
        Assertions.assertTrue(ret, "Expected the cache add to return true");
        Assertions.assertEquals(0, internalCache.size());
    }
    
    @Test
    public void testDuplicateRemoval() throws Exception {
        
        String queryId = "12345";
        String userId = "me";
        long timestamp1 = 1l, timestamp2 = 2l;
        
        when(System.currentTimeMillis()).thenReturn(timestamp1);
        when(System.currentTimeMillis()).thenReturn(timestamp2);
        
        boolean ret1 = qlCache.add(queryId, userId, queryLogic, conn);
        boolean ret2 = qlCache.add(queryId, userId, queryLogic, conn);
        qlCache.poll(queryId);
        
        // We should never be allowing collisions of query-ids, as such, if multiple are present
        // from different times, we should not be trying to assert anything on order
        Assertions.assertTrue(ret1, "Expected the cache add to return true for first");
        Assertions.assertTrue(ret2, "Expected the cache add to return true for second");
        Assertions.assertEquals(1, internalCache.size());
    }
    
    @Test
    public void testRemovalOfOldEntries() throws Exception {
        String queryId1 = "12345", queryId2 = "123456", queryId3 = "1234567";
        String userId = "me";
        long timestamp1 = 1l, timestamp2 = 2l, timestamp3 = 3l;
        
        when(System.currentTimeMillis()).thenReturn(timestamp1);
        when(System.currentTimeMillis()).thenReturn(timestamp2);
        when(System.currentTimeMillis()).thenReturn(timestamp3);
        
        boolean ret1 = qlCache.add(queryId1, userId, queryLogic, conn);
        boolean ret2 = qlCache.add(queryId2, userId, queryLogic, conn);
        boolean ret3 = qlCache.add(queryId3, userId, queryLogic, conn);
        
        Map<String,Pair<QueryLogic<?>,Connector>> oldEntries = qlCache.entriesOlderThan(5l, 2l), olderEntries = qlCache.entriesOlderThan(5l, 3l), noEntries = qlCache
                        .entriesOlderThan(5l, 4l);
        
        // We should never be allowing collisions of query-ids, as such, if multiple are present
        // from different times, we should not be trying to assert anything on order
        Assertions.assertTrue(ret1, "Expected the cache add to return true for first");
        Assertions.assertTrue(ret2, "Expected the cache add to return true for second");
        Assertions.assertTrue(ret3, "Expected the cache add to return true for third");
        
        Assertions.assertEquals(2, oldEntries.size());
        Assertions.assertEquals(Sets.newHashSet(queryId1, queryId2), oldEntries.keySet());
        
        Assertions.assertEquals(1, olderEntries.size());
        Assertions.assertEquals(Collections.singleton(queryId1), olderEntries.keySet());
        
        Assertions.assertEquals(0, noEntries.size());
    }
    
    @Test
    public void testPollByUser() throws Exception {
        String queryId1 = "12345", queryId2 = "123456";
        String user1 = "me", user2 = "you";
        long timestamp1 = 1l, timestamp2 = 2l;
        
        when(System.currentTimeMillis()).thenReturn(timestamp1);
        when(System.currentTimeMillis()).thenReturn(timestamp2);
        
        boolean ret1 = qlCache.add(queryId1, user1, queryLogic, conn);
        boolean ret2 = qlCache.add(queryId2, user2, queryLogic, conn);
        
        Pair<QueryLogic<?>,Connector> user2FetchQuery1 = qlCache.pollIfOwnedBy(queryId1, user2), user1FetchQuery2 = qlCache.pollIfOwnedBy(queryId2, user1);
        
        Assertions.assertTrue(ret1, "Did not successfully insert record 1");
        Assertions.assertTrue(ret2, "Did not successfully insert record 2");
        
        Assertions.assertNull(user1FetchQuery2);
        Assertions.assertNull(user2FetchQuery1);
        
        Assertions.assertEquals(2, internalCache.size());
        
        Pair<QueryLogic<?>,Connector> user1FetchQuery1 = qlCache.pollIfOwnedBy(queryId1, user1), user2FetchQuery2 = qlCache.pollIfOwnedBy(queryId2, user2);
        
        Assertions.assertNotNull(user1FetchQuery1);
        Assertions.assertNotNull(user2FetchQuery2);
        
        Assertions.assertEquals(0, internalCache.size());
    }
    
    @Test
    public void testSnapshot() throws Exception {
        String queryId1 = "12345", queryId2 = "123456";
        String userId = "me";
        long timestamp = 1l;
        
        when(System.currentTimeMillis()).thenReturn(timestamp);
        
        boolean ret1 = qlCache.add(queryId1, userId, queryLogic, conn);
        
        Map<String,Pair<QueryLogic<?>,Connector>> snapshot = qlCache.snapshot();
        
        Assertions.assertTrue(ret1, "Expected the cache add to return true");
        Assertions.assertEquals(1, internalCache.size());
        Assertions.assertEquals(1, snapshot.size());
        
        when(System.currentTimeMillis()).thenReturn(timestamp);
        
        boolean ret2 = qlCache.add(queryId2, userId, queryLogic, conn);
        
        Assertions.assertTrue(ret2, "Expected the cache add to return true");
        Assertions.assertEquals(2, internalCache.size());
        Assertions.assertEquals(1, snapshot.size());
    }
    
}
