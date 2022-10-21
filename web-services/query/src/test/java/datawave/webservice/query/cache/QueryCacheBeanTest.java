package datawave.webservice.query.cache;

import datawave.microservice.querymetric.QueryMetricFactoryImpl;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.logic.QueryLogic;
import datawave.webservice.query.runner.RunningQuery;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.util.Pair;
import org.easymock.EasyMockExtension;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import static org.easymock.EasyMock.expect;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(EasyMockExtension.class)
public class QueryCacheBeanTest extends EasyMockSupport {
    
    @Mock
    QueryCache altCache;
    
    @Mock
    QueryLogic<?> logic;
    
    @Mock
    Pair<QueryLogic<?>,Connector> pair;
    
    @Mock
    CreatedQueryLogicCacheBean remoteCache;
    
    @Mock
    RunningQuery runningQuery;
    
    @Test
    public void testInit() throws Exception {
        // Run the test
        new QueryCacheBean();
    }
    
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void testListRunningQueries() {
        // Set expectations
        expect(altCache.iterator()).andReturn((Iterator<RunningQuery>) new HashMap().values().iterator());
        Map<String,Pair<QueryLogic<?>,Connector>> snapshot = new HashMap<>();
        snapshot.put("key", this.pair);
        expect(this.remoteCache.snapshot()).andReturn(snapshot);
        expect(this.pair.getFirst()).andReturn((QueryLogic) this.logic);
        
        // Run the test
        replayAll();
        QueryCacheBean subject = new QueryCacheBean();
        ReflectionTestUtils.setField(subject, "cache", altCache);
        ReflectionTestUtils.setField(subject, "qlCache", remoteCache);
        String result1 = subject.listRunningQueries();
        verifyAll();
        
        // Verify results
        assertNotNull(result1, "List of running queries should not be null");
    }
    
    @Test
    public void testCancelUserQuery_CacheReturnsNonRunningQuery() throws Exception {
        // Set expectations
        UUID queryId = UUID.randomUUID();
        expect(this.altCache.get(queryId.toString())).andReturn(runningQuery);
        
        // Run the test
        replayAll();
        QueryCacheBean subject = new QueryCacheBean();
        ReflectionTestUtils.setField(subject, "cache", altCache);
        String result1 = subject.cancelUserQuery(queryId.toString());
        verifyAll();
        
        // Verify results
        assertNotNull("List of running queries should not be null", result1);
    }
    
    @Test
    public void testCancelUserQuery_HappyPath() throws Exception {
        
        // Set expectations
        UUID queryId = UUID.randomUUID();
        expect(this.altCache.get(queryId.toString())).andReturn(null);
        
        // Run the test
        replayAll();
        QueryCacheBean subject = new QueryCacheBean();
        ReflectionTestUtils.setField(subject, "cache", altCache);
        String result1 = subject.cancelUserQuery(queryId.toString());
        verifyAll();
        
        // Verify results
        assertNotNull("List of running queries should not be null", result1);
    }
    
    @Test
    public void testGetRunningQueries() throws Exception {
        QueryImpl q = new QueryImpl();
        q.setQueryLogicName("EventQuery");
        q.setBeginDate(new Date());
        q.setEndDate(new Date());
        q.setExpirationDate(new Date());
        q.setId(UUID.randomUUID());
        q.setPagesize(10);
        q.setQuery("FOO == BAR");
        q.setQueryName("test query");
        q.setQueryAuthorizations("ALL");
        q.setUserDN("some user");
        q.setDnList(Collections.singletonList("some user"));
        
        expect(logic.getCollectQueryMetrics()).andReturn(false);
        expect(logic.isLongRunningQuery()).andReturn(false);
        expect(logic.getResultLimit(q.getDnList())).andReturn(-1L);
        expect(logic.getMaxResults()).andReturn(-1L);
        
        replayAll();
        
        RunningQuery query = new RunningQuery(null, AccumuloConnectionFactory.Priority.HIGH, logic, q, null, null, new QueryMetricFactoryImpl());
        QueryCacheBean bean = new QueryCacheBean();
        
        QueryCache cache = new QueryCache();
        cache.init();
        cache.put(query.getSettings().getId().toString(), query);
        CreatedQueryLogicCacheBean qlCache = new CreatedQueryLogicCacheBean();
        ReflectionTestUtils.setField(bean, "cache", cache);
        ReflectionTestUtils.setField(bean, "qlCache", qlCache);

        String expectedResult = query.toString();
        
        RunningQueries output = bean.getRunningQueries();
        assertEquals(expectedResult, output.getQueries().get(0));
    }
}
