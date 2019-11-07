package datawave.webservice.query.cache;

import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.logic.QueryLogic;
import datawave.webservice.query.runner.RunningQuery;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.util.Pair;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.powermock.reflect.Whitebox.setInternalState;

@RunWith(PowerMockRunner.class)
public class QueryCacheBeanTest {
    
    private RunningQuery query = null;
    
    private QueryCacheBean bean = null;
    private String expectedResult = null;
    
    @Mock
    QueryCache altCache;
    
    @Mock
    QueryLogic<?> logic;
    
    @Mock
    Pair<QueryLogic<?>,Connector> pair;
    
    @Mock
    CreatedQueryLogicCacheBean remoteCache;
    
    @Before
    public void setup() throws Exception {
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
        
        query = new RunningQuery(null, AccumuloConnectionFactory.Priority.HIGH, null, q, null, null, new QueryMetricFactoryImpl());
        bean = new QueryCacheBean();
        
        QueryCache cache = new QueryCache();
        cache.init();
        cache.put(query.getSettings().getId().toString(), query);
        CreatedQueryLogicCacheBean qlCache = new CreatedQueryLogicCacheBean();
        setInternalState(bean, QueryCache.class, cache);
        setInternalState(bean, CreatedQueryLogicCacheBean.class, qlCache);
        expectedResult = query.toString();
    }
    
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void testInit() throws Exception {
        // Run the test
        PowerMock.replayAll();
        new QueryCacheBean();
        PowerMock.verifyAll();
    }
    
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void testListRunningQueries() throws Exception {
        // Set expectations
        expect(altCache.iterator()).andReturn((Iterator<RunningQuery>) new HashMap().values().iterator());
        Map<String,Pair<QueryLogic<?>,Connector>> snapshot = new HashMap<>();
        snapshot.put("key", this.pair);
        expect(this.remoteCache.snapshot()).andReturn(snapshot);
        expect(this.pair.getFirst()).andReturn((QueryLogic) this.logic);
        
        // Run the test
        PowerMock.replayAll();
        QueryCacheBean subject = new QueryCacheBean();
        setInternalState(subject, QueryCache.class, altCache);
        setInternalState(subject, CreatedQueryLogicCacheBean.class, remoteCache);
        String result1 = subject.listRunningQueries();
        PowerMock.verifyAll();
        
        // Verify results
        assertNotNull("List of running queries should not be null", result1);
    }
    
    @Test
    public void testCancelUserQuery_CacheReturnsNonRunningQuery() throws Exception {
        // Set expectations
        UUID queryId = UUID.randomUUID();
        expect(this.altCache.get(queryId.toString())).andReturn(PowerMock.createMock(RunningQuery.class));
        
        // Run the test
        PowerMock.replayAll();
        QueryCacheBean subject = new QueryCacheBean();
        setInternalState(subject, QueryCache.class, altCache);
        String result1 = subject.cancelUserQuery(queryId.toString());
        PowerMock.verifyAll();
        
        // Verify results
        assertNotNull("List of running queries should not be null", result1);
    }
    
    @Test
    public void testCancelUserQuery_HappyPath() throws Exception {
        // Set expectations
        UUID queryId = UUID.randomUUID();
        expect(this.altCache.get(queryId.toString())).andReturn(this.query);
        
        // Run the test
        PowerMock.replayAll();
        QueryCacheBean subject = new QueryCacheBean();
        setInternalState(subject, QueryCache.class, altCache);
        String result1 = subject.cancelUserQuery(queryId.toString());
        PowerMock.verifyAll();
        
        // Verify results
        assertNotNull("List of running queries should not be null", result1);
    }
    
    @Test
    public void testGetRunningQueries() {
        RunningQueries output = bean.getRunningQueries();
        assertEquals(expectedResult, output.getQueries().get(0));
    }
}
