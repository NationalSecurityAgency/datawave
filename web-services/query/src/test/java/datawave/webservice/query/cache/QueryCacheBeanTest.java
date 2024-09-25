package datawave.webservice.query.cache;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.powermock.reflect.Whitebox.setInternalState;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import org.apache.accumulo.core.client.AccumuloClient;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.modules.junit4.PowerMockRunner;

import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.query.logic.QueryLogic;
import datawave.microservice.authorization.util.AuthorizationsUtil;
import datawave.microservice.query.QueryImpl;
import datawave.microservice.querymetric.QueryMetricFactoryImpl;
import datawave.webservice.query.runner.RunningQuery;

@RunWith(PowerMockRunner.class)
public class QueryCacheBeanTest {

    @Mock
    QueryCache altCache;

    @Mock
    QueryLogic<?> logic;

    @Mock
    AbstractMap.SimpleEntry<QueryLogic<?>,AccumuloClient> pair;

    @Mock
    CreatedQueryLogicCacheBean remoteCache;

    @Test
    public void testInit() throws Exception {
        // Run the test
        PowerMock.replayAll();
        new QueryCacheBean();
        PowerMock.verifyAll();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void testListRunningQueries() {
        // Set expectations
        expect(altCache.iterator()).andReturn((Iterator<RunningQuery>) new HashMap().values().iterator());
        Map<String,AbstractMap.SimpleEntry<QueryLogic<?>,AccumuloClient>> snapshot = new HashMap<>();
        this.pair = new AbstractMap.SimpleEntry<>(this.logic, null);
        snapshot.put("key", this.pair);
        expect(this.remoteCache.snapshot()).andReturn(snapshot);

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
        expect(this.altCache.get(queryId.toString())).andReturn(null);

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
        expect(logic.getResultLimit(q)).andReturn(-1L);
        expect(logic.getMaxResults()).andReturn(-1L);
        logic.preInitialize(q, AuthorizationsUtil.buildAuthorizations(null));
        expect(logic.getUserOperations()).andReturn(null);

        PowerMock.replayAll();

        RunningQuery query = new RunningQuery(null, AccumuloConnectionFactory.Priority.HIGH, logic, q, null, null, new QueryMetricFactoryImpl());
        QueryCacheBean bean = new QueryCacheBean();

        QueryCache cache = new QueryCache();
        cache.init();
        cache.put(query.getSettings().getId().toString(), query);
        CreatedQueryLogicCacheBean qlCache = new CreatedQueryLogicCacheBean();
        setInternalState(bean, QueryCache.class, cache);
        setInternalState(bean, CreatedQueryLogicCacheBean.class, qlCache);
        String expectedResult = query.toString();

        RunningQueries output = bean.getRunningQueries();
        assertEquals(expectedResult, output.getQueries().get(0));
    }
}
