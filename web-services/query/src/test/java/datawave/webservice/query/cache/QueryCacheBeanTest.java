package datawave.webservice.query.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.query.logic.QueryLogic;
import datawave.microservice.authorization.util.AuthorizationsUtil;
import datawave.microservice.query.QueryImpl;
import datawave.microservice.querymetric.QueryMetricFactoryImpl;
import datawave.webservice.query.runner.RunningQuery;

@ExtendWith(MockitoExtension.class)
public class QueryCacheBeanTest {

    @Mock
    QueryCache altCache;

    @Mock
    QueryLogic<?> logic;

    @Mock
    Pair<QueryLogic<?>,AccumuloClient> pair;

    @Mock
    CreatedQueryLogicCacheBean remoteCache;

    @Test
    public void testInit() throws Exception {
        // Run the test
        new QueryCacheBean();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void testListRunningQueries() {
        // Set expectations
        when(altCache.iterator()).thenReturn(Collections.emptyIterator());
        Map<String,Pair<QueryLogic<?>,AccumuloClient>> snapshot = new HashMap<>();
        snapshot.put("key", this.pair);
        when(this.remoteCache.snapshot()).thenReturn(snapshot);
        when(this.pair.getLeft()).thenReturn((QueryLogic) this.logic);

        // Run the test
        QueryCacheBean subject = new QueryCacheBean();
        ReflectionTestUtils.setField(subject, "cache", altCache);
        ReflectionTestUtils.setField(subject, "qlCache", remoteCache);
        String result1 = subject.listRunningQueries();

        // Verify results
        assertNotNull(result1, "List of running queries should not be null");
    }

    @Test
    public void testCancelUserQuery_CacheReturnsNonRunningQuery() throws Exception {
        // Set expectations
        UUID queryId = UUID.randomUUID();
        when(this.altCache.get(queryId.toString())).thenReturn(mock(RunningQuery.class));

        // Run the test
        QueryCacheBean subject = new QueryCacheBean();
        ReflectionTestUtils.setField(subject, "cache", altCache);
        String result1 = subject.cancelUserQuery(queryId.toString());

        // Verify results
        assertNotNull(result1, "List of running queries should not be null");
    }

    @Test
    public void testCancelUserQuery_HappyPath() throws Exception {

        // Set expectations
        UUID queryId = UUID.randomUUID();
        when(this.altCache.get(queryId.toString())).thenReturn(null);

        // Run the test
        QueryCacheBean subject = new QueryCacheBean();
        ReflectionTestUtils.setField(subject, "cache", altCache);
        String result1 = subject.cancelUserQuery(queryId.toString());

        // Verify results
        assertNotNull(result1, "List of running queries should not be null");
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

        when(logic.getCollectQueryMetrics()).thenReturn(false);
        when(logic.getResultLimit(q)).thenReturn(-1L);
        when(logic.getMaxResults()).thenReturn(-1L);
        logic.preInitialize(q, AuthorizationsUtil.buildAuthorizations(null));
        when(logic.getUserOperations()).thenReturn(null);

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
