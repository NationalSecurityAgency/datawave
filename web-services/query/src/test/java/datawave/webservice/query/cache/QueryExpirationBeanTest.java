package datawave.webservice.query.cache;

import com.google.common.cache.Cache;
import datawave.microservice.querymetric.QueryMetricFactoryImpl;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.runner.RunningQuery;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Date;
import java.util.UUID;

@Disabled
@ExtendWith(MockitoExtension.class)
public class QueryExpirationBeanTest {
    
    private static CreatedQueryLogicCacheBean qlCache;
    @Mock
    private AccumuloConnectionFactory connFactory;
    private static QueryCache queryCache;
    
    @BeforeAll
    public static void setup() throws IllegalArgumentException, IllegalAccessException {
        queryCache = new QueryCache();
        queryCache.init();
        qlCache = new CreatedQueryLogicCacheBean();
    }
    
    @Test
    public void testRemoveIdleOrExpired() throws Exception {
        QueryExpirationBean bean = createBean(0);
        RunningQuery query = createRunningQuery();
        bean.init();
        String qid = query.getSettings().getId().toString();
        queryCache.put(qid, query);
        qlCache.add(qid, "test", query.getLogic(), null);
        
        Assertions.assertTrue(queryCache.containsKey(qid), "Query Cache doesn't contain query");
        Assertions.assertTrue(qlCache.snapshot().containsKey(qid), "Query Logic Cache doesn't contain query logic");
        
        bean.removeIdleOrExpired();
        Assertions.assertFalse(queryCache.containsKey(qid), "Query Cache still contains query");
        Assertions.assertFalse(qlCache.snapshot().containsKey(qid), "Query Logic Cache still contains query logic");
        
        for (int i = 0; i < 5; i++) {
            RunningQuery runningQuery = createRunningQuery();
            String key = runningQuery.getSettings().getId().toString();
            queryCache.put(key, runningQuery);
            qlCache.add(key, key, runningQuery.getLogic(), null);
        }
        int queryCacheSize = ((Cache) ReflectionTestUtils.getField(queryCache, "cache")).asMap().size();
        Assertions.assertEquals(5, queryCacheSize);
        Assertions.assertEquals(5, qlCache.snapshot().size());
        bean.close();
        qlCache.shutdown();
        queryCacheSize = ((Cache) ReflectionTestUtils.getField(queryCache, "cache")).asMap().size();
        Assertions.assertEquals(0, queryCacheSize, "Query Cache is not empty: " + queryCacheSize);
        Assertions.assertEquals(0, qlCache.snapshot().size(), "Query Logic Cache is not empty: " + qlCache.snapshot().size());
    }
    
    private QueryExpirationBean createBean(int expireTime) throws IllegalArgumentException, IllegalAccessException {
        QueryExpirationBean bean = new QueryExpirationBean();
        
        QueryExpirationConfiguration expirationConfiguration = new QueryExpirationConfiguration();
        ReflectionTestUtils.setField(expirationConfiguration, "idleTimeMinutes", expireTime);
        ReflectionTestUtils.setField(expirationConfiguration, "callTimeMinutes", expireTime);
        
        ReflectionTestUtils.setField(bean, "conf", expirationConfiguration);
        ReflectionTestUtils.setField(bean, "cache", queryCache);
        ReflectionTestUtils.setField(bean, "qlCache", qlCache);
        ReflectionTestUtils.setField(bean, "connectionFactory", connFactory);
        
        return bean;
    }
    
    private RunningQuery createRunningQuery() throws Exception {
        QueryImpl q = new QueryImpl();
        q.setQueryLogicName("EventQuery");
        q.setBeginDate(new Date());
        q.setEndDate(new Date());
        q.setExpirationDate(new Date(new Date().getTime() - 86400));
        q.setId(UUID.randomUUID());
        q.setPagesize(10);
        q.setQuery("FOO == BAR");
        q.setQueryName("test query");
        q.setQueryAuthorizations("ALL");
        q.setUserDN("some user");
        
        return new RunningQuery(null, AccumuloConnectionFactory.Priority.HIGH, new TestQueryLogic(), q, null, null, new QueryMetricFactoryImpl());
    }
    
}
