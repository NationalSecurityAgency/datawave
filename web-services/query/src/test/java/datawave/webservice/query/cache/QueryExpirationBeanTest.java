package datawave.webservice.query.cache;

import java.util.Date;
import java.util.UUID;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.cache.Cache;

import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.microservice.query.QueryImpl;
import datawave.microservice.query.config.QueryExpirationProperties;
import datawave.microservice.querymetric.QueryMetricFactoryImpl;
import datawave.webservice.query.runner.RunningQuery;

public class QueryExpirationBeanTest {

    private static CreatedQueryLogicCacheBean qlCache;
    private static AccumuloConnectionFactory connFactory;
    private static QueryCache queryCache;

    @BeforeClass
    public static void setup() throws IllegalArgumentException, IllegalAccessException {
        queryCache = new QueryCache();
        queryCache.init();
        qlCache = new CreatedQueryLogicCacheBean();
        connFactory = Mockito.mock(AccumuloConnectionFactory.class);
    }

    @Test
    public void testRemoveIdleOrExpired() throws Exception {
        QueryExpirationBean bean = createBean(0);
        RunningQuery query = createRunningQuery();
        bean.init();
        String qid = query.getSettings().getId().toString();
        queryCache.put(qid, query);
        qlCache.add(qid, "test", query.getLogic(), null);

        Assert.assertTrue("Query Cache doesn't contain query", queryCache.containsKey(qid));
        Assert.assertTrue("Query Logic Cache doesn't contain query logic", qlCache.snapshot().containsKey(qid));

        bean.removeIdleOrExpired();
        Assert.assertFalse("Query Cache still contains query", queryCache.containsKey(qid));
        Assert.assertFalse("Query Logic Cache still contains query logic", qlCache.snapshot().containsKey(qid));

        Cache<String,RunningQuery> queryCacheBuild = queryCache.buildCache();
        for (int i = 0; i < 5; i++) {
            RunningQuery runningQuery = createRunningQuery();
            String key = runningQuery.getSettings().getId().toString();
            queryCacheBuild.put(key, runningQuery);
            qlCache.add(key, key, runningQuery.getLogic(), null);
        }
        int queryCacheSize = queryCacheBuild.asMap().size();
        Assert.assertEquals(5, queryCacheSize);
        Assert.assertEquals(5, qlCache.snapshot().size());
        bean.close();
        qlCache.shutdown();

        queryCacheBuild = queryCache.buildCache();
        queryCacheSize = queryCacheBuild.asMap().size();
        Assert.assertEquals("Query Cache is not empty: " + queryCacheSize, 0, queryCacheSize);
        Assert.assertEquals("Query Logic Cache is not empty: " + qlCache.snapshot().size(), 0, qlCache.snapshot().size());
    }

    private QueryExpirationBean createBean(int expireTime) throws IllegalArgumentException, IllegalAccessException {
        QueryExpirationBean bean = new QueryExpirationBean();

        QueryExpirationProperties expirationConfiguration = new QueryExpirationProperties();
        expirationConfiguration.setIdleTimeout(expireTime);
        expirationConfiguration.setCallTimeout(expireTime);

        bean.conf = expirationConfiguration;
        bean.cache = queryCache;
        bean.qlCache = qlCache;
        bean.connectionFactory = connFactory;

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
