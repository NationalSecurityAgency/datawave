package datawave.webservice.query.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Set;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.query.logic.QueryLogic;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.microservice.query.config.QueryExpirationProperties;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.limit.QueryLimiter;
import datawave.webservice.query.metric.QueryMetricsBean;
import datawave.webservice.query.runner.RunningQuery;

public class QueryExpirationBeanTest {

    private QueryExpirationBean bean;

    private QueryCache queryCache;
    private QueryExpirationProperties properties;
    private AccumuloConnectionFactory connectionFactory;
    private QueryMetricsBean metricsBean;
    private QueryLimiter queryLimiter;

    @Before
    public void setUp() throws Exception {
        queryCache = new QueryCache();
        queryCache.init();

        properties = new QueryExpirationProperties();

        CreatedQueryLogicCacheBean queryLogicCache = new CreatedQueryLogicCacheBean();

        connectionFactory = Mockito.mock(AccumuloConnectionFactory.class);
        metricsBean = Mockito.mock(QueryMetricsBean.class);
        queryLimiter = Mockito.mock(QueryLimiter.class);

        this.bean = new QueryExpirationBean();
        ReflectionTestUtils.setField(bean, "queryCache", queryCache);
        ReflectionTestUtils.setField(bean, "config", properties);
        ReflectionTestUtils.setField(bean, "connectionFactory", connectionFactory);
        ReflectionTestUtils.setField(bean, "queryLogicCacheBean", queryLogicCache);
        ReflectionTestUtils.setField(bean, "metricsBean", metricsBean);
        ReflectionTestUtils.setField(bean, "queryLimiter", queryLimiter);

        bean.init();
    }

    /**
     * Given a cache which contains a single query that is not considered active or expired, verify that {@link QueryExpirationBean#removeIdleOrExpired()} has
     * no effect on the cache.
     */
    @Test
    public void testRemoveIdleOrExpiredGivenNonIdleOrExpired() throws Exception {
        RunningQuery query = createNonIdleOrExpiredQuery();

        // Do not return any active queries from the query limiter for this test.
        when(queryLimiter.getActiveQueries()).thenReturn(Set.of());

        // Remove all idle/expired queries.
        bean.removeIdleOrExpired();

        // Verify the query is not removed.
        verifyQueryNotRemoved(query);
    }

    /**
     * Given a cache which contains a single query that is considered idle for too long, verify that {@link QueryExpirationBean#removeIdleOrExpired()} removes
     * the query from the cache.
     */
    @Test
    public void testRemoveIdleOrExpiredGivenIdleQuery() throws Exception {
        RunningQuery query = createIdleQuery();

        // Do not return any active queries from the query limiter for this test.
        when(queryLimiter.getActiveQueries()).thenReturn(Set.of());

        // Remove all idle/expired queries.
        bean.removeIdleOrExpired();

        // Verify the query is removed.
        verifyQueryRemoved(query, DatawaveErrorCode.QUERY_TIMEOUT);
    }

    /**
     * Given a cache which contains a single query that is where the next call is considered to be running for too long, verify that
     * {@link QueryExpirationBean#removeIdleOrExpired()} removes the query from the cache.
     */
    @Test
    public void testRemoveIdleOrExpiredGivenNextTooLong() throws Exception {
        RunningQuery query = createNextTooLongQuery();

        // Do not return any active queries from the query limiter for this test.
        when(queryLimiter.getActiveQueries()).thenReturn(Set.of());

        // Remove all idle/expired queries.
        bean.removeIdleOrExpired();

        // Verify the query is removed.
        verifyQueryRemoved(query, DatawaveErrorCode.QUERY_TIMEOUT);
    }

    /**
     * Given a mix of queries in the cache, verify that {@link QueryExpirationBean#removeIdleOrExpired()} only removes the queries that are idle for too long or
     * taking too long to complete its next call.
     */
    @Test
    public void testRemoveIdleOrExpiredGivenMix() throws Exception {
        // Create a mix of queries.
        RunningQuery okayQuery1 = createNonIdleOrExpiredQuery();
        RunningQuery okayQuery2 = createNonIdleOrExpiredQuery();
        RunningQuery okayQuery3 = createNonIdleOrExpiredQuery();
        RunningQuery idleQuery1 = createIdleQuery();
        RunningQuery idleQuery2 = createIdleQuery();
        RunningQuery idleQuery3 = createIdleQuery();
        RunningQuery nextTooLongQuery1 = createNextTooLongQuery();
        RunningQuery nextTooLongQuery2 = createNextTooLongQuery();
        RunningQuery nextTooLongQuery3 = createNextTooLongQuery();

        // Remove all idle/expired queries.
        bean.removeIdleOrExpired();

        // Verify the okay queries are not removed, and the idle/nextTooLong queries are removed.
        verifyQueryNotRemoved(okayQuery1);
        verifyQueryNotRemoved(okayQuery2);
        verifyQueryNotRemoved(okayQuery3);

        verifyQueryRemoved(idleQuery1, DatawaveErrorCode.QUERY_TIMEOUT);
        verifyQueryRemoved(idleQuery2, DatawaveErrorCode.QUERY_TIMEOUT);
        verifyQueryRemoved(idleQuery3, DatawaveErrorCode.QUERY_TIMEOUT);

        verifyQueryRemoved(nextTooLongQuery1, DatawaveErrorCode.QUERY_TIMEOUT);
        verifyQueryRemoved(nextTooLongQuery2, DatawaveErrorCode.QUERY_TIMEOUT);
        verifyQueryRemoved(nextTooLongQuery3, DatawaveErrorCode.QUERY_TIMEOUT);
    }

    /**
     * Verify that when {@link QueryExpirationBean#close()} is called, that all queries are removed from the cache.
     */
    @Test
    public void testClose() throws Exception {
        // Create a mix of queries.
        RunningQuery okayQuery1 = createNonIdleOrExpiredQuery();
        RunningQuery okayQuery2 = createNonIdleOrExpiredQuery();
        RunningQuery okayQuery3 = createNonIdleOrExpiredQuery();
        RunningQuery idleQuery1 = createIdleQuery();
        RunningQuery idleQuery2 = createIdleQuery();
        RunningQuery idleQuery3 = createIdleQuery();
        RunningQuery nextTooLongQuery1 = createNextTooLongQuery();
        RunningQuery nextTooLongQuery2 = createNextTooLongQuery();
        RunningQuery nextTooLongQuery3 = createNextTooLongQuery();

        // Close the bean.
        bean.close();

        // Verify all queries were removed due to a server shutdown.
        verifyQueryRemoved(okayQuery1, DatawaveErrorCode.SERVER_SHUTDOWN);
        verifyQueryRemoved(okayQuery2, DatawaveErrorCode.SERVER_SHUTDOWN);
        verifyQueryRemoved(okayQuery3, DatawaveErrorCode.SERVER_SHUTDOWN);
        verifyQueryRemoved(idleQuery1, DatawaveErrorCode.SERVER_SHUTDOWN);
        verifyQueryRemoved(idleQuery2, DatawaveErrorCode.SERVER_SHUTDOWN);
        verifyQueryRemoved(idleQuery3, DatawaveErrorCode.SERVER_SHUTDOWN);
        verifyQueryRemoved(nextTooLongQuery1, DatawaveErrorCode.SERVER_SHUTDOWN);
        verifyQueryRemoved(nextTooLongQuery2, DatawaveErrorCode.SERVER_SHUTDOWN);
        verifyQueryRemoved(nextTooLongQuery3, DatawaveErrorCode.SERVER_SHUTDOWN);
    }

    /**
     * Verify that when the query limiter has query IDs not contained within the query cache during a call to {@link QueryExpirationBean#removeIdleOrExpired()},
     * that they are invalidated in the query limiter.
     */
    @Test
    public void testActiveQuerySynchronization() throws Exception {
        RunningQuery query = createNonIdleOrExpiredQuery();

        // Return a set of active queries that contain the ID of the query above, and some extra ones.
        Set<String> queryIds = Set.of(query.getSettings().getId().toString(), "otherQuery1", "otherQuery2", "otherQuery3");
        when(queryLimiter.getActiveQueries()).thenReturn(queryIds);

        // Remove all idle/expired queries.
        bean.removeIdleOrExpired();

        // Verify the query is not removed.
        verifyQueryNotRemoved(query);

        // Verify that the query IDs not in the query cache are no longer counted towards query limits.
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Set<String>> captor = ArgumentCaptor.forClass(Set.class);
        verify(queryLimiter).stopCountingQueriesTowardsLimits(captor.capture());
        assertThat(captor.getValue()).containsExactlyInAnyOrder("otherQuery1", "otherQuery2", "otherQuery3");
    }

    private RunningQuery createNonIdleOrExpiredQuery() {
        RunningQuery query = createBaseQuery();

        // Make the query return a time of current call that will not trigger a timeout.
        when(query.hasActiveCall()).thenReturn(true);
        when(query.getLastUsed()).thenReturn(System.currentTimeMillis() - 1);

        return query;
    }

    private RunningQuery createIdleQuery() {
        RunningQuery query = createBaseQuery();

        // Ensure the query is not considered to have an active call.
        when(query.hasActiveCall()).thenReturn(false);

        // Make the query return a last-used time that will trigger an idle timeout.
        when(query.getLastUsed()).thenReturn(System.currentTimeMillis() - (properties.getIdleTimeoutMillis() + 1000));

        return query;
    }

    private RunningQuery createNextTooLongQuery() {
        RunningQuery query = createBaseQuery();

        // Ensure the query is considered to have an active call.
        when(query.hasActiveCall()).thenReturn(true);

        // Make the query return a time of current call that will trigger a timeout.
        when(query.getTimeOfCurrentCall()).thenReturn(System.currentTimeMillis() - (properties.getCallTimeoutMillis() + 1000));

        return query;
    }

    private RunningQuery createBaseQuery() {
        RunningQuery query = Mockito.mock(RunningQuery.class);

        QueryImpl settings = new QueryImpl();
        settings.setId(UUID.randomUUID());
        when(query.getSettings()).thenReturn(settings);

        // Mock up methods needed to verify interactions with the metrics bean.
        @SuppressWarnings("rawtypes")
        QueryLogic queryLogic = Mockito.mock(QueryLogic.class);
        when(queryLogic.getCollectQueryMetrics()).thenReturn(true);
        // noinspection unchecked
        when(query.getLogic()).thenReturn(queryLogic);
        BaseQueryMetric metric = Mockito.mock(BaseQueryMetric.class);
        when(query.getMetric()).thenReturn(metric);

        // Add the query to the cache.
        queryCache.put(settings.getId().toString(), query);

        return query;
    }

    private void verifyQueryNotRemoved(RunningQuery query) throws Exception {
        Query settings = query.getSettings();
        String queryId = settings.getId().toString();

        // Verify we do not fetch the metrics for the query to update the metrics bean.
        verify(query, never()).getLogic();

        // Verify that the query is not canceled.
        verify(query, never()).cancel();

        // Verify we do not close the connection.
        verify(query, never()).closeConnection(connectionFactory);

        // Verify that we do not stop counting the query towards query limits.
        verify(queryLimiter, never()).stopCountingQueryTowardsLimits(queryId);

        // Verify the query is still in the query cache.
        assertThat(queryCache.get(queryId)).isNotNull();

        // Verify that a timeout exception was not recorded.
        assertThat(query.getSettings().getUncaughtExceptionHandler()).isNull();
    }

    private void verifyQueryRemoved(RunningQuery query, DatawaveErrorCode errorCode) throws Exception {
        Query settings = query.getSettings();
        String queryId = settings.getId().toString();

        // Verify the metric bean updated metrics for the query.
        verify(metricsBean).updateMetric(query.getMetric());

        // Verify that the query was canceled if it had an active call.
        if (query.hasActiveCall()) {
            verify(query).cancel();
        } else {
            verify(query, never()).cancel();
        }

        // Verify we closed the connection.
        verify(query).closeConnection(connectionFactory);

        // Verify that we stopped counting the query towards query limits.
        verify(queryLimiter).stopCountingQueryTowardsLimits(queryId);

        // Verify the query is no longer in the query cache.
        assertThat(queryCache.get(queryId)).isNull();

        // Verify that a timeout exception was recorded in the exception handler.
        Throwable throwable = query.getSettings().getUncaughtExceptionHandler().getUncaughtException().getLeft();
        assertThat(throwable).isInstanceOf(QueryException.class);
        assertThat(throwable.getMessage()).isEqualTo(errorCode.toString());
    }
}
