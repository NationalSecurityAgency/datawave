package datawave.webservice.query.runner;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import datawave.core.query.cache.ResultsPage;
import datawave.core.query.logic.QueryLogic;
import datawave.core.query.logic.QueryLogicTransformer;
import datawave.microservice.query.QueryImpl;
import datawave.microservice.querymetric.QueryMetric;
import datawave.webservice.query.limit.QueryLimiter;
import datawave.webservice.result.BaseQueryResponse;

class QueryExecutorCompletionTest {
    @Test
    void fullPageKeepsQueryCounted() throws Exception {
        QueryLimiter limiter = nextPage(false);
        verify(limiter, never()).stopCountingQueryTowardsLimits(any(String.class));
    }

    @Test
    void terminalPageReleasesQueryCount() throws Exception {
        QueryLimiter limiter = nextPage(true);
        verify(limiter).stopCountingQueryTowardsLimits(any(String.class));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private QueryLimiter nextPage(boolean finished) throws Exception {
        QueryExecutorBean bean = new QueryExecutorBean();
        QueryLimiter limiter = mock(QueryLimiter.class);
        ReflectionTestUtils.setField(bean, "queryLimiter", limiter);
        RunningQuery query = mock(RunningQuery.class);
        QueryImpl settings = new QueryImpl();
        settings.setId(UUID.randomUUID());
        QueryLogic logic = mock(QueryLogic.class);
        QueryLogicTransformer transformer = mock(QueryLogicTransformer.class);
        ResultsPage page = new ResultsPage(Collections.singletonList("row"), ResultsPage.Status.COMPLETE);
        when(query.next()).thenReturn(page);
        when(query.isFinished()).thenReturn(finished);
        when(query.getLogic()).thenReturn(logic);
        when(query.getSettings()).thenReturn(settings);
        when(query.getMetric()).thenReturn(new QueryMetric());
        when(logic.getEnrichedTransformer(settings)).thenReturn(transformer);
        when(transformer.createResponse(page)).thenReturn(mock(BaseQueryResponse.class));
        ReflectionTestUtils.invokeMethod(bean, "_next", query, settings.getId().toString(), Collections.emptyList());
        return limiter;
    }
}
