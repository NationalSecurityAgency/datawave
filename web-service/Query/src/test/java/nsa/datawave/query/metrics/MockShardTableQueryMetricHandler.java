package nsa.datawave.query.metrics;

import java.util.Date;

import nsa.datawave.security.authorization.DatawavePrincipal;
import nsa.datawave.webservice.query.metric.BaseQueryMetric;
import nsa.datawave.webservice.query.metric.QueryMetric;
import nsa.datawave.webservice.query.metric.QueryMetricHandler;
import nsa.datawave.webservice.query.metric.QueryMetricListResponse;
import nsa.datawave.webservice.query.metric.QueryMetricsSummaryHtmlResponse;
import nsa.datawave.webservice.query.metric.QueryMetricsSummaryResponse;

import javax.inject.Singleton;

/**
 * Simulated QueryMetricHandler to prevent classloading failure in unit test for UpgradeTo_2_2 and QueryMetricsBean. Used in conjunction with PowerMock to
 * create a wrapped instance of a mock QueryMetricHandler.
 */
@Singleton
public class MockShardTableQueryMetricHandler implements QueryMetricHandler<QueryMetric> {
    private QueryMetricHandler<QueryMetric> mockHandler;
    
    public MockShardTableQueryMetricHandler() {
        reload();
    }
    
    @Override
    public void updateMetric(QueryMetric metric, DatawavePrincipal datawavePrincipal) throws Exception {
        this.mockHandler.updateMetric(metric, datawavePrincipal);
    }
    
    @Override
    public QueryMetricListResponse query(String user, String queryId, DatawavePrincipal datawavePrincipal) {
        return (QueryMetricListResponse) this.mockHandler.query(user, queryId, datawavePrincipal);
    }
    
    @Override
    public QueryMetricsSummaryResponse getTotalQueriesSummaryCounts(Date begin, Date end, DatawavePrincipal datawavePrincipal) {
        return this.mockHandler.getTotalQueriesSummaryCounts(begin, end, datawavePrincipal);
    }
    
    @Override
    public QueryMetricsSummaryHtmlResponse getUserQueriesSummary(Date begin, Date end, DatawavePrincipal datawavePrincipal) {
        return this.mockHandler.getTotalQueriesSummary(begin, end, datawavePrincipal);
    }
    
    @Override
    public void flush() throws Exception {
        this.mockHandler.flush();
    }
    
    @Override
    public void reload() {
        this.mockHandler = new MockQueryMetricHandlerFactory().newMockQueryMetricHandler();
    }
    
    @Override
    public QueryMetricsSummaryHtmlResponse getTotalQueriesSummary(Date begin, Date end, DatawavePrincipal datawavePrincipal) {
        return this.mockHandler.getTotalQueriesSummary(begin, end, datawavePrincipal);
    }
}
