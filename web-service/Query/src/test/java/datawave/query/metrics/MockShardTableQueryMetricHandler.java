package datawave.query.metrics;

import java.util.Date;
import java.util.Map;

import datawave.security.authorization.DatawavePrincipal;
import datawave.webservice.query.metric.BaseQueryMetric;
import datawave.webservice.query.metric.QueryMetric;
import datawave.webservice.query.metric.QueryMetricHandler;
import datawave.webservice.query.metric.QueryMetricListResponse;
import datawave.webservice.query.metric.QueryMetricsSummaryHtmlResponse;
import datawave.webservice.query.metric.QueryMetricsSummaryResponse;

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
    
    public Map<String,String> getEventFields(BaseQueryMetric queryMetric) {
        return this.mockHandler.getEventFields(queryMetric);
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
