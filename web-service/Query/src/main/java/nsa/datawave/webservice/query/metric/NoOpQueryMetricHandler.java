package nsa.datawave.webservice.query.metric;

import nsa.datawave.security.authorization.DatawavePrincipal;

import java.util.Date;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Alternative;

@Alternative
@ApplicationScoped
public class NoOpQueryMetricHandler implements QueryMetricHandler {
    
    @Override
    public void updateMetric(BaseQueryMetric metric, DatawavePrincipal datawavePrincipal) throws Exception {
        
    }
    
    @Override
    public QueryMetricListResponse query(String user, String queryId, DatawavePrincipal datawavePrincipal) {
        return new QueryMetricListResponse();
    }
    
    @Override
    public QueryMetricsSummaryResponse getTotalQueriesSummaryCounts(Date begin, Date end, DatawavePrincipal datawavePrincipal) {
        return new QueryMetricsSummaryResponse();
    }
    
    @Override
    public QueryMetricsSummaryHtmlResponse getTotalQueriesSummary(Date begin, Date end, DatawavePrincipal datawavePrincipal) {
        return new QueryMetricsSummaryHtmlResponse();
    }
    
    @Override
    public QueryMetricsSummaryHtmlResponse getUserQueriesSummary(Date begin, Date end, DatawavePrincipal datawavePrincipal) {
        return new QueryMetricsSummaryHtmlResponse();
    }
    
    @Override
    public void reload() {
        
    }
    
    @Override
    public void flush() throws Exception {
        
    }
}
