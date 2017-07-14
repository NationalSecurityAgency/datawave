package datawave.webservice.query.metric;

import java.util.Date;
import java.util.Map;

import datawave.security.authorization.DatawavePrincipal;

public interface QueryMetricHandler<T extends BaseQueryMetric> {
    
    void updateMetric(T metric, DatawavePrincipal datawavePrincipal) throws Exception;
    
    public Map<String,String> getEventFields(BaseQueryMetric queryMetric);
    
    BaseQueryMetricListResponse<T> query(String user, String queryId, DatawavePrincipal datawavePrincipal);
    
    QueryMetricsSummaryResponse getTotalQueriesSummaryCounts(Date begin, Date end, DatawavePrincipal datawavePrincipal);
    
    QueryMetricsSummaryHtmlResponse getTotalQueriesSummary(Date begin, Date end, DatawavePrincipal datawavePrincipal);
    
    QueryMetricsSummaryHtmlResponse getUserQueriesSummary(Date begin, Date end, DatawavePrincipal datawavePrincipal);
    
    void flush() throws Exception;
    
    /**
     * Tells this handler to reload any dependent resources. This method might be called in the event of a failed write or flush to re-open any connections to
     * external resources such as Accumulo.
     */
    void reload();
}
