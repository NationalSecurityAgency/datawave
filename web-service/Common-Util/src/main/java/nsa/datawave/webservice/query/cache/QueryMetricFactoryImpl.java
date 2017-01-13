package nsa.datawave.webservice.query.cache;

import nsa.datawave.webservice.query.metric.BaseQueryMetric;
import nsa.datawave.webservice.query.metric.QueryMetric;

public class QueryMetricFactoryImpl implements QueryMetricFactory {
    
    @Override
    public BaseQueryMetric createMetric() {
        return new QueryMetric();
    }
    
}
