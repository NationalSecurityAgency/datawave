package datawave.webservice.query.cache;

import datawave.webservice.query.metric.BaseQueryMetric;
import datawave.webservice.query.metric.QueryMetric;

public class QueryMetricFactoryImpl implements QueryMetricFactory {
    
    @Override
    public BaseQueryMetric createMetric() {
        return new QueryMetric();
    }
    
}
