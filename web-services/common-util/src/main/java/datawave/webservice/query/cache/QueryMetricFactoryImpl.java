package datawave.webservice.query.cache;

import datawave.webservice.query.metric.QueryMetric;

public class QueryMetricFactoryImpl implements QueryMetricFactory {
    
    @Override
    public QueryMetric createMetric() {
        return new QueryMetric();
    }
    
}
