package datawave.webservice.query.cache;

import datawave.webservice.query.metric.BaseQueryMetric;

public interface QueryMetricFactory {
    
    BaseQueryMetric createMetric();
    
}
