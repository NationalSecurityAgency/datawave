package nsa.datawave.webservice.query.cache;

import nsa.datawave.webservice.query.metric.BaseQueryMetric;

public interface QueryMetricFactory {
    
    BaseQueryMetric createMetric();
    
}
