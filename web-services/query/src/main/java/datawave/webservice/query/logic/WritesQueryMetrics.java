package datawave.webservice.query.logic;

import datawave.microservice.querymetric.BaseQueryMetric;

public interface WritesQueryMetrics {
    
    void writeQueryMetrics(BaseQueryMetric metric);
    
}
