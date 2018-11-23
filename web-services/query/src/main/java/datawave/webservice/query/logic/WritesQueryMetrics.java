package datawave.webservice.query.logic;

import datawave.webservice.query.metric.BaseQueryMetric;

public interface WritesQueryMetrics {
    
    void writeQueryMetrics(BaseQueryMetric metric);
    
}
