package datawave.webservice.query.logic;

import datawave.webservice.query.metric.BaseQueryMetric;

public interface WritesQueryMetrics {
    
    public void writeQueryMetrics(BaseQueryMetric metric);
    
}
