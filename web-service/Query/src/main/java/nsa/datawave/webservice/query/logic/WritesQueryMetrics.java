package nsa.datawave.webservice.query.logic;

import nsa.datawave.webservice.query.metric.BaseQueryMetric;

public interface WritesQueryMetrics {
    
    public void writeQueryMetrics(BaseQueryMetric metric);
    
}
