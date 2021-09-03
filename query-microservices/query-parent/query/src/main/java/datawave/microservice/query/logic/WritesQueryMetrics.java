package datawave.microservice.query.logic;

import datawave.webservice.query.metric.BaseQueryMetric;

public interface WritesQueryMetrics {
    
    void writeQueryMetrics(BaseQueryMetric metric);
    
    public long getSourceCount();
    
    public long getNextCount();
    
    public long getSeekCount();
    
    public long getYieldCount();
    
    public long getDocRanges();
    
    public long getFiRanges();
    
    public void resetMetrics();
}
