package datawave.microservice.querymetric;

public interface QueryMetricFactory {
    
    BaseQueryMetric createMetric();
    
    BaseQueryMetric createMetric(boolean populateVersionMap);
}
