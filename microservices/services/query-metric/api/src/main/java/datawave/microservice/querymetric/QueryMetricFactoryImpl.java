package datawave.microservice.querymetric;

public class QueryMetricFactoryImpl implements QueryMetricFactory {
    
    @Override
    public BaseQueryMetric createMetric() {
        return createMetric(true);
    }
    
    @Override
    public BaseQueryMetric createMetric(boolean populateVersionMap) {
        BaseQueryMetric queryMetric = new QueryMetric();
        if (populateVersionMap) {
            queryMetric.populateVersionMap();
        }
        return queryMetric;
    }
}
