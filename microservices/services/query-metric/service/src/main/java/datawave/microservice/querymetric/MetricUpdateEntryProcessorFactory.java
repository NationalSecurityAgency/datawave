package datawave.microservice.querymetric;

import datawave.microservice.querymetric.handler.QueryMetricCombiner;

public class MetricUpdateEntryProcessorFactory {
    
    private QueryMetricCombiner combiner;
    
    public MetricUpdateEntryProcessorFactory(QueryMetricCombiner combiner) {
        this.combiner = combiner;
    }
    
    MetricUpdateEntryProcessor createEntryProcessor(QueryMetricUpdateHolder metricUpdate) {
        return new MetricUpdateEntryProcessor(metricUpdate, combiner);
    }
}
