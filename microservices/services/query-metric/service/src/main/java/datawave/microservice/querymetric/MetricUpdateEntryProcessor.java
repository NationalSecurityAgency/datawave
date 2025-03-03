package datawave.microservice.querymetric;

import java.util.Map;

import com.hazelcast.map.EntryProcessor;

import datawave.microservice.querymetric.handler.QueryMetricCombiner;

public class MetricUpdateEntryProcessor implements EntryProcessor<String,QueryMetricUpdateHolder,Long> {
    
    private QueryMetricCombiner combiner;
    private QueryMetricUpdateHolder metricUpdate;
    
    public MetricUpdateEntryProcessor(QueryMetricUpdateHolder metricUpdate, QueryMetricCombiner combiner) {
        this.metricUpdate = metricUpdate;
        this.combiner = combiner;
    }
    
    @Override
    public Long process(Map.Entry<String,QueryMetricUpdateHolder> entry) {
        QueryMetricUpdateHolder storedHolder;
        QueryMetricType metricType = this.metricUpdate.getMetricType();
        BaseQueryMetric updatedMetric = this.metricUpdate.getMetric();
        long start = System.currentTimeMillis();
        if (entry.getValue() == null) {
            storedHolder = this.metricUpdate;
        } else {
            storedHolder = entry.getValue();
            BaseQueryMetric storedMetric = storedHolder.getMetric();
            BaseQueryMetric combinedMetric;
            combinedMetric = this.combiner.combineMetrics(updatedMetric, storedMetric, metricType);
            storedHolder.setMetric(combinedMetric);
            storedHolder.setMetricType(metricType);
            storedHolder.updateLowestLifecycle(this.metricUpdate.getLowestLifecycle());
        }
        
        if (metricType.equals(QueryMetricType.DISTRIBUTED) && updatedMetric != null) {
            // these values are added incrementally in a distributed update. Because we can not be sure
            // exactly when the incomingQueryMetricCache value is stored, it would otherwise be possible
            // for updates to be included twice. These values are reset after being used in the AccumuloMapStore
            storedHolder.addValue("sourceCount", updatedMetric.getSourceCount());
            storedHolder.addValue("nextCount", updatedMetric.getNextCount());
            storedHolder.addValue("seekCount", updatedMetric.getSeekCount());
            storedHolder.addValue("yieldCount", updatedMetric.getYieldCount());
            storedHolder.addValue("docSize", updatedMetric.getDocSize());
            storedHolder.addValue("docRanges", updatedMetric.getDocRanges());
            storedHolder.addValue("fiRanges", updatedMetric.getFiRanges());
        }
        entry.setValue(storedHolder);
        return Long.valueOf(System.currentTimeMillis() - start);
    }
}
