package datawave.microservice.querymetric;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.merge.MergingLastUpdateTime;
import com.hazelcast.spi.merge.MergingValue;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;

import datawave.microservice.querymetric.handler.QueryMetricCombiner;

public class QueryMetricSplitBrainMergePolicy<V extends QueryMetricUpdateHolder,T extends MergingValue<V> & MergingLastUpdateTime,R extends QueryMetricUpdateHolder>
                implements SplitBrainMergePolicy<V,T,R> {
    
    private final Logger log = LoggerFactory.getLogger(getClass());
    protected QueryMetricCombiner queryMetricCombiner;
    
    public QueryMetricSplitBrainMergePolicy() {
        this.queryMetricCombiner = getQueryMetricCombiner();
    }
    
    protected QueryMetricCombiner getQueryMetricCombiner() {
        return new QueryMetricCombiner();
    }
    
    @Override
    public R merge(T mergingValue, T existingValue) {
        QueryMetricUpdateHolder mergedValue;
        if (existingValue == null) {
            mergedValue = mergingValue.getValue();
            if (log.isTraceEnabled()) {
                log.trace("Merged metrics existing is null, using merging: " + mergedValue.getMetric());
            } else {
                log.debug("Merged metric: " + mergedValue.getMetric().getQueryId());
            }
        } else {
            
            QueryMetricUpdateHolder merging = mergingValue.getValue();
            QueryMetricUpdateHolder existing = existingValue.getValue();
            
            try {
                BaseQueryMetric metric = this.queryMetricCombiner.combineMetrics(merging.getMetric(), existing.getMetric(), existing.getMetricType());
                mergedValue = new QueryMetricUpdateHolder(metric, existing.getMetricType());
            } catch (Exception e) {
                if (existingValue.getLastUpdateTime() >= mergingValue.getLastUpdateTime()) {
                    mergedValue = existing;
                } else {
                    mergedValue = merging;
                }
            }
            if (log.isTraceEnabled()) {
                log.trace("Merged metrics existing: " + existing.getMetric() + " merging: " + merging.getMetric() + " merged: " + mergedValue.getMetric());
            } else {
                log.debug("Merged metric: " + mergedValue.getMetric().getQueryId());
            }
        }
        
        return (R) mergedValue;
    }
    
    @Override
    public void readData(ObjectDataInput in) {
        
    }
    
    @Override
    public void writeData(ObjectDataOutput out) {
        
    }
}
