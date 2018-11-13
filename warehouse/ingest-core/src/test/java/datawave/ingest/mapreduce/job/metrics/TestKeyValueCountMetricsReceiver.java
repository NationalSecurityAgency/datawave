package datawave.ingest.mapreduce.job.metrics;

import com.google.common.collect.Multimap;
import datawave.ingest.data.config.NormalizedContentInterface;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * A {@link MetricsReceiver} for handling the key/value count metrics. Used for testing.
 */
public class TestKeyValueCountMetricsReceiver<OK,OV> extends BaseMetricsReceiver<OK,OV> {
    
    public TestKeyValueCountMetricsReceiver() {
        super(Metric.KV_PER_TABLE);
    }
    
    @Override
    protected Iterable<String> constructKeys(Metric metric, Map<String,String> labels, Multimap<String,NormalizedContentInterface> fields) {
        List<String> keys = new LinkedList<>();
        
        String table = labels.get("table");
        
        for (String field : getFieldNames()) {
            for (NormalizedContentInterface entry : fields.get(field)) {
                String fieldAndValue = field + QUAL_DELIM + entry.getEventFieldValue();
                String keyStr = KeyConverter.toString(getShardId(fieldAndValue), getMetricName(), fieldAndValue + QUAL_DELIM + table, getVisibility());
                keys.add(keyStr);
            }
        }
        
        return keys;
    }
}
