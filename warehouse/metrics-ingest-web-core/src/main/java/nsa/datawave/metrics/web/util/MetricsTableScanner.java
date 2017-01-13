package nsa.datawave.metrics.web.util;

import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

/**
 * This class will take a scanner and create summaries specific to each data type returned. The summaries are capable of generating summaries and time series
 * and can be used to generate text or graphs.
 * 
 */
public class MetricsTableScanner {
    
    public Map<String,MetricsSummary> scan(Iterable<Entry<Key,Value>> scan) {
        TreeMap<String,MetricsSummary> typeSummaries = new TreeMap<>();
        for (Entry<Key,Value> entry : scan) {
            Key key = entry.getKey();
            String dataType = key.getColumnFamily().toString();
            // get the summary and add the tuple to it
            MetricsSummary summary = typeSummaries.get(dataType);
            if (summary == null) {
                summary = new MetricsSummary();
                typeSummaries.put(dataType, summary);
            }
            summary.addEntry(entry.getValue());
        }
        
        return typeSummaries;
    }
}
