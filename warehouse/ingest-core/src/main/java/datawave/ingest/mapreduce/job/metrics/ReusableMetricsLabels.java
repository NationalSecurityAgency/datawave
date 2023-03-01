package datawave.ingest.mapreduce.job.metrics;

import java.util.Map;
import java.util.TreeMap;

/**
 * A thread-safe, reusable map for metrics labels. Useful for M/R jobs.
 */
public class ReusableMetricsLabels {
    
    private ThreadLocal<Map<String,String>> threadLocalMap = ThreadLocal.withInitial(TreeMap::new);
    
    /**
     * Add a new label
     *
     * @param name
     *            a name
     * @param value
     *            a value
     */
    public void put(String name, String value) {
        get().put(name, value);
    }
    
    /**
     * Clear all labels.
     */
    public void clear() {
        get().clear();
    }
    
    /**
     * @return all of the current labels on this thread
     */
    public Map<String,String> get() {
        return threadLocalMap.get();
    }
}
