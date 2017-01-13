package nsa.datawave.ingest.mapreduce.job.metrics;

/**
 * The storage used by the MetricsService. Needs to be thread-safe.
 */
public interface MetricsStore<OK,OV> extends AutoCloseable {
    
    /**
     * Increase the count for the given key.
     *
     * @param key
     * @param count
     */
    void increase(String key, long count);
}
