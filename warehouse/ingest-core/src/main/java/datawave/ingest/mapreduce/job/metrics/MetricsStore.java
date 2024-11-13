package datawave.ingest.mapreduce.job.metrics;

/**
 * The storage used by the MetricsService. Needs to be thread-safe.
 */
public interface MetricsStore<OK,OV> extends AutoCloseable {

    /**
     * Increase the count for the given key.
     *
     * @param key
     *            the key string
     * @param count
     *            the count
     */
    void increase(String key, long count);
}
