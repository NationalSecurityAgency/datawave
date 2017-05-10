package datawave.ingest.mapreduce.job.metrics;

/**
 * The currently generated metrics. If you want to track them, implement the corresponding MetricsReceiver.
 */
public enum Metric {
    KV_PER_TABLE, EVENT_COUNT, BYTE_COUNT, MILLIS_IN_HANDLER, MILLIS_IN_EVENT_MAPPER
}
