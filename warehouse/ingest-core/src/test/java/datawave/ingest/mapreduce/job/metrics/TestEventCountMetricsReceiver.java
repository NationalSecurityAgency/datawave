package datawave.ingest.mapreduce.job.metrics;

/**
 * A metrics receiver for the number of records.
 */
public class TestEventCountMetricsReceiver<OK,OV> extends BaseMetricsReceiver<OK,OV> {
    
    public TestEventCountMetricsReceiver() {
        super(Metric.EVENT_COUNT);
    }
}
