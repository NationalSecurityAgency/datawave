package datawave.metrics.analytic;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Partitions based on the row (which for metrics is a date to the day for daily summaries or date to the hour for hourly summaries) in order to guarantee that
 * all metric name/value pairs for a single time unit go to the same reducer so that they may be aggregated together.
 */
public class RowPartitioner extends Partitioner<Key,Value> {
    private Text row = new Text();

    @Override
    public int getPartition(Key key, Value value, int numPartitions) {
        key.getRow(row);
        return (row.hashCode() >>> 1) % numPartitions;
    }

    public static void configureJob(Job job) {
        job.setPartitionerClass(RowPartitioner.class);
    }
}
