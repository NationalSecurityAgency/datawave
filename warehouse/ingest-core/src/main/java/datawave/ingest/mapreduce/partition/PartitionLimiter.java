package datawave.ingest.mapreduce.partition;

import org.apache.hadoop.conf.Configuration;

public class PartitionLimiter {
    public static final String MAX_PARTITIONS_PROPERTY = "partition.limiter.max";
    private Configuration conf;
    private int maxPartitions = 0;

    public PartitionLimiter(Configuration conf) {
        this.conf = conf;
        this.maxPartitions = conf.getInt(MAX_PARTITIONS_PROPERTY, 0); // generic setting
    }

    public void configureWithPrefix(String prefix) {
        this.maxPartitions = conf.getInt(prefix + "." + MAX_PARTITIONS_PROPERTY, 0);
        // e.g. for table x use x.partition.limiter.max of y
    }

    public int getNumPartitions() {
        return maxPartitions;
    }

    public void setMaxPartitions(int maxPartitions) {
        this.maxPartitions = maxPartitions;
    }

    public int limit(int numPartitions, int selectedPartition) {
        if (numPartitions < maxPartitions) {
            maxPartitions = Math.min(maxPartitions, numPartitions);
        }
        return selectedPartition % maxPartitions;
    }
}
