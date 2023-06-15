package datawave.ingest.mapreduce.partition;

import datawave.ingest.mapreduce.job.BulkIngestKey;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.log4j.Logger;

/**
 * Partitions a key into a defined number of bins instead of the entire reducer space This is meant to be used for tables that don't benefit from the row splits
 * approach that we use elsewhere but don't have enough data to justify creating many small rfiles per job
 */
public class LimitedKeyPartitioner extends Partitioner<BulkIngestKey,Value> implements Configurable, DelegatePartitioner {
    private static Logger log = Logger.getLogger(LimitedKeyPartitioner.class);
    private Configuration conf;
    private PartitionLimiter partitionLimiter;

    @Override
    public int getPartition(BulkIngestKey bKey, Value value, int numPartitions) {
        // ensure this returns a positive value (note Math.abs does not always return a positive number, go figure)
        int partitioner = getKeyHashcode(bKey) & Integer.MAX_VALUE;
        return partitionLimiter.limit(numPartitions, partitioner);
    }

    /**
     * Compute the hash on the bulk ingest key. Override this to get custom behavior. The result of this will be AND'd with Integer.MAX_VALUE yielding an always
     * positive number.
     *
     * @param bKey
     *            the bulk ingest key
     * @return the hashcode of the key
     */
    protected int getKeyHashcode(BulkIngestKey bKey) {
        return bKey.hashCode();
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        partitionLimiter = new PartitionLimiter(conf);
    }

    @Override
    public void configureWithPrefix(String prefix) {
        partitionLimiter.configureWithPrefix(prefix);
    }

    @Override
    public int getNumPartitions() {
        return partitionLimiter.getNumPartitions();
    }

    @Override
    public void initializeJob(Job job) {
        // no op
    }
}
