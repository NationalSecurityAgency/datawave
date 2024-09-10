package datawave.ingest.mapreduce.partition;

import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import datawave.ingest.mapreduce.job.BulkIngestKey;

/**
 * This partitioner will read the accumulo splits for its table.
 *
 * Rather than just partitioning a table's row based on which split it should go into, it will apply a multiplier to the index, based on a configurable number.
 *
 * In other words, if you configure this partitioner to double the partitioner space, then data for split 0 will go to partitions 0 or 1. Similarly, if you
 * configure the partitioner to quadruple the partitioner space, then data for split 1 will go into partitions 4, 5, 6 or 7.
 *
 * Be careful with the multiplier, because a given table will receive that many more files per job.
 *
 * If the partitioner space runs out, it will wrap. The wrapping may cause a file to get imported into multiple tablets.
 *
 * This was designed to help with time-based tables that get a lot of data in a small subset of its splits and nearly nothing in its others, e.g. dqr
 */
public class SplitBasedHashPartitioner extends MultiTableRangePartitioner implements Configurable, DelegatePartitioner {
    public static final String PARTITIONER_SPACE_MULTIPLIER = "split.based.hash.partitioner.multiplier";
    private static Logger log = Logger.getLogger(SplitBasedHashPartitioner.class);
    private int spaceMultiplier = 0;

    public void setMultiplier(int multiplier) {
        this.spaceMultiplier = multiplier;
    }

    @Override
    public int getPartition(BulkIngestKey bKey, Value value, int numPartitions) {
        int indexWithinSplits = super.getPartition(bKey, value, numPartitions);
        // ensure this returns a positive value. note Math.abs does not always return a positive number
        int offsetWithinPartitionerSpace = (bKey.getKey().hashCode() & Integer.MAX_VALUE) % spaceMultiplier;
        int selectedPartitioner = indexWithinSplits * spaceMultiplier + offsetWithinPartitionerSpace;
        return selectedPartitioner % numPartitions;
    }

    @Override
    public void setConf(Configuration conf) {
        super.setConf(conf);
        this.spaceMultiplier = conf.getInt(PARTITIONER_SPACE_MULTIPLIER, 2); // generic property
    }

    @Override
    public void configureWithPrefix(String prefix) {
        super.configureWithPrefix(prefix);
        // use table specific property, if available
        this.spaceMultiplier = super.getConf().getInt(prefix + "." + PARTITIONER_SPACE_MULTIPLIER, this.spaceMultiplier);
        // e.g. for table x use x.split.based.hash.partitioner.multiplier of y
    }

    @Override
    protected int calculateIndex(int index, int numPartitions, String tableName, int cutPointArrayLength) {
        // will wrap indexes
        return super.calculateIndex(index, numPartitions, tableName, cutPointArrayLength) % numPartitions;
    }

}
