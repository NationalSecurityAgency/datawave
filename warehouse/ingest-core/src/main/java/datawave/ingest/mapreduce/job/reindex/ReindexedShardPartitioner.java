package datawave.ingest.mapreduce.job.reindex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;

import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.partition.DelegatePartitioner;

/**
 * this partitioner is to be used with the ShardReindexJob to prevent hotspots on reducers for sharded data
 */
public class ReindexedShardPartitioner extends Partitioner<BulkIngestKey,Value> implements Configurable, DelegatePartitioner {
    public static final String MAX_PARTITIONS = "reindex.max.partitions";
    public static final String MIN_KEYS = "reindex.max.keys";

    private Configuration config;

    // evenly spread keys across this many reducers max
    // suggest setting this as a multiple of the accumulo property for MajC (maxOpenFiles-1)
    private int maxPartitions = 7;

    // if configured only add another partition when the current partition already has partitioned at least this many keys
    private int minKeys = -1;

    private Map<ByteSequence,List<PartitionTracker>> partitionOffsetMap;

    @Override
    public void configureWithPrefix(String prefix) {
        // no-op
        maxPartitions = config.getInt(prefix + "." + MAX_PARTITIONS, maxPartitions);
        minKeys = config.getInt(prefix + "." + MIN_KEYS, minKeys);
    }

    @Override
    public int getNumPartitions() {
        return 0;
    }

    @Override
    public void initializeJob(Job job) {
        // no-op
    }

    @Override
    public boolean needSplits() {
        return false;
    }

    @Override
    public boolean needSplitLocations() {
        return false;
    }

    @Override
    public void setConf(Configuration config) {
        this.config = config;
    }

    @Override
    public Configuration getConf() {
        return config;
    }

    private PartitionTracker getNextPartition(List<PartitionTracker> partitions) {
        PartitionTracker lowest = partitions.get(0);

        for (int i = 1; i < partitions.size(); i++) {
            if (partitions.get(i).getCount() < lowest.getCount()) {
                return partitions.get(i);
            }
        }

        // every partition has the same count
        return lowest;
    }

    /**
     * Get the next partition to assign the row to
     *
     * @param row
     * @param partitions
     * @param reducers
     * @return
     */
    private PartitionTracker getNextPartition(ByteSequence row, List<PartitionTracker> partitions, int reducers) {
        // check if the last partition is beyond the min to add another partition
        int partitionCount = partitions.size();
        PartitionTracker last = partitions.get(partitionCount - 1);
        if (last.getCount() < minKeys) {
            return last;
        } else {
            // check if the current partitions are maxed
            if (partitionCount < maxPartitions) {
                PartitionTracker next = allocatePartition(row, partitions.size(), reducers);
                if (next != null) {
                    partitions.add(next);
                    return next;
                }
            }

            // already maxed, so round robin
            return getNextPartition(partitions);
        }
    }

    /**
     * Allocates a new partition based on the hash of the row, incrementing by 1 for each subsequent allocation for the same row wrapping based on the number of
     * reducers
     *
     * @param row
     * @param count
     * @param reducers
     * @return
     */
    private PartitionTracker allocatePartition(ByteSequence row, int count, int reducers) {
        if (count >= reducers) {
            // cannot allocate, already have as many as we can have
            return null;
        }

        int basePartition = (row.hashCode() & Integer.MAX_VALUE) % reducers;
        int targetPartition = (basePartition + count) % reducers;

        return new PartitionTracker(targetPartition);
    }

    private int getPartition(ByteSequence row, int reducers) {
        if (partitionOffsetMap == null) {
            partitionOffsetMap = new HashMap<>();
        }

        List<PartitionTracker> partitions = partitionOffsetMap.computeIfAbsent(row, k -> new ArrayList<>());
        PartitionTracker pt;
        if (partitions.isEmpty()) {
            pt = allocatePartition(row, 0, reducers);
            if (pt == null) {
                throw new IllegalStateException("failed to allocate initial partition");
            }
            partitions.add(pt);
        } else {
            pt = getNextPartition(row, partitions, reducers);
        }
        pt.increment();
        return pt.getPartition();
    }

    @Override
    public int getPartition(BulkIngestKey bulkIngestKey, Value value, int reducers) {
        Key key = bulkIngestKey.getKey();
        ByteSequence row = key.getRowData();

        return getPartition(row, reducers);
    }

    private static class PartitionTracker {
        private final int partition;
        private long counter;

        private PartitionTracker(int partition) {
            this.partition = partition;
        }

        public int getPartition() {
            return partition;
        }

        public void increment() {
            ++counter;
        }

        public long getCount() {
            return counter;
        }
    }
}
