package datawave.ingest.mapreduce.partition;

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configurable;
import org.apache.log4j.Logger;

import datawave.ingest.mapreduce.job.BulkIngestKey;

/**
 * The CachingShardIdPartitioner will generate partitions using ShardIdPartitioner's algorithm and cache the result
 */
public class CachingShardIdPartitioner extends ShardIdPartitioner implements Configurable, DelegatePartitioner {
    private static final Logger log = Logger.getLogger(CachingShardIdPartitioner.class);
    private Map<String,Integer> shardPartitions = new HashMap<>();

    @Override
    public synchronized int getPartition(BulkIngestKey key, Value value, int numReduceTasks) {
        String shardId = key.getKey().getRow().toString();

        try {
            if (null != shardPartitions.get(shardId)) {
                return shardPartitions.get(shardId);
            }
            long shardIndex = generateNumberForShardId(shardId, getBaseTime());
            int partition = (int) (shardIndex % numReduceTasks);
            shardPartitions.put(shardId, partition);
            return partition;

        } catch (Exception e) {
            return (shardId.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }
}
