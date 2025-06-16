package datawave.ingest.mapreduce.partition;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.log4j.Logger;

import datawave.ingest.mapreduce.handler.shard.ShardIdFactory;
import datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.SplitsCache;

/**
 * The BalancedShardPartitioner takes advantage of the way that shards are balanced. See ShardedTableTabletBalancer. * The partitioner is designed to have no
 * collisions for today and for previous days, the number of days depends on the cluster size and its balance (see below). Moving beyond that threshold,
 * collisions will start to occur. * For the shard tables, the previously used partitioner was TabletLocationHashPartitioner. It guaranteed that shards hosted
 * on the same tablet server would go to the same reducer. This partitioner also guarantees that. The hash approach didn't take into account the balanced nature
 * of each day's shards and the hashCode function caused collisions for a given day's shards. * The number of days without collisions = floor(min(r, ts) / x)
 * 'ts' = number of tablet servers, 'r' = number of partitioners 'x' = number of shards in a given day * Depends on the ShardedTableMapFile for getting splits
 * and for identifying the tables it might see (ShardedTableMapFile.CONFIGURED_SHARDED_TABLE_NAMES), also assumes that the num of shard ids property is set.
 */
public class BalancedShardPartitioner extends Partitioner<BulkIngestKey,Value> implements Configurable, DelegatePartitioner {
    private static final Logger log = Logger.getLogger(BalancedShardPartitioner.class);
    private Configuration conf;
    private Map<String,Map<Text,Integer>> shardPartitionsByTable;
    private Map<Text,Integer> offsetsFactorByTable;
    private SplitsCache splitsCache;
    private String missingShardStrategy;
    int missingShardIdCount = 0;

    public static final String MISSING_SHARD_STRATEGY_PROP = "datawave.ingest.mapreduce.partition.BalancedShardPartitioner.missing.shard.strategy";

    private ShardIdFactory shardIdFactory = null;

    @Override
    public int getPartition(BulkIngestKey key, Value value, int numReduceTasks) {
        try {
            // partition will be balanced for a given day, more so for recent days
            int partition = getAssignedPartition(key.getTableName().toString(), key.getKey().getRow());

            // the offsets should help send today's shard data to a different set of reducers than today's error shard data
            if (null == offsetsFactorByTable.get(key.getTableName())) {
                log.error("We have received a key for a table we are not configured for.  Please verify your sharded table configurations.");
            }
            int offsetForTable = shardIdFactory.getNumShards(key.getKey().getTimestamp()) * offsetsFactorByTable.get(key.getTableName());

            return (partition + offsetForTable) % numReduceTasks;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private int getAssignedPartition(String tableName, Text shardId) throws IOException {
        // if the partitionId is not there, either shards were not created for the day
        // or not all shards were created for the day

        switch (missingShardStrategy) {
            case "hash":
                int partition = splitsCache.getExactPartition(tableName, shardId);
                if (partition >= 0) {
                    return partition;
                }
                // only warn a few times per partitioner to avoid flooding the logs
                if (missingShardIdCount < 10) {
                    log.warn("shardId didn't have a partition assigned to it: " + shardId);
                    missingShardIdCount++;
                }
                return (shardId.hashCode() & Integer.MAX_VALUE);
            case "collapse":
                return splitsCache.getNearestPartition(tableName, shardId);
            default:
                throw new RuntimeException("Unsupported missing shard strategy " + MISSING_SHARD_STRATEGY_PROP + "=" + missingShardStrategy);
        }
    }

    @Override
    public void configureWithPrefix(String prefix) {/* no op */}

    @Override
    public int getNumPartitions() {
        return Integer.MAX_VALUE;
    }

    @Override
    public void initializeJob(Job job) {}

    @Override
    public boolean needSplits() {
        return true;
    }

    @Override
    public boolean needSplitLocations() {
        return true;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        shardIdFactory = new ShardIdFactory(conf);
        missingShardStrategy = conf.get(MISSING_SHARD_STRATEGY_PROP, "hash");
        splitsCache = SplitsCache.getInstance(conf);

        defineOffsetsForTables(conf);
    }

    private void defineOffsetsForTables(Configuration conf) {
        offsetsFactorByTable = new HashMap<>();
        int offsetFactor = 0;
        for (String tableName : conf.getStrings(ShardedDataTypeHandler.SHARDED_TNAMES)) {
            offsetsFactorByTable.put(new Text(tableName), offsetFactor++);
        }
    }
}
