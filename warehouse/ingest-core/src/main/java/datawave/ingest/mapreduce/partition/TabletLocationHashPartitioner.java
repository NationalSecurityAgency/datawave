package datawave.ingest.mapreduce.partition;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.ShardedTableMapFile;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.log4j.Logger;

/**
 * The TabletLocationHashPartitioner will generate partitions for the shard table using the hashCode method on the tserver location string
 */
public class TabletLocationHashPartitioner extends Partitioner<BulkIngestKey,Value> implements Configurable, DelegatePartitioner {
    private static final Logger log = Logger.getLogger(TabletLocationHashPartitioner.class);

    private Configuration conf;
    private Map<String,Map<Text,Integer>> shardHashes;

    /**
     * Given a map of shard IDs to tablet server locations, this method determines a partition for a given key's shard ID. The goal is that we want to ensure
     * that all shard IDs served by a given tablet server get sent to the same reducer. To do this, we look up where the shard ID is supposed to be stored and
     * use a hash of that (modded by the number of reduces) to come up with the final allocation. This mapping needs to be computed at job startup and, so long
     * as no migration goes on during a job, will produce a single map file per tablet server. Note that it is also possible that we receive data for a day that
     * hasn't been loaded yet. In that case, we'll just hash the shard ID and send data to that reducer. This will spread out the data for a given day, but the
     * map files produced for it will not belong to any given tablet server. So, in the worst case, we have other older data when is already assigned to a
     * tablet server and new data which is not. In this case, we'd end up sending two map files to each tablet server. Of course, if tablets get moved around
     * between when the job starts and the map files are loaded, then we may end up sending multiple map files to each tablet server.
     */
    @Override
    public synchronized int getPartition(BulkIngestKey key, Value value, int numReduceTasks) {
        try {
            Text shardId = key.getKey().getRow();
            Map<Text,Integer> shardHash = getShardHashes(key.getTableName().toString());
            Integer hash = shardHash.get(shardId);
            if (hash != null) {
                return (hash & Integer.MAX_VALUE) % numReduceTasks;
            } else {
                return (shardId.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    /**
     * hashCode of the tserver name Read in the sequence file (that was created at job startup) that contains a list of shard IDs and the corresponding tablet
     * server to which that shard is assigned. The hash is a simple hashCode of the location string.
     *
     * @param tableName
     *            the table name
     * @throws IOException
     *             for issues with read or write
     * @return a mapping of the shard hashes
     */
    private Map<Text,Integer> getShardHashes(String tableName) throws IOException {
        if (this.shardHashes == null) {
            this.shardHashes = new HashMap<>();
        }

        if (null == this.shardHashes.get(tableName)) {
            Map<Text,Integer> hashedForTable = new HashMap<>();

            for (Map.Entry<Text,String> entry : ShardedTableMapFile.getShardIdToLocations(conf, tableName).entrySet()) {
                hashedForTable.put(entry.getKey(), entry.getValue().toString().hashCode());
            }

            this.shardHashes.put(tableName, hashedForTable);
        }

        return this.shardHashes.get(tableName);
    }

    @Override
    public void configureWithPrefix(String prefix) { /* no op */}

    @Override
    public int getNumPartitions() {
        return Integer.MAX_VALUE;
    }

    @Override
    public void initializeJob(Job job) {}
}
