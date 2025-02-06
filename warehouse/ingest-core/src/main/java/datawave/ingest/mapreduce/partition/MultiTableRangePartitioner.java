package datawave.ingest.mapreduce.partition;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.log4j.Logger;

import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.SplitsFile;

/**
 * Range partitioner that uses a split file with the format: {@code tableName<tab>splitPoint<tab>tabletLocation}
 *
 */
public class MultiTableRangePartitioner extends Partitioner<BulkIngestKey,Value> implements DelegatePartitioner {
    private static final String PREFIX = MultiTableRangePartitioner.class.getName();
    public static final String PARTITION_STATS = PREFIX + ".partitionStats";

    private static final Logger log = Logger.getLogger(MultiTableRangePartitioner.class);
    static TaskInputOutputContext<?,?,?,?> context = null;
    private static boolean collectStats = false;

    protected volatile boolean cacheFilesRead = false;

    private Text holder = new Text();
    private DecimalFormat formatter = new DecimalFormat("000");
    private Configuration conf;
    private PartitionLimiter partitionLimiter;
    protected Object semaphore = new Object();

    private void readCacheFilesIfNecessary() {
        if (cacheFilesRead) {
            return;
        }

        synchronized (semaphore) {
            if (cacheFilesRead) {
                return;
            }

            Path[] localCacheFiles;

            try {
                // Moved the deprecation call from NonShardedSplitsFile to simplify testing
                // We need a replacement that isn't deprecated, but context.getCacheFiles() returns paths that are not local
                // No Hadoop documentation seems to indicate what is the correct replacement for this method
                localCacheFiles = context.getLocalCacheFiles();
            } catch (Exception e) {
                log.error("Failed to get localCacheFiles from context", e);
                throw new RuntimeException("Failed to get localCacheFiles from context", e);
            }

            try {
                if (SplitsFile.getSplits(conf).isEmpty()) {
                    log.error("Non-sharded splits by table cannot be empty.  If this is a development system, please create at least one split in one of the non-sharded tables (see bin/ingest/seed_index_splits.sh).");
                    throw new IOException("splits by table cannot be empty");
                }
            } catch (IOException e) {
                log.error("Failed to read splits in MultiTableRangePartitioner: cache files: " + Arrays.toString(localCacheFiles), e);
                throw new RuntimeException("Failed to read splits in MultiTableRangePartitioner, fatal error. cache files: " + Arrays.toString(localCacheFiles),
                                e);
            }
            cacheFilesRead = true;
        }
    }

    @Override
    public int getPartition(BulkIngestKey key, Value value, int numPartitions) {
        readCacheFilesIfNecessary();

        String tableName = key.getTableName().toString();

        List<Text> cutPointArray = null;
        try {
            cutPointArray = SplitsFile.getSplits(conf, tableName);
        } catch (IOException e) {
            log.error("Failed to read splits in MultiTableRangePartitioner for  " + tableName);
        }
        if (null == cutPointArray) {
            return (tableName.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
        key.getKey().getRow(holder);
        int index = Collections.binarySearch(cutPointArray, holder);
        index = calculateIndex(index, numPartitions, tableName, cutPointArray.size());

        index = partitionLimiter.limit(numPartitions, index);

        TaskInputOutputContext<?,?,?,?> c = context;
        if (c != null && collectStats) {
            c.getCounter("Partitions: " + key.getTableName(), "part." + formatter.format(index)).increment(1);
        }

        return index;
    }

    protected int calculateIndex(int index, int numPartitions, String tableName, int cutPointArrayLength) {

        return index < 0 ? (index + 1) * -1 : index;

    }

    public static void setContext(TaskInputOutputContext<?,?,?,?> context) {
        MultiTableRangePartitioner.context = context;
        collectStats = (context != null) && context.getConfiguration().getBoolean(PARTITION_STATS, false);
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        partitionLimiter = new PartitionLimiter(conf);
        if (partitionLimiter.getNumPartitions() == 0) {
            partitionLimiter.setMaxPartitions(Integer.MAX_VALUE);
        }
    }

    // There could be multiple instances of this partitioner, for different tables.
    // Each may have a different setting
    @Override
    public void configureWithPrefix(String prefix) {
        int originalMax = partitionLimiter.getNumPartitions();
        partitionLimiter.configureWithPrefix(prefix);
        if (0 == partitionLimiter.getNumPartitions()) {
            partitionLimiter.setMaxPartitions(originalMax);
        }
    }

    @Override
    public int getNumPartitions() {
        return partitionLimiter.getNumPartitions();
    }

    @Override
    public void initializeJob(Job job) {
        // noop
    }

    @Override
    public boolean needSplits() {
        return true;
    }

    @Override
    public boolean needSplitLocations() {
        return false;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }
}
