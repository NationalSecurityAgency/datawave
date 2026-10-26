package datawave.ingest.mapreduce.job;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

/**
 * Singleton cache providing shared, thread-safe access to the splits file across MapReduce tasks, avoiding repeated initialization and duplicated memory use.
 * Uses double-checked locking for lazy initialization.
 */
public interface SplitsCache extends AutoCloseable {
    /**
     * Get or create the singleton SplitsCache instance. The implementation can be customized via {@code datawave.ingest.splits.cache.impl}; defaults to
     * SplitsFile.
     *
     * @param conf
     *            the configuration to use for initialization
     * @return the singleton SplitsCache instance
     * @throws RuntimeException
     *             if implementation class cannot be instantiated
     */
    static SplitsCache getInstance(final Configuration conf) {
        return SplitsCacheFactory.getSplitsCache(conf);
    }

    /**
     * Initializing the cache
     *
     * @param conf
     *            - configuration to use
     */
    void init(Configuration conf);

    /**
     * Take the cache file and add it to the job configuration.
     *
     * @param job
     *            - the job to setup
     * @param conf
     *            - the job's own configuration; must not be the configuration captured at {@link #init(Configuration)} time, since a job created via
     *            {@code Job.getInstance(conf)} snapshots conf and later mutations to that conf object are not visible via {@code job.getConfiguration()}
     */
    void setupJob(final Job job, final Configuration conf) throws IOException;

    /**
     * Check whether the cache contains any splits.
     */
    boolean hasSplits();

    /**
     * Get the count of splits for the specified table.
     *
     * @param table
     *            - table where we wish to retrieve the count
     * @return the count of splits for the specified table, or {@code -1} if no splits are recorded for the table (e.g. an unrecognized or unconfigured table
     *         name) — distinct from {@code 0}, which means the table is recognized but genuinely has no splits
     */
    int getSplitsCount(String table);

    /**
     * Get the binary search index of the key within the sorted splits, indicating which tablet interval contains the key.
     *
     * @param table
     *            - table where we wish to look up the index location
     * @param key
     *            - key to look up
     * @return a positive index if the key exactly matches a split, or a negative value (-insertionPoint - 1) indicating the tablet interval
     */
    int getExactIndex(String table, Text key);

    /**
     * Obtain the partition that is storing the key, based on the table mappings.
     *
     * @param table
     *            - table where we wish to look up the shard
     * @param shardId
     *            - shard ID to look up
     * @return the assigned partition, or {@code -1} if shardId has no assigned partition (a miss) — callers should fall back to another strategy rather than
     *         treat {@code -1} as a real partition
     */
    int getExactPartition(String table, Text shardId);

    /**
     * Obtain the partition that is storing the key, and if unable to get an exact match find the nearest partition.
     *
     * @param table
     *            - table where we wish to look up the key
     * @param key
     *            - key to look up
     * @return the exact or nearest assigned partition; if the table has no assignments at all, falls back to a hash-based partition of key instead of an exact
     *         or nearest match
     */
    int getNearestPartition(String table, Text key);

    /**
     * Look up the tablet server location which is serving the rowkey - if no location is found then use the rowkey as the default.
     *
     * @param table
     *            - the table to look up
     * @param key
     *            - the row key to search for
     * @param defaultFn
     *            - the callback function to return the rowkey if nothing is found via the lookup
     * @return the name of the tserver
     */
    String getExactLocation(String table, Text key, Supplier<String> defaultFn);

    /**
     * Retrieve the splits from the requested table
     *
     * @param tableName
     *            - name of the table
     * @return the list of splits
     */
    List<Text> getSplits(String tableName) throws IOException;

    Map<Text,String> getSplitsAndLocationByTable(String table) throws IOException;
}
