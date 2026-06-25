package datawave.ingest.mapreduce.job;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

/**
 * The SplitsCache allows us to keep a reference to the splits file for more streamlined access without the need of multiple initiations throughout the
 * codebase.
 *
 */
public interface SplitsCache extends AutoCloseable {
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
     */
    void setupJob(final Job job) throws IOException;

    /**
     * Does the cache have any splits?
     */
    boolean hasSplits();

    /**
     * Receive a count of the splits.
     *
     * @param table
     *            - table where we wish to retrieve the count
     */
    int getSplitsCount(String table);

    /**
     * Obtain the split location based on the key, using the table mappings as a reference.
     *
     * @param table
     *            - table where we wish to look up the index location
     * @param key
     *            - key to look up
     */
    int getExactIndex(String table, Text key);

    /**
     * Obtain then partition that is storing the key, based on the table mappings.
     *
     * @param table
     *            - table where we wish to look up the key
     * @param key
     *            - key to look up
     */
    int getExactPartition(String table, Text key);

    /**
     * Obtain then partition that is storing the key, and if unable to get an exact match find the nearest partition.
     *
     * @param table
     *            - table where we wish to look up the key
     * @param key
     *            - key to look up
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
     */
    String getExactLocation(String table, Text key, Supplier<String> defaultFn);

    List<Text> getSplits(Configuration conf, String tableName) throws IOException;
}
