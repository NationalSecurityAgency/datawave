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

    void init(Configuration conf);

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

    String getExactLocation(String table, Text key, Supplier<String> defaultFn);

    List<Text> getSplits(Configuration conf, String tableName) throws IOException;
}
