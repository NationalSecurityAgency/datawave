package datawave.ingest.mapreduce.job;

import java.io.IOException;
import java.util.function.Supplier;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

public interface SplitsCache extends AutoCloseable {
    static SplitsCache getInstance(final Configuration conf) {
        return SplitsCacheFactory.getSplitsCache(conf);
    }

    void init(Configuration conf);

    void setupJob(final Job job) throws IOException;

    boolean hasSplits();

    int getSplitsCount(String table);

    int getExactIndex(String table, Text key);

    int getExactPartition(String table, Text key);

    int getNearestPartition(String table, Text key);

    String getExactLocation(String table, Text key, Supplier<String> defaultFn);
}
