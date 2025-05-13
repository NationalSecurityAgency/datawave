package datawave.ingest.mapreduce.job;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public interface SplitsJobContext {
    void prepareContext(Configuration conf) throws IOException;
}
