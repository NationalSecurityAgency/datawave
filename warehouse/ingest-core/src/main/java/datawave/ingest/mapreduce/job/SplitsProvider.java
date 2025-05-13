package datawave.ingest.mapreduce.job;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public interface SplitsProvider {
    void init(Configuration conf) throws IOException;
    void refreshView() throws IOException;
    SplitsView readView() throws IOException;
}
