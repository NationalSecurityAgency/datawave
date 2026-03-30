
package datawave.ingest.mapreduce.job;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

public interface SplitsJobContext {
    void prepareContext(Configuration conf) throws IOException;
}
