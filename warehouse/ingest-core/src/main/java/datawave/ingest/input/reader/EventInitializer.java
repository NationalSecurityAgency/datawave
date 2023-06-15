package datawave.ingest.input.reader;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public interface EventInitializer {

    void initializeEvent(Configuration conf) throws IOException;

}
