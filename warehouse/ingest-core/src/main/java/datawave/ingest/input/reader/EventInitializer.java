package datawave.ingest.input.reader;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

public interface EventInitializer {

    void initializeEvent(Configuration conf) throws IOException;

}
