package datawave.ingest.mapreduce.handler;

import org.apache.hadoop.conf.Configuration;

/**
 * Support IngestJob setup on a DataTypeHandler to avoid per mapper computation. Run once at job setup time and prepare/serialize into configuration for later
 * use in mappers.
 */
public interface JobSetupHandler {
    void setup(Configuration conf);
}
