package datawave.ingest.mapreduce.partition;

import datawave.ingest.mapreduce.job.BulkIngestKey;

/**
 * Hashes the row of each bulk ingest key and applies a limit on the number of possible partitions that is {@code <= numReducers}.
 */
public class LimitedRowPartitioner extends LimitedKeyPartitioner {
    @Override
    protected int getKeyHashcode(BulkIngestKey bKey) {
        return bKey.getKey().getRow().hashCode();
    }
}
