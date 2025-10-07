package datawave.ingest.mapreduce.job.reindex;

import datawave.ingest.data.config.ingest.AbstractContentIngestHelper;
import datawave.ingest.data.config.ingest.CSVIngestHelper;
import datawave.ingest.mapreduce.handler.tokenize.ContentIndexingColumnBasedHandler;

/**
 * Used by {@link ShardedDataGenerator}
 */
public class SimpleContentIndexingColumnBasedHandler extends ContentIndexingColumnBasedHandler {
    @Override
    public AbstractContentIngestHelper getContentIndexingDataTypeHelper() {
        return (CSVIngestHelper) helper;
    }
}
