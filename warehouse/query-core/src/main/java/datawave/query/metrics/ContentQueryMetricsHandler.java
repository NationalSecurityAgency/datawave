package datawave.query.metrics;

import datawave.ingest.data.config.ingest.AbstractContentIngestHelper;
import datawave.ingest.mapreduce.handler.tokenize.ContentIndexingColumnBasedHandler;

public class ContentQueryMetricsHandler<KEYIN> extends ContentIndexingColumnBasedHandler<KEYIN> {

    @Override
    public AbstractContentIngestHelper getContentIndexingDataTypeHelper() {
        return (ContentQueryMetricsIngestHelper) helper;
    }

}
