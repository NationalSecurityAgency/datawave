package nsa.datawave.query.metrics;

import nsa.datawave.ingest.data.config.ingest.AbstractContentIngestHelper;
import nsa.datawave.ingest.mapreduce.handler.tokenize.ContentIndexingColumnBasedHandler;

public class ContentQueryMetricsHandler<KEYIN> extends ContentIndexingColumnBasedHandler<KEYIN> {
    
    @Override
    public AbstractContentIngestHelper getContentIndexingDataTypeHelper() {
        return (ContentQueryMetricsIngestHelper) helper;
    }
    
}
