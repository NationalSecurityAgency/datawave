package datawave.ingest.json.mr.handler;

import datawave.ingest.data.config.ingest.AbstractContentIngestHelper;
import datawave.ingest.mapreduce.handler.tokenize.ContentIndexingColumnBasedHandler;
import datawave.ingest.json.config.helper.JsonIngestHelper;

import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class ContentJsonColumnBasedHandler<KEYIN> extends ContentIndexingColumnBasedHandler<KEYIN> {

    @Override
    public void setup(TaskAttemptContext context) {
        super.setup(context);
    }

    @Override
    public AbstractContentIngestHelper getContentIndexingDataTypeHelper() {
        return (JsonIngestHelper) helper;
    }
}
