package datawave.ingest.json.mr.handler;

import org.apache.hadoop.mapreduce.TaskAttemptContext;

import datawave.ingest.data.config.ingest.AbstractContentIngestHelper;
import datawave.ingest.json.config.helper.JsonIngestHelper;
import datawave.ingest.mapreduce.handler.tokenize.ContentIndexingColumnBasedHandler;

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
