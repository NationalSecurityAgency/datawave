package datawave.ingest.csv.mr.handler;

import org.apache.hadoop.mapreduce.TaskAttemptContext;

import datawave.ingest.csv.config.helper.ExtendedCSVIngestHelper;
import datawave.ingest.data.config.ingest.AbstractContentIngestHelper;
import datawave.ingest.mapreduce.handler.tokenize.ContentIndexingColumnBasedHandler;

public class ContentCSVColumnBasedHandler<KEYIN> extends ContentIndexingColumnBasedHandler<KEYIN> {

    @Override
    public void setup(TaskAttemptContext context) {
        super.setup(context);
    }

    @Override
    public AbstractContentIngestHelper getContentIndexingDataTypeHelper() {
        return (ExtendedCSVIngestHelper) helper;
    }
}
