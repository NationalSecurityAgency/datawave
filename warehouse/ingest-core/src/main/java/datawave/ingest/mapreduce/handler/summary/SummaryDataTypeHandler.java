package datawave.ingest.mapreduce.handler.summary;

import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import com.google.common.collect.Multimap;

import datawave.core.common.logging.ThreadConfigurableLogger;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.ingest.IngestHelperInterface;
import datawave.ingest.mapreduce.handler.DataTypeHandler;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.metadata.RawRecordMetadata;

public abstract class SummaryDataTypeHandler<KEYIN> implements DataTypeHandler<KEYIN> {
    private static final Logger log = ThreadConfigurableLogger.getLogger(SummaryDataTypeHandler.class);

    private Configuration mConf = null;

    protected abstract Multimap<BulkIngestKey,Value> createEntries(RawRecordContainer record, Multimap<String,NormalizedContentInterface> fields,
                    ColumnVisibility vis, long timestampe, IngestHelperInterface iHelper);

    @Override
    public void setup(TaskAttemptContext context) {
        Configuration conf = context.getConfiguration();
        TypeRegistry.getInstance(conf);
        mConf = conf;
    }

    @Override
    public Multimap<BulkIngestKey,Value> processBulk(KEYIN key, RawRecordContainer record, Multimap<String,NormalizedContentInterface> fields,
                    StatusReporter reporter) {
        IngestHelperInterface iHelper = this.getHelper(record.getDataType());
        return createEntries(record, fields, getVisibility(), System.currentTimeMillis(), iHelper);
    }

    // Since this is a summary use the configured default classification and access controls for column visibility
    private ColumnVisibility getVisibility() {
        ColumnVisibility vis;
        vis = new ColumnVisibility();
        return vis;
    }

    @Override
    public IngestHelperInterface getHelper(Type datatype) {
        return datatype.getIngestHelper(mConf);
    }

    @Override
    public void close(TaskAttemptContext context) {
        // do nothing
    }

    @Override
    public RawRecordMetadata getMetadata() {
        return null; // do nothing
    }
}
