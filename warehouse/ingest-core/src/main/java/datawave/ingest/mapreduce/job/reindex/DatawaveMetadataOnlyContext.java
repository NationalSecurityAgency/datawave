package datawave.ingest.mapreduce.job.reindex;

import static datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler.METADATA_TABLE_NAME;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import com.google.common.collect.Multimap;

import datawave.data.ColumnFamilyConstants;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.writer.ContextWriter;

public class DatawaveMetadataOnlyContext implements ContextWriter<BulkIngestKey,Value> {
    private final ContextWriter<BulkIngestKey,Value> delegate;
    private final boolean excludeFrequency;

    private Text metadataTableName = null;

    public DatawaveMetadataOnlyContext(ContextWriter<BulkIngestKey,Value> delegate) {
        this(delegate, false);
    }

    public DatawaveMetadataOnlyContext(ContextWriter<BulkIngestKey,Value> delegate, boolean excludeFrequency) {
        this.delegate = delegate;
        this.excludeFrequency = excludeFrequency;
    }

    private boolean canWrite(BulkIngestKey key, Value value) {
        if (metadataTableName == null) {
            throw new IllegalStateException("must call setup before using");
        }

        if (metadataTableName.equals(key.getTableName())) {
            // test for exclusion of a frequency
            if (excludeFrequency && key.getKey().getColumnFamily().equals(ColumnFamilyConstants.COLF_F)) {
                return false;
            }

            return true;
        }

        return false;
    }

    @Override
    public void setup(Configuration conf, boolean outputTableCounters) throws IOException, InterruptedException {
        delegate.setup(conf, outputTableCounters);

        // pull the DatawaveMetadata table name
        String metadataTableNameString = conf.get(METADATA_TABLE_NAME, null);
        if (metadataTableNameString == null) {
            throw new IllegalStateException(METADATA_TABLE_NAME + " must be configured in job");
        }
        metadataTableName = new Text(metadataTableNameString);
    }

    @Override
    public void write(BulkIngestKey key, Value value, TaskInputOutputContext<?,?,BulkIngestKey,Value> context) throws IOException, InterruptedException {
        if (canWrite(key, value)) {
            delegate.write(key, value, context);
        }
    }

    @Override
    public void write(Multimap<BulkIngestKey,Value> entries, TaskInputOutputContext<?,?,BulkIngestKey,Value> context) throws IOException, InterruptedException {
        for (Map.Entry<BulkIngestKey,Value> entry : entries.entries()) {
            if (canWrite(entry.getKey(), entry.getValue())) {
                delegate.write(entry.getKey(), entry.getValue(), context);
            }
        }
    }

    @Override
    public void commit(TaskInputOutputContext<?,?,BulkIngestKey,Value> context) throws IOException, InterruptedException {
        delegate.commit(context);
    }

    @Override
    public void rollback() throws IOException, InterruptedException {
        delegate.rollback();
    }

    @Override
    public void cleanup(TaskInputOutputContext<?,?,BulkIngestKey,Value> context) throws IOException, InterruptedException {
        delegate.cleanup(context);
    }
}
