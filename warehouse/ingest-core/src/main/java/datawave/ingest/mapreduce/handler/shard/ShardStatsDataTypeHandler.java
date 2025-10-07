package datawave.ingest.mapreduce.handler.shard;

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.common.collect.Multimap;

import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.ingest.IngestHelperInterface;
import datawave.ingest.mapreduce.handler.DataTypeHandler;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.metadata.RawRecordMetadata;

/**
 * Minimal data handler for generating index statistics for the shard table. This data handler only supports the {@link #getTableNames(Configuration)} and
 * {@link #getTableLoaderPriorities(Configuration)} methods.
 *
 * @param <KEYIN>
 *            the type of the input key
 */
public class ShardStatsDataTypeHandler<KEYIN> implements DataTypeHandler<KEYIN> {

    // value must match ingest configuration file
    public static final String STATS_TABLE = "shardStats.table.name";
    private static final String STATS_LPRIORITY = "shardStats.table.loader.priority";

    @Override
    public void setup(TaskAttemptContext context) {
        throw new UnsupportedOperationException("setup method is not supported");
    }

    @Override
    public String[] getTableNames(Configuration conf) {
        List<String> tables = new ArrayList<>(1);
        String tableName = conf.get(STATS_TABLE, null);
        if (null != tableName) {
            tables.add(tableName);
        }

        return tables.toArray(new String[tables.size()]);
    }

    @Override
    public int[] getTableLoaderPriorities(Configuration conf) {
        int[] priorities;
        String tableName = conf.get(STATS_TABLE, null);
        if (null != tableName) {
            priorities = new int[1];
            priorities[0] = conf.getInt(STATS_LPRIORITY, 30);
        } else {
            priorities = new int[0];
        }

        return priorities;
    }

    @Override
    public Multimap<BulkIngestKey,Value> processBulk(KEYIN key, RawRecordContainer event, Multimap<String,NormalizedContentInterface> fields,
                    StatusReporter reporter) {
        throw new UnsupportedOperationException(" method is not supported");
    }

    @Override
    public IngestHelperInterface getHelper(Type datatype) {
        throw new UnsupportedOperationException(" method is not supported");
    }

    @Override
    public void close(TaskAttemptContext context) {
        throw new UnsupportedOperationException(" method is not supported");
    }

    @Override
    public RawRecordMetadata getMetadata() {
        throw new UnsupportedOperationException(" method is not supported");
    }
}
