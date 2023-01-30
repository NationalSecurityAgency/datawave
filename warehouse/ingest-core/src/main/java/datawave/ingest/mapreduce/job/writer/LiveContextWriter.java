package datawave.ingest.mapreduce.job.writer;

import java.io.IOException;
import java.util.Map;

import datawave.ingest.mapreduce.job.BulkIngestKey;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;

import com.google.common.collect.Multimap;

/**
 * A simple context writer that simply passes the key, value as a text, mutation to the context.
 * 
 * 
 * 
 */
public class LiveContextWriter extends AbstractContextWriter<Text,Mutation> {
    
    @Override
    protected void flush(Multimap<BulkIngestKey,Value> entries, TaskInputOutputContext<?,?,Text,Mutation> context) throws IOException, InterruptedException {
        for (Map.Entry<BulkIngestKey,Value> entry : entries.entries()) {
            writeToContext(context, entry);
        }
    }
    
    protected void writeToContext(TaskInputOutputContext<?,?,Text,Mutation> context, Map.Entry<BulkIngestKey,Value> entry) throws IOException,
                    InterruptedException {
        context.write(entry.getKey().getTableName(), getMutation(entry.getKey().getKey(), entry.getValue()));
    }
    
    /**
     * Turn a key, value into a mutation
     * 
     * @param key
     *            a key
     * @param value
     *            a value
     * @return the mutation
     */
    protected Mutation getMutation(Key key, Value value) {
        Mutation m = new Mutation(key.getRow());
        if (key.isDeleted()) {
            m.putDelete(key.getColumnFamily(), key.getColumnQualifier(), new ColumnVisibility(key.getColumnVisibility()), key.getTimestamp());
        } else {
            m.put(key.getColumnFamily(), key.getColumnQualifier(), new ColumnVisibility(key.getColumnVisibility()), key.getTimestamp(), value);
        }
        return m;
    }
    
}
