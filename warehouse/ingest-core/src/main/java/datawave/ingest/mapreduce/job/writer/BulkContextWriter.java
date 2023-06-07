package datawave.ingest.mapreduce.job.writer;

import java.io.IOException;
import java.util.Map;

import datawave.ingest.mapreduce.job.BulkIngestKey;

import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import org.apache.accumulo.core.data.Value;

import com.google.common.collect.Multimap;

/**
 * A simple context writer that simply passes the key and value directly to the context.
 * 
 * 
 * 
 */
public class BulkContextWriter extends AbstractContextWriter<BulkIngestKey,Value> {
    
    @Override
    protected void flush(Multimap<BulkIngestKey,Value> entries, TaskInputOutputContext<?,?,BulkIngestKey,Value> context)
                    throws IOException, InterruptedException {
        for (Map.Entry<BulkIngestKey,Value> entry : entries.entries()) {
            context.write(entry.getKey(), entry.getValue());
        }
    }
    
}
