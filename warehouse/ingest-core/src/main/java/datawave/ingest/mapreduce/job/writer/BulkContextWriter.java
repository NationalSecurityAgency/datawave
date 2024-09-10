package datawave.ingest.mapreduce.job.writer;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import com.google.common.collect.Multimap;

import datawave.ingest.mapreduce.job.BulkIngestKey;

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
