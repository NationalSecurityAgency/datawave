package datawave.ingest.mapreduce;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.writer.ContextWriter;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import java.io.IOException;

/**
 * A {@link ContextWriter} implementation for testing. Saves data into a static memory buffer and allows unit tests to fetch the written results.
 * <p/>
 * We use a static memory buffer so that classes such as EventMapper can dynamically create it but write to a place where we can check it.
 */
public class TestContextWriter<OK,OV> implements ContextWriter<OK,OV> {
    
    private static final Multimap<BulkIngestKey,Value> written = HashMultimap.create();
    
    public TestContextWriter() {
        synchronized (written) {
            written.clear();
        }
    }
    
    @Override
    public void setup(Configuration conf, boolean outputTableCounters) throws IOException, InterruptedException {}
    
    @Override
    public void write(BulkIngestKey key, Value value, TaskInputOutputContext<?,?,OK,OV> context) throws IOException, InterruptedException {
        synchronized (written) {
            written.put(key, value);
        }
    }
    
    @Override
    public void write(Multimap<BulkIngestKey,Value> entries, TaskInputOutputContext<?,?,OK,OV> context) throws IOException, InterruptedException {
        synchronized (written) {
            written.putAll(entries);
        }
    }
    
    @Override
    public void commit(TaskInputOutputContext<?,?,OK,OV> context) throws IOException, InterruptedException {}
    
    @Override
    public void rollback() throws IOException, InterruptedException {}
    
    @Override
    public void cleanup(TaskInputOutputContext<?,?,OK,OV> context) throws IOException, InterruptedException {}
    
    /**
     * @return All entries written to this context.
     */
    public static Multimap<BulkIngestKey,Value> getWritten() {
        return written;
    }
}
