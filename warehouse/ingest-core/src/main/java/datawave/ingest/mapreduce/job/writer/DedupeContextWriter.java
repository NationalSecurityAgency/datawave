package datawave.ingest.mapreduce.job.writer;

import java.io.IOException;

import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import com.google.common.collect.Multimap;

import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.reduce.BulkIngestKeyDedupeCombiner;

/**
 * This is a caching context writer that will use the BulkIngestDedupeCombiner to do the actual context writing.
 *
 *
 *
 */
public class DedupeContextWriter<OK,OV> extends AbstractContextWriter<OK,OV> implements ChainedContextWriter<OK,OV> {

    public static final String CONTEXT_WRITER_CLASS = BulkIngestKeyDedupeCombiner.CONTEXT_WRITER_CLASS;
    private BulkIngestKeyDedupeCombiner<OK,OV> combiner = new BulkIngestKeyDedupeCombiner<>();

    @Override
    public void configureChainedContextWriter(Configuration conf, Class<? extends ContextWriter<OK,OV>> contextWriterClass) {
        conf.setClass(CONTEXT_WRITER_CLASS, contextWriterClass, ContextWriter.class);
    }

    @Override
    public void setup(Configuration conf, boolean outputTableCounters) throws IOException, InterruptedException {
        super.setup(conf, false);
        conf.setBoolean(BulkIngestKeyDedupeCombiner.CONTEXT_WRITER_OUTPUT_TABLE_COUNTERS, outputTableCounters);
        combiner.setup(conf);
    }

    @Override
    public void cleanup(TaskInputOutputContext<?,?,OK,OV> context) throws IOException, InterruptedException {
        super.cleanup(context);
        combiner.finish(context);
    }

    @Override
    protected void flush(Multimap<BulkIngestKey,Value> entries, TaskInputOutputContext<?,?,OK,OV> context) throws IOException, InterruptedException {
        // Note we are not calling the "countWrite" method as this will be done by the underlying ContextWriter
        // if so configured
        combiner.reduce(entries, context);
        combiner.flush(context);
    }

}
