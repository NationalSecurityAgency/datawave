package datawave.ingest.mapreduce.job.writer;

import java.io.IOException;

import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.reduce.BulkIngestKeyAggregatingReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import org.apache.accumulo.core.data.Value;

import com.google.common.collect.Multimap;

/**
 * This is a caching context writer that will use the BulkIngestDedupeCombiner to do the actual context writing.
 *
 *
 *
 */
public class AggregatingContextWriter<OK,OV> extends AbstractContextWriter<OK,OV> implements ChainedContextWriter<OK,OV> {

    public static final String CONTEXT_WRITER_CLASS = BulkIngestKeyAggregatingReducer.CONTEXT_WRITER_CLASS;
    private BulkIngestKeyAggregatingReducer<OK,OV> reducer = new BulkIngestKeyAggregatingReducer<>();

    @Override
    public void configureChainedContextWriter(Configuration conf, Class<? extends ContextWriter<OK,OV>> contextWriterClass) {
        conf.setClass(CONTEXT_WRITER_CLASS, contextWriterClass, ContextWriter.class);
    }

    @Override
    public void setup(Configuration conf, boolean outputTableCounters) throws IOException, InterruptedException {
        super.setup(conf, false);
        conf.setBoolean(BulkIngestKeyAggregatingReducer.CONTEXT_WRITER_OUTPUT_TABLE_COUNTERS, outputTableCounters);
        reducer.setup(conf);
    }

    @Override
    public void cleanup(TaskInputOutputContext<?,?,OK,OV> context) throws IOException, InterruptedException {
        super.cleanup(context);
        reducer.finish(context);
    }

    @Override
    protected void flush(Multimap<BulkIngestKey,Value> entries, TaskInputOutputContext<?,?,OK,OV> context) throws IOException, InterruptedException {
        // Note we are not calling the "countWrite" method as this will be done by the underlying ContextWriter
        // if so configured
        reducer.reduce(entries, context);
        reducer.flush(context);
    }

}
