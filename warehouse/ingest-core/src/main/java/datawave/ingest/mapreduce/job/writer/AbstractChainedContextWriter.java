package datawave.ingest.mapreduce.job.writer;

import java.io.IOException;

import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import com.google.common.collect.Multimap;

import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.statsd.StatsDHelper;

public abstract class AbstractChainedContextWriter<OK,OV> extends StatsDHelper implements ChainedContextWriter<OK,OV> {

    /**
     * The chained context writer
     */
    protected ContextWriter<OK,OV> contextWriter = null;

    /**
     * Get the option used to configure the chained context writer after this one
     *
     * @return the object (e.g. ingest.mytype.index.context.writer.class)
     */
    protected abstract String getChainedContextWriterOption();

    @Override
    public void configureChainedContextWriter(Configuration conf, Class<? extends ContextWriter<OK,OV>> contextWriterClass) {
        conf.setClass(getChainedContextWriterOption(), contextWriterClass, ContextWriter.class);
    }

    @Override
    public void setup(Configuration conf, boolean outputTableCounters) throws IOException, InterruptedException {
        super.setup(conf);

        @SuppressWarnings("unchecked")
        Class<ContextWriter<OK,OV>> contextWriterClass = (Class<ContextWriter<OK,OV>>) conf.getClass(getChainedContextWriterOption(), null,
                        ContextWriter.class);
        if (contextWriterClass == null) {
            throw new IllegalArgumentException(this.getClass() + " is a ChainedContextWriter but no follow-on context writer has been configured for "
                            + getChainedContextWriterOption());
        }
        try {
            contextWriter = contextWriterClass.newInstance();
            contextWriter.setup(conf, outputTableCounters);
        } catch (Exception e) {
            throw new IOException("Failed to initialized " + contextWriterClass + " from property " + getChainedContextWriterOption(), e);
        }
    }

    @Override
    public void write(BulkIngestKey key, Value value, TaskInputOutputContext<?,?,OK,OV> context) throws IOException, InterruptedException {
        contextWriter.write(key, value, context);
    }

    @Override
    public void write(Multimap<BulkIngestKey,Value> entries, TaskInputOutputContext<?,?,OK,OV> context) throws IOException, InterruptedException {
        contextWriter.write(entries, context);
    }

    @Override
    public void commit(TaskInputOutputContext<?,?,OK,OV> context) throws IOException, InterruptedException {
        contextWriter.commit(context);
    }

    @Override
    public void rollback() throws IOException, InterruptedException {
        contextWriter.rollback();
    }

    @Override
    public void cleanup(TaskInputOutputContext<?,?,OK,OV> context) throws IOException, InterruptedException {
        contextWriter.cleanup(context);
        super.close();
    }

}
