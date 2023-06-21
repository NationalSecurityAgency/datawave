package datawave.ingest.mapreduce.job.writer;

import java.io.IOException;

import datawave.ingest.mapreduce.job.BulkIngestKey;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import org.apache.accumulo.core.data.Value;

import com.google.common.collect.Multimap;

public interface ContextWriter<OK,OV> {

    /**
     * Initialize this context writer.
     *
     * @param conf
     *            configuration
     * @param outputTableCounters
     *            flag to determine whether to output table counters
     * @throws IOException
     *             if there is an issue
     * @throws InterruptedException
     *             if the process is interrupted
     */
    void setup(Configuration conf, boolean outputTableCounters) throws IOException, InterruptedException;

    /**
     * Write the key, value to the cache.
     *
     * @param key
     *            the key to write
     * @param value
     *            the value to write
     * @param context
     *            the context to write to
     * @throws IOException
     *             if there is an issue
     * @throws InterruptedException
     *             if the process is interrupted
     */
    void write(BulkIngestKey key, Value value, TaskInputOutputContext<?,?,OK,OV> context) throws IOException, InterruptedException;

    /**
     * Write the keys, values to the cache.
     *
     * @param entries
     *            the entries to write
     * @param context
     *            the context to write to
     * @throws IOException
     *             if there is an issue
     * @throws InterruptedException
     *             if the process is interrupted
     */
    void write(Multimap<BulkIngestKey,Value> entries, TaskInputOutputContext<?,?,OK,OV> context) throws IOException, InterruptedException;

    /**
     * Flush the cache from the current thread to the context. This method is expected to be called periodically. If a thread has used the write methods, then
     * this method must be called before the thread terminates.
     *
     * @param context
     *            the context to write to
     * @throws IOException
     *             if there is an issue
     * @throws InterruptedException
     *             if the process is interrupted
     */
    void commit(TaskInputOutputContext<?,?,OK,OV> context) throws IOException, InterruptedException;

    /**
     * Rollback the context. This method will rollback to the last time this context was flushed in this thread.
     *
     * @throws IOException
     *             if there is an issue
     * @throws InterruptedException
     *             if the process is interrupted
     */
    void rollback() throws IOException, InterruptedException;

    /**
     * Clean up the context writer. Default implementation executes the flush method.
     *
     * @param context
     *            the context writer to clean
     * @throws IOException
     *             if there is an issue
     * @throws InterruptedException
     *             if the process is interrupted
     */
    void cleanup(TaskInputOutputContext<?,?,OK,OV> context) throws IOException, InterruptedException;
}
