package datawave.ingest.mapreduce.job.writer;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import datawave.ingest.data.config.ingest.BaseIngestHelper;
import datawave.ingest.mapreduce.job.BulkIngestCounters;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.ConstraintChecker;
import datawave.ingest.mapreduce.job.TableConfigurationUtil;
import datawave.ingest.mapreduce.job.statsd.StatsDHelper;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * An abstraction of writing to a context object. This class always takes BulkIngestKeys and Values in the write methods. The actual writing to the context
 * object may require a translation such as to a Text and Mutation object.
 *
 *
 *
 * @param <OK>
 *            The output key
 * @param <OV>
 *            The output value
 */
public abstract class AbstractContextWriter<OK,OV> extends StatsDHelper implements ContextWriter<OK,OV> {
    
    public static final String CONTEXT_WRITER_COUNTERS = "context.writer.counters";
    public static final String CONTEXT_WRITER_MAX_CACHE_SIZE = "context.writer.max.cache.size";
    
    private BulkIngestCounters counters = null;
    // caching the simple class name as the calculation is actually a little expensive
    private String simpleClassName = null;
    private long count = 0;
    
    // the cache
    private Multimap<BulkIngestKey,Value> cache = ArrayListMultimap.create();
    
    // the maximum size of the cache. When the cache reaches this size, it will automatically be flushed
    private int maxSize = 2500;
    
    private ConstraintChecker constraintChecker;
    
    /**
     * Initialize this context writer.
     *
     * @param conf
     */
    @Override
    public void setup(Configuration conf, boolean outputTableCounters) throws IOException, InterruptedException {
        super.setup(conf);
        if (outputTableCounters) {
            counters = new BulkIngestCounters(conf);
            boolean deleteMode = conf.getBoolean(BaseIngestHelper.INGEST_MODE_DELETE, false);
            // Get the list of tables that we are bulk ingesting into.
            Set<String> tables = TableConfigurationUtil.getTables(conf);
            for (String table : tables) {
                // Create the counters for this table.
                counters.createCounter(table, deleteMode);
            }
        }
        if (conf.getBoolean(CONTEXT_WRITER_COUNTERS, false)) {
            simpleClassName = getClass().getSimpleName();
        }
        maxSize = conf.getInt(CONTEXT_WRITER_MAX_CACHE_SIZE, maxSize);
        constraintChecker = ConstraintChecker.create(conf);
    }
    
    /**
     * Write the key, value to the cache.
     */
    @Override
    public void write(BulkIngestKey key, Value value, TaskInputOutputContext<?,?,OK,OV> context) throws IOException, InterruptedException {
        if (constraintChecker != null && constraintChecker.isConfigured()) {
            constraintChecker.check(key.getTableName(), key.getKey().getColumnVisibilityData().getBackingArray());
        }
        
        cache.put(key, value);
        this.count++;
        if (counters != null) {
            counters.incrementCounter(key);
        }
        if (cache.size() > this.maxSize) {
            commit(context);
        }
    }
    
    /**
     * Write the keys, values to the cache.
     */
    @Override
    public void write(Multimap<BulkIngestKey,Value> entries, TaskInputOutputContext<?,?,OK,OV> context) throws IOException, InterruptedException {
        if (constraintChecker != null && constraintChecker.isConfigured()) {
            for (BulkIngestKey key : entries.keySet()) {
                constraintChecker.check(key.getTableName(), key.getKey().getColumnVisibilityData().getBackingArray());
            }
        }
        
        cache.putAll(entries);
        this.count += entries.size();
        if (counters != null) {
            for (Map.Entry<BulkIngestKey,Collection<Value>> entry : entries.asMap().entrySet()) {
                counters.incrementCounter(entry.getKey(), entry.getValue().size());
            }
        }
        if (cache.size() > this.maxSize) {
            commit(context);
        }
    }
    
    /**
     * Flush the cache from the current thread to the context. This method is expected to be called periodically. If a thread has used the write methods, then
     * this method must be called before the thread terminates.
     */
    @Override
    public void commit(TaskInputOutputContext<?,?,OK,OV> context) throws IOException, InterruptedException {
        flush(cache, context);
        // cache.clear() can be fairly expensive, so let's let garbage collection do that
        cache = ArrayListMultimap.create();
    }
    
    /**
     * Rollback the context. This method will rollback to the last time this context was flushed in this thread.
     */
    @Override
    public void rollback() throws IOException, InterruptedException {
        count -= cache.size();
        if (counters != null) {
            for (Map.Entry<BulkIngestKey,Collection<Value>> entry : cache.asMap().entrySet()) {
                counters.incrementCounter(entry.getKey(), (0 - entry.getValue().size()));
            }
        }
        // cache.clear() can be fairly expensive, so let's let garbage collection do that
        cache = ArrayListMultimap.create();
    }
    
    /**
     * The method that actually flushes the entries to the context.
     *
     * @param entries
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    protected abstract void flush(Multimap<BulkIngestKey,Value> entries, TaskInputOutputContext<?,?,OK,OV> context) throws IOException, InterruptedException;
    
    /**
     * Clean up the context writer. Default implementation executes the flush method.
     *
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void cleanup(TaskInputOutputContext<?,?,OK,OV> context) throws IOException, InterruptedException {
        commit(context);
        // also flush the counters at this point
        if (simpleClassName != null) {
            getCounter(context, "ContextWriter", simpleClassName).increment(this.count);
            this.count = 0;
        }
        if (counters != null) {
            counters.flush(getContext(context));
        }
        super.close();
    }
}
