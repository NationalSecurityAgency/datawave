package datawave.ingest.mapreduce.job.writer;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.reduce.BulkIngestKeyDedupeCombiner;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

/**
 * This is a caching context writer that will cache the entries for a table and will only actually flush entries once that cache is full. The cache will be
 * aggregated as the entries are received. This cache will cache values across calls to commit, which means that entries are aggregated across events (see
 * EventMapper contextWriter commit calls).
 *
 */
public class TableCachingContextWriter extends AbstractContextWriter<BulkIngestKey,Value> implements ChainedContextWriter<BulkIngestKey,Value> {

    // THe property used for to configure the next writer in the chain
    public static final String CONTEXT_WRITER_CLASS = "ingest.table.caching.context.writer.class";

    // The property used to determine whether we are outputting mutations or keys such that a default chained context writer can be configured
    public static final String MAPRED_OUTPUT_VALUE_CLASS = "mapreduce.job.output.value.class";

    // a counter to keep track of how often the buffer for a table gets fluhed
    public static final String FLUSHED_BUFFER_COUNTER = "TABLE_CACHE_FLUSHES";
    public static final String FLUSHED_BUFFER_TOTAL = "TABLE_CACHE_FLUSHED_ENTRIES";

    // This is the cache configuration
    private static final Map<Text,Integer> tableCacheConf = new HashMap<>();

    // the tables to cache will be configured by setting a <tablename>.table.context.writer.cache property where the value is the max size of the cache in
    // entries
    public static final String TABLES_TO_CACHE_SUFFIX = ".table.context.writer.cache";

    // This is the cache
    private final Map<Text,Multimap<BulkIngestKey,Value>> aggregatedCache = new HashMap<>();

    // This is the combiner used to aggregate values
    private CachingContextWriter combinerCache = new CachingContextWriter();
    private BulkIngestKeyDedupeCombiner<BulkIngestKey,Value> combiner = new BulkIngestKeyDedupeCombiner<BulkIngestKey,Value>() {
        @Override
        protected void setupContextWriter(Configuration conf) throws IOException {
            setContextWriter(combinerCache);
        }
    };

    // The chained context writer
    private ContextWriter<BulkIngestKey,Value> contextWriter;

    @Override
    public void configureChainedContextWriter(Configuration conf, Class<? extends ContextWriter<BulkIngestKey,Value>> contextWriterClass) {
        conf.setClass(CONTEXT_WRITER_CLASS, contextWriterClass, ContextWriter.class);
    }

    @Override
    public void setup(Configuration conf, boolean outputTableCounters) throws IOException, InterruptedException {
        super.setup(conf, false);

        // Configure the combiner
        combiner.setup(conf);

        // get the tables to cache configuration
        for (Map.Entry<String,String> prop : conf) {
            if (prop.getKey().endsWith(TABLES_TO_CACHE_SUFFIX)) {
                String tableName = prop.getKey().substring(0, prop.getKey().length() - TABLES_TO_CACHE_SUFFIX.length());
                int maxCacheSize = Integer.parseInt(prop.getValue());
                tableCacheConf.put(new Text(tableName), maxCacheSize);
            }
        }

        // create and setup the chained context writer
        Class<ContextWriter<BulkIngestKey,Value>> contextWriterClass = null;
        if (Mutation.class.equals(conf.getClass(MAPRED_OUTPUT_VALUE_CLASS, null))) {
            contextWriterClass = (Class<ContextWriter<BulkIngestKey,Value>>) conf.getClass(CONTEXT_WRITER_CLASS, LiveContextWriter.class, ContextWriter.class);
        } else {
            contextWriterClass = (Class<ContextWriter<BulkIngestKey,Value>>) conf.getClass(CONTEXT_WRITER_CLASS, BulkContextWriter.class, ContextWriter.class);
        }
        try {
            contextWriter = contextWriterClass.getDeclaredConstructor().newInstance();
            contextWriter.setup(conf, outputTableCounters);
        } catch (Exception e) {
            throw new IOException("Failed to initialized " + contextWriterClass + " from property " + CONTEXT_WRITER_CLASS, e);
        }

    }

    @Override
    public void commit(TaskInputOutputContext<?,?,BulkIngestKey,Value> context) throws IOException, InterruptedException {
        super.commit(context);
        contextWriter.commit(context);
    }

    @Override
    protected void flush(Multimap<BulkIngestKey,Value> entries, TaskInputOutputContext<?,?,BulkIngestKey,Value> context)
                    throws IOException, InterruptedException {
        Multimap<BulkIngestKey,Value> residual = HashMultimap.create();
        for (BulkIngestKey key : entries.keySet()) {
            Collection<Value> values = entries.get(key);
            if (tableCacheConf.containsKey(key.getTableName())) {
                cache(key, values, context);
            } else {
                residual.putAll(key, values);
            }
        }
        if (!residual.isEmpty()) {
            contextWriter.write(residual, context);
        }
    }

    @Override
    public void rollback() throws IOException, InterruptedException {
        super.rollback();
        contextWriter.rollback();
    }

    @Override
    public void cleanup(TaskInputOutputContext<?,?,BulkIngestKey,Value> context) throws IOException, InterruptedException {
        super.cleanup(context);
        flushAll(context);
        contextWriter.cleanup(context);
    }

    private void flushAll(TaskInputOutputContext<?,?,BulkIngestKey,Value> context) throws IOException, InterruptedException {
        // pass all of the data through the delegate and clear the cache
        for (Map.Entry<Text,Multimap<BulkIngestKey,Value>> entries : aggregatedCache.entrySet()) {
            if (!entries.getValue().isEmpty()) {
                getCounter(context, FLUSHED_BUFFER_TOTAL, entries.getKey().toString()).increment(entries.getValue().size());
                getCounter(context, FLUSHED_BUFFER_COUNTER, entries.getKey().toString()).increment(1);
                contextWriter.write(entries.getValue(), context);
            }
        }
        aggregatedCache.clear();
    }

    private void cache(BulkIngestKey key, Collection<Value> values, TaskInputOutputContext<?,?,BulkIngestKey,Value> context)
                    throws IOException, InterruptedException {
        List<Value> valueList = new ArrayList<>();
        valueList.addAll(values);

        Multimap<BulkIngestKey,Value> entries = aggregatedCache.get(key.getTableName());
        if (entries != null) {
            valueList.addAll(entries.removeAll(key));
        } else {
            entries = HashMultimap.create();
            aggregatedCache.put(key.getTableName(), entries);
        }

        // reduce the entries as needed
        if (valueList.size() > 1) {
            entries.putAll(key, reduceValues(key, valueList, context));
        } else {
            entries.putAll(key, valueList);
        }

        // now flush this tables cache if needed
        if (entries.size() >= tableCacheConf.get(key.getTableName())) {
            // register that we overran the cache for this table
            getCounter(context, FLUSHED_BUFFER_TOTAL, key.getTableName().toString()).increment(entries.size());
            getCounter(context, FLUSHED_BUFFER_COUNTER, key.getTableName().toString()).increment(1);
            contextWriter.write(entries, context);
            aggregatedCache.remove(key.getTableName());
        }
    }

    /**
     * Reduce the list of values for a key.
     *
     * @param key
     *            a key
     * @param values
     *            a set of values
     * @param context
     *            the context
     * @return the reduced collection of values
     * @throws IOException
     *             if there is an issue with read or write
     * @throws InterruptedException
     *             if the thread is interrupted
     */
    private Collection<Value> reduceValues(BulkIngestKey key, Collection<Value> values, TaskInputOutputContext<?,?,BulkIngestKey,Value> context)
                    throws IOException, InterruptedException {
        combiner.doReduce(key, values, context);
        try {
            return combinerCache.getValues(key);
        } finally {
            combinerCache.clear();
        }
    }

    /**
     * This is a context writer that simply puts the keys into a cache, retrievable by the getKeys() and getValues() call
     */
    private static class CachingContextWriter implements ContextWriter<BulkIngestKey,Value> {

        private Multimap<BulkIngestKey,Value> reduced = HashMultimap.create();

        public Collection<BulkIngestKey> getKeys() {
            return reduced.keySet();
        }

        public Collection<Value> getValues(BulkIngestKey key) {
            return reduced.get(key);
        }

        public void clear() {
            reduced = HashMultimap.create();
        }

        @Override
        public void setup(Configuration conf, boolean outputTableCounters) throws IOException, InterruptedException {

        }

        @Override
        public void write(BulkIngestKey key, Value value, TaskInputOutputContext<?,?,BulkIngestKey,Value> context) throws IOException, InterruptedException {
            reduced.put(key, value);
        }

        @Override
        public void write(Multimap<BulkIngestKey,Value> entries, TaskInputOutputContext<?,?,BulkIngestKey,Value> context)
                        throws IOException, InterruptedException {
            reduced.putAll(entries);
        }

        @Override
        public void commit(TaskInputOutputContext<?,?,BulkIngestKey,Value> context) throws IOException, InterruptedException {

        }

        @Override
        public void rollback() throws IOException, InterruptedException {

        }

        @Override
        public void cleanup(TaskInputOutputContext<?,?,BulkIngestKey,Value> context) throws IOException, InterruptedException {

        }
    }
}
