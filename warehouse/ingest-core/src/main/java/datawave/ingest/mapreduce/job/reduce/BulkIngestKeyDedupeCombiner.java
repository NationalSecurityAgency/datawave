package datawave.ingest.mapreduce.job.reduce;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.writer.BulkContextWriter;
import datawave.ingest.mapreduce.job.writer.ContextWriter;
import datawave.ingest.mapreduce.job.writer.LiveContextWriter;
import datawave.ingest.metric.IngestOutput;
import datawave.ingest.table.aggregator.PropogatingCombiner;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import com.google.common.collect.Iterators;

public class BulkIngestKeyDedupeCombiner<K2,V2> extends AggregatingReducer<BulkIngestKey,Value,K2,V2> {
    
    public static final String CONTEXT_WRITER_CLASS = "ingest.dedupe.combiner.context.writer.class";
    public static final String CONTEXT_WRITER_OUTPUT_TABLE_COUNTERS = "ingest.dedupe.combiner.context.writer.output.table.counters";
    public static final String USING_COMBINER = "ingest.using.combiner";
    public static final String MAPRED_OUTPUT_VALUE_CLASS = "mapreduce.job.output.value.class";
    protected static final String COUNTER_CLASS_NAME = BulkIngestKeyDedupeCombiner.class.getSimpleName();
    
    private ContextWriter<K2,V2> contextWriter = null;
    
    @SuppressWarnings("unchecked")
    @Override
    public void setup(Configuration conf) throws IOException, InterruptedException {
        super.setup(conf);
        
        setupContextWriter(conf);
        
        // Throw an error if the flag is not set. If this combiner is being used then others need to know
        // about it.
        if (!conf.getBoolean(USING_COMBINER, false)) {
            throw new IOException("Expected " + USING_COMBINER + " to be set to true when using this class");
        }
    }
    
    protected void setupContextWriter(Configuration conf) throws IOException {
        Class<ContextWriter<K2,V2>> contextWriterClass = null;
        if (Mutation.class.equals(conf.getClass(MAPRED_OUTPUT_VALUE_CLASS, null))) {
            contextWriterClass = (Class<ContextWriter<K2,V2>>) conf.getClass(CONTEXT_WRITER_CLASS, LiveContextWriter.class, ContextWriter.class);
        } else {
            contextWriterClass = (Class<ContextWriter<K2,V2>>) conf.getClass(CONTEXT_WRITER_CLASS, BulkContextWriter.class, ContextWriter.class);
        }
        try {
            setContextWriter(contextWriterClass.getDeclaredConstructor().newInstance());
            contextWriter.setup(conf, conf.getBoolean(CONTEXT_WRITER_OUTPUT_TABLE_COUNTERS, false));
        } catch (Exception e) {
            throw new IOException("Failed to initialized " + contextWriterClass + " from property " + CONTEXT_WRITER_CLASS, e);
        }
    }
    
    public void setContextWriter(ContextWriter<?,?> writer) {
        this.contextWriter = (ContextWriter<K2,V2>) writer;
    }
    
    public ContextWriter<K2,V2> getContextWriter() {
        return this.contextWriter;
    }
    
    @Override
    public void finish(TaskInputOutputContext<?,?,K2,V2> context) throws IOException, InterruptedException {
        super.finish(context);
        contextWriter.cleanup(context);
    }
    
    public void flush(TaskInputOutputContext<?,?,K2,V2> context) throws IOException, InterruptedException {
        contextWriter.commit(context);
    }
    
    /**
     * This method can be overwritten to write directly to the context if K2, V2 are BulkIngestKey, Value, or this method can translate to something else such
     * as Text, Mutation
     * 
     * @param key
     *            a key
     * @param value
     *            a value
     * @param ctx
     *            the context
     * @throws IOException
     *             if there is an issue with read or write
     * @throws InterruptedException
     *             if the thread is interrupted
     */
    protected void writeBulkIngestKey(BulkIngestKey key, Value value, TaskInputOutputContext<?,?,K2,V2> ctx) throws IOException, InterruptedException {
        contextWriter.write(key, value, ctx);
        // the combiner needs to preserve order writing out, so lets avoid any caching
        contextWriter.commit(ctx);
    }
    
    @Override
    public void doReduce(BulkIngestKey key, Iterable<Value> values, TaskInputOutputContext<?,?,K2,V2> ctx) throws IOException, InterruptedException {
        long ts = 0;
        boolean useTSDedup = false;
        
        if (useAggregators(key.getTableName())) {
            
            List<Combiner> aggList = getAggregators(key.getTableName(), key.getKey());
            
            if (aggList.isEmpty()) {
                /**
                 * if we have a key that matches on TableNAme row_key columnFamily columnQualifier timestamp then it is a dup This works for the case where no
                 * aggregation is used
                 */
                boolean firstValue = true;
                int duplicates = 0;
                
                for (Value value : values) {
                    
                    if (firstValue) {
                        writeBulkIngestKey(key, value, ctx);
                        firstValue = false;
                    } else {
                        duplicates++;
                    }
                    ctx.progress();
                }
                ctx.getCounter(IngestOutput.DUPLICATE_VALUE).increment(duplicates);
            } else {
                /***
                 * Dedup tables either by timestamp or by use of the value Currently there are tables that use aggregators that use this deduping functionality.
                 * 
                 * edge: we use the timestamp to the ms to remove duplicate counts from the same record/event
                 * 
                 * Global indices (term and reverse) : Contain counts of uuids so they are deduped by the uniqueness of the value
                 * 
                 * DataWaveMetadata: Counts are aggregated as number of times fields appear. There really is no concept of dups here
                 * 
                 */
                if (TSDedupTables.contains(key.getTableName())) {
                    useTSDedup = true;
                }
                if (noTSDedupTables.contains(key.getTableName())) {
                    
                    useTSDedup = false;
                }
                
                BulkIngestKey outKey = new BulkIngestKey(key.getTableName(), key.getKey());
                if (useTSDedup && outKey.getKey().getTimestamp() > 0) {
                    /**
                     * Congratulations you have selected to use timestamp deduping
                     * 
                     */
                    
                    ts = (outKey.getKey().getTimestamp()) / MILLISPERDAY;
                    outKey.getKey().setTimestamp(-1 * ts);
                    boolean firstValue = true;
                    int duplicates = 0;
                    for (Value value : values) {
                        if (firstValue) {
                            writeBulkIngestKey(outKey, value, ctx);
                            firstValue = false;
                        } else {
                            duplicates++;
                        }
                        ctx.progress();
                    }
                    ctx.getCounter(IngestOutput.TIMESTAMP_DUPLICATE).increment(duplicates);
                } else {
                    
                    Iterator<Value> valueItr = values.iterator();
                    
                    Value reducedValue = null;
                    
                    long mergedValues = 0;
                    for (Combiner agg : aggList) {
                        
                        reducedValue = agg.reduce(key.getKey(), valueItr);
                        
                        valueItr = Iterators.singletonIterator(reducedValue);
                        
                        ctx.progress();
                        
                        mergedValues++;
                        
                        if (agg instanceof PropogatingCombiner) {
                            ((PropogatingCombiner) agg).reset();
                        }
                    }
                    
                    writeBulkIngestKey(outKey, reducedValue, ctx);
                    
                    ctx.getCounter(IngestOutput.MERGED_VALUE).increment(mergedValues);
                    
                }
                
            }
        } else {
            for (Value value : values) {
                writeBulkIngestKey(key, value, ctx);
                ctx.progress();
            }
        }
        ctx.progress();
        
    }
    
    public static long bytesToLong(byte[] b) throws IOException {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(b));
        return WritableUtils.readVLong(dis);
    }
}
