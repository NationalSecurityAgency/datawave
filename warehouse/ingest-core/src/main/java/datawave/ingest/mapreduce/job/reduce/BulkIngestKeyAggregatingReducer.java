package datawave.ingest.mapreduce.job.reduce;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.log4j.Logger;

import com.google.common.collect.Iterators;

import datawave.ingest.data.TypeRegistry;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.IngestJob;
import datawave.ingest.mapreduce.job.writer.BulkContextWriter;
import datawave.ingest.mapreduce.job.writer.ContextWriter;
import datawave.ingest.mapreduce.job.writer.LiveContextWriter;
import datawave.ingest.metric.IngestOutput;
import datawave.ingest.table.aggregator.PropogatingCombiner;

@SuppressWarnings("deprecation")
public class BulkIngestKeyAggregatingReducer<K2,V2> extends AggregatingReducer<BulkIngestKey,Value,K2,V2> {
    private static final Logger log = Logger.getLogger(BulkIngestKeyAggregatingReducer.class);

    public static final String VERBOSE_COUNTERS = "verboseCounters";
    public static final String MAPRED_OUTPUT_VALUE_CLASS = "mapreduce.job.output.value.class";
    public static final String CONTEXT_WRITER_CLASS = "ingest.aggregating.reducer.context.writer.class";
    public static final String CONTEXT_WRITER_OUTPUT_TABLE_COUNTERS = "ingest.aggregating.reducer.context.writer.output.table.counters";
    public static final String VERBOSE_PARTITIONING_COUNTERS = "table.partition.counters"; // you must also be in verbose mode to use this
    private ContextWriter<K2,V2> contextWriter = null;
    private boolean usingCombiner = false;
    private boolean verboseCounters = false;
    private boolean superExtraExplanatoryHappyPartitionerMode = false;

    @SuppressWarnings("unchecked")
    @Override
    public void setup(Configuration conf) throws IOException, InterruptedException {
        super.setup(conf);

        // Initialize the Type Registry
        TypeRegistry.getInstance(conf);

        setupContextWriter(conf);

        usingCombiner = conf.getBoolean(BulkIngestKeyDedupeCombiner.USING_COMBINER, false);
        verboseCounters = conf.getBoolean(VERBOSE_COUNTERS, verboseCounters);
        superExtraExplanatoryHappyPartitionerMode = conf.getBoolean(VERBOSE_PARTITIONING_COUNTERS, false);
    }

    protected void setupContextWriter(Configuration conf) throws IOException {
        Class<ContextWriter<K2,V2>> contextWriterClass = null;
        if (Mutation.class.equals(conf.getClass(MAPRED_OUTPUT_VALUE_CLASS, null))) {
            contextWriterClass = (Class<ContextWriter<K2,V2>>) conf.getClass(CONTEXT_WRITER_CLASS, LiveContextWriter.class, ContextWriter.class);
        } else {
            contextWriterClass = (Class<ContextWriter<K2,V2>>) conf.getClass(CONTEXT_WRITER_CLASS, BulkContextWriter.class, ContextWriter.class);
        }
        try {
            setContextWriter(contextWriterClass.newInstance());
            contextWriter.setup(conf, conf.getBoolean(CONTEXT_WRITER_OUTPUT_TABLE_COUNTERS, false));
        } catch (Exception e) {
            throw new IOException("Failed to initialized " + contextWriterClass + " from property " + CONTEXT_WRITER_CLASS, e);
        }
    }

    public void setContextWriter(ContextWriter<?,?> writer) {
        this.contextWriter = (ContextWriter<K2,V2>) writer;
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
        // the reducer needs to preserve order writing out, so lets avoid any caching
        contextWriter.commit(ctx);
    }

    @Override
    public void doReduce(BulkIngestKey key, Iterable<Value> values, TaskInputOutputContext<?,?,K2,V2> ctx) throws IOException, InterruptedException {
        long ts = 0;
        boolean useTSDedup = false;

        if (verboseCounters) {
            values = IngestJob.verboseCounters(ctx, "reducer", key, values);
        }

        // if super verbose counters, then add one to each
        if (superExtraExplanatoryHappyPartitionerMode && TaskType.REDUCE == ctx.getTaskAttemptID().getTaskType()) {
            int reducerId = ctx.getTaskAttemptID().getTaskID().getId();
            // increment one per key
            if (reducerId < 50) {
                ctx.getCounter("REDUCER " + Integer.toString(reducerId), "TABLE " + key.getTableName()).increment(1);
                ctx.getCounter("TABLE " + key.getTableName(), "REDUCER " + Integer.toString(reducerId)).increment(1);
            }
        }
        if (useAggregators(key.getTableName())) {

            List<Combiner> aggList = getAggregators(key.getTableName(), key.getKey());

            if (aggList.isEmpty()) {
                boolean firstValue = true;
                boolean foundDuplicate = false;
                for (Value value : values) {
                    if (firstValue) {
                        writeBulkIngestKey(key, value, ctx);
                        firstValue = false;
                    } else {
                        foundDuplicate = true;
                        break;
                    }
                    ctx.progress();
                }
                if (foundDuplicate)
                    ctx.getCounter(IngestOutput.DUPLICATE_KEY).increment(1);
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
                if ((!usingCombiner) && useTSDedup) {
                    /**
                     * Congratulations you have selected to use timestamp deduping
                     *
                     */
                    ts = (outKey.getKey().getTimestamp()) / MILLISPERDAY;
                    outKey.getKey().setTimestamp(ts * MILLISPERDAY);
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

    protected ContextWriter<K2,V2> getContextWriter() {
        return contextWriter;
    }

    public static long bytesToLong(byte[] b) throws IOException {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(b));
        return WritableUtils.readVLong(dis);
    }

}
