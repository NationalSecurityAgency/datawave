package datawave.ingest.mapreduce.job.metrics;

import com.google.common.collect.Multimap;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.mapreduce.handler.DataTypeHandler;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.writer.ContextWriter;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This {@link ContextWriter} wraps another writer and enables the counting of key/value pairs by table.
 */
public class KeyValueCountingContextWriter<OK,OV> implements ContextWriter<OK,OV> {
    
    private static final Logger logger = Logger.getLogger(KeyValueCountingContextWriter.class);
    private static final int COUNTS_SIZE = 64; // the number of tables
    
    public static final String GROUP = "tableCounts";
    public static final String TABLE_LABEL = "table";
    public static final String HANDLER_LABEL = "handler";
    public static final String DATATYPE_LABEL = "dataType";
    
    private final ContextWriter<OK,OV> inner;
    private final MetricsService metricsService;
    private final ReusableMetricsLabels labels;
    private final Counts<Text> counts;
    
    public KeyValueCountingContextWriter(ContextWriter<OK,OV> inner, MetricsService metricsService) {
        this.inner = inner;
        this.metricsService = metricsService;
        this.labels = new ReusableMetricsLabels();
        this.counts = new Counts<>(COUNTS_SIZE);
    }
    
    /**
     * Writes out all previously collected table counts and associates them with this event/fields.
     *
     * @param event
     *            the event container
     * @param fields
     *            the event fields
     * @param handler
     *            the event handler
     */
    public void writeMetrics(RawRecordContainer event, Multimap<String,NormalizedContentInterface> fields, DataTypeHandler<?> handler) {
        String dataType = event.getDataType().typeName();
        String handlerName = handler.getClass().getName();
        
        if (logger.isTraceEnabled()) {
            logger.trace("Writing metrics for record: dataType=" + dataType + ", handler=" + handlerName);
        }
        
        labels.clear();
        labels.put(DATATYPE_LABEL, dataType);
        labels.put(HANDLER_LABEL, handlerName);
        
        counts.flush(new FlushMetrics(fields));
    }
    
    @Override
    public void setup(Configuration conf, boolean outputTableCounters) throws IOException, InterruptedException {
        inner.setup(conf, outputTableCounters);
    }
    
    @Override
    public void write(BulkIngestKey key, Value value, TaskInputOutputContext<?,?,OK,OV> context) throws IOException, InterruptedException {
        inner.write(key, value, context);
        counts.add(key.getTableName(), 1);
    }
    
    @Override
    public void write(Multimap<BulkIngestKey,Value> entries, TaskInputOutputContext<?,?,OK,OV> context) throws IOException, InterruptedException {
        inner.write(entries, context);
        
        for (BulkIngestKey key : entries.keys()) {
            counts.add(key.getTableName(), 1);
        }
    }
    
    @Override
    public void commit(TaskInputOutputContext<?,?,OK,OV> context) throws IOException, InterruptedException {
        inner.commit(context);
    }
    
    @Override
    public void rollback() throws IOException, InterruptedException {
        inner.rollback();
    }
    
    @Override
    public void cleanup(TaskInputOutputContext<?,?,OK,OV> context) throws IOException, InterruptedException {
        try {
            metricsService.close();
        } catch (Exception e) {
            logger.error("Could not close metricsService", e);
        }
        
        inner.cleanup(context);
    }
    
    private class FlushMetrics implements Counts.FlushOp<Text> {
        
        private final Multimap<String,NormalizedContentInterface> fields;
        
        public FlushMetrics(Multimap<String,NormalizedContentInterface> fields) {
            this.fields = fields;
        }
        
        @Override
        public void flush(ConcurrentMap<Text,AtomicLong> counts) {
            for (Map.Entry<Text,AtomicLong> entry : counts.entrySet()) {
                labels.put(TABLE_LABEL, entry.getKey().toString());
                metricsService.collect(Metric.KV_PER_TABLE, labels.get(), fields, entry.getValue().get());
            }
        }
    }
}
