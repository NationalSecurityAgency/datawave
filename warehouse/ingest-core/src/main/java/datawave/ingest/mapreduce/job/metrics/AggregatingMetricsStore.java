package datawave.ingest.mapreduce.job.metrics;

import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.writer.ContextWriter;
import datawave.util.TextUtil;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This implementation pre-aggregates in memory before flushing to the underlying context writer.
 */
public class AggregatingMetricsStore<OK,OV> implements MetricsStore<OK,OV> {

    private static final Logger logger = Logger.getLogger(AggregatingMetricsStore.class);

    private final int maxSize;
    private final ContextWriter<OK,OV> contextWriter;
    private final TaskInputOutputContext<?,?,OK,OV> context;
    private final Text table;

    private Counts<String> counts;
    private FlushMetrics flusher;

    public AggregatingMetricsStore(ContextWriter<OK,OV> contextWriter, TaskInputOutputContext<?,?,OK,OV> context) {
        this.contextWriter = contextWriter;
        this.context = context;
        this.maxSize = MetricsConfiguration.getAggBufferSize(context.getConfiguration());
        this.table = new Text(MetricsConfiguration.getTable(context.getConfiguration()));
        this.counts = new Counts<>(maxSize * 2);
        this.flusher = new FlushMetrics();
    }

    @Override
    public void increase(String key, long count) {
        counts.add(key, count);

        if (counts.size() > maxSize) {
            counts.flush(flusher);
        }
    }

    @Override
    public void close() throws Exception {
        counts.flush(flusher);
    }

    private class FlushMetrics implements Counts.FlushOp<String> {
        @Override
        public void flush(ConcurrentMap<String,AtomicLong> counts) {
            for (Map.Entry<String,AtomicLong> entry : counts.entrySet()) {
                try {
                    byte[] countAsBytes = TextUtil.toUtf8(String.valueOf(entry.getValue().get()));
                    Key key = KeyConverter.fromString(entry.getKey());
                    contextWriter.write(new BulkIngestKey(table, key), new Value(countAsBytes), context);
                } catch (Exception e) {
                    logger.error("Could not flush metrics, dropping them", e);
                }
            }
        }
    }
}
