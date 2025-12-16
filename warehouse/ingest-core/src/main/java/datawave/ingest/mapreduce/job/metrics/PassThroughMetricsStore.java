package datawave.ingest.mapreduce.job.metrics;

import java.io.UnsupportedEncodingException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.log4j.Logger;

import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.writer.ContextWriter;

/**
 * This implementation is writes directly to the underlying context. Relies on the use of a combiner for efficiency.
 */
public class PassThroughMetricsStore<OK,OV> implements MetricsStore<OK,OV> {
    private static final String ENCODING = "UTF-8";
    private static final Value ONE = createOne();
    private static final Logger logger = Logger.getLogger(PassThroughMetricsStore.class);

    private final Text table;

    /*
     * Initialize a reusable value for a count of 1, which is the most common use.
     */
    private static Value createOne() {
        try {
            return new Value("1".getBytes(ENCODING));
        } catch (UnsupportedEncodingException e) {
            return null;
        }
    }

    private final ContextWriter<OK,OV> contextWriter;
    private final TaskInputOutputContext<?,?,OK,OV> context;

    public PassThroughMetricsStore(ContextWriter<OK,OV> contextWriter, TaskInputOutputContext<?,?,OK,OV> context) {
        this.contextWriter = contextWriter;
        this.context = context;
        this.table = new Text(MetricsConfiguration.getTable(context.getConfiguration()));
    }

    @Override
    public void increase(String key, long count) {
        try {
            Value v = (count == 1L && ONE != null) ? ONE : new Value(String.valueOf(count).getBytes(ENCODING));
            Key k = KeyConverter.fromString(key);
            contextWriter.write(new BulkIngestKey(table, k), v, context);
        } catch (Exception e) {
            logger.error("Could not write metrics to the context writer, dropping them...", e);
        }
    }

    @Override
    public void close() throws Exception {}
}
