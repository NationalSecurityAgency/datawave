package datawave.ingest.mapreduce.job.metrics;

import com.google.common.base.Preconditions;
import com.google.common.collect.Multimap;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.mapreduce.job.writer.ContextWriter;
import datawave.util.time.DateHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.log4j.Logger;

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Receives ingest metrics and processes them asynchronously. Once the queue fills up, this service will block metrics producers.
 * <p>
 * Metrics have four components <br>
 * 1) Name : Name for this metric <br>
 * 2) Labels : Labels are used to determine if this metric is recorded. <br>
 * 3) Fields : Fields are like labels except that they will be recorded along with the name and value.<br>
 * 3) Value : The magnitude of this metric (usually count). <br>
 * <p>
 * An example metric looks like:
 * 
 * <pre>
 * {@code
 * 
 *     name = "KeyValueCounts"
 *     labels = { "table": "shard", "dataType": "flow1" }
 *     fields = { "dataType": "flow1", "fileExtension" : "gz" }
 *     value = 1
 * }
 * </pre>
 */
public class MetricsService<OK,OV> implements AutoCloseable {
    
    private static final Logger logger = Logger.getLogger(MetricsService.class);
    
    public static final String WILDCARD = "*";
    
    private final Map<Metric,MetricsReceiver> receivers;
    private final String date;
    
    private final Multimap<String,String> enabledLabels;
    private final Set<String> enabledKeys;
    private final Set<String> wildcardedLabels;
    
    private final Set<String> fieldNames;
    private final MetricsStore<OK,OV> store;
    
    public MetricsService(ContextWriter<OK,OV> contextWriter, TaskInputOutputContext<?,?,OK,OV> context) {
        Configuration conf = context.getConfiguration();
        
        this.date = DateHelper.format(new Date());
        
        this.fieldNames = MetricsConfiguration.getFieldNames(conf);
        this.enabledLabels = MetricsConfiguration.getLabels(conf);
        this.enabledKeys = enabledLabels.keySet();
        
        this.wildcardedLabels = new HashSet<>();
        for (Map.Entry<String,String> entry : enabledLabels.entries()) {
            if (WILDCARD.equals(entry.getValue())) {
                wildcardedLabels.add(entry.getKey());
            }
        }
        
        this.receivers = new HashMap<>();
        for (MetricsReceiver receiver : MetricsConfiguration.getReceivers(conf)) {
            this.receivers.put(receiver.getMetric(), receiver);
            receiver.configure(conf, date);
        }
        
        this.store = new AggregatingMetricsStore<>(contextWriter, context);
        
        if (logger.isInfoEnabled()) {
            logger.info("Metrics Service Initialized");
            logger.info("enabledLabels = " + enabledLabels);
            logger.info("receivers = " + receivers);
            logger.info("fieldNames = " + fieldNames);
        }
        
        Preconditions.checkNotNull(fieldNames);
        Preconditions.checkArgument(!enabledLabels.isEmpty());
        Preconditions.checkArgument(!receivers.isEmpty());
    }
    
    /**
     * Collects a metric with the given fields. NOTE: keysAndValues are a vararg that accepts "key1, value1, key2, value2... An exception will be thrown if the
     * keysAndValues are not an even number.
     *
     * @param metric
     *            the metric
     * @param labels
     *            a map of labels
     * @param fields
     *            mapping of normalized event fields
     * @param value
     *            the event value
     * @throws IllegalArgumentException
     *             If keysAndValues are not even (missing part of a pair).
     */
    public void collect(Metric metric, Map<String,String> labels, Multimap<String,NormalizedContentInterface> fields, Long value) {
        if (logger.isTraceEnabled()) {
            logger.trace("Received metric " + metric + " with labels " + labels);
        }
        
        if (shouldCollect(labels)) {
            
            if (logger.isTraceEnabled()) {
                logger.trace("Metric " + metric + " collected.");
            }
            
            MetricsReceiver receiver = receivers.get(metric);
            
            if (receiver != null) {
                try {
                    receiver.process(store, metric, labels, fields, value);
                } catch (Exception e) {
                    logger.error("Error occurred with receiver: " + receiver, e);
                }
            }
        }
    }
    
    private boolean shouldCollect(Map<String,String> metricLabels) {
        for (String key : enabledKeys) {
            String metricsValue = metricLabels.get(key);
            
            if (!(metricsValue == null || isWildcard(key) || enabledLabels.containsEntry(key, metricsValue))) {
                return false;
            }
        }
        
        return !enabledKeys.isEmpty();
    }
    
    private boolean isWildcard(String labelKey) {
        return wildcardedLabels.contains(labelKey);
    }
    
    @Override
    public void close() {
        try {
            store.close();
        } catch (Exception e) {
            logger.error("Could not close the metrics service, some metrics may have not been written", e);
        }
    }
}
