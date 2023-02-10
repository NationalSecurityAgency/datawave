package datawave.ingest.mapreduce.job.metrics;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Set;
import java.util.TreeSet;

/**
 * Houses configurations needed by {@link MetricsService}.
 */
public class MetricsConfiguration {
    
    public static final String METRICS_ENABLED_CONFIG = "ingest.metrics.enabled";
    public static final String METRICS_TABLE_CONFIG = "ingest.metrics.table.name";
    public static final int METRICS_TABLE_DEFAULT_PRIORITY = 100;
    public static final String METRICS_TABLE_PRIORITY = "ingest.metrics.loader.priority";
    public static final String NUM_SHARDS_CONFIG = "ingest.metrics.num.shards";
    public static final String ENABLED_LABELS_CONFIG = "ingest.metrics.enabled.labels";
    public static final String FIELDS_CONFIG = "ingest.metrics.fields";
    public static final String RECEIVERS_CONFIG = "ingest.metrics.receivers";
    
    public static final String METRICS_AGGREGATING_BUFFER_SIZE = "ingest.metrics.agg.buffer.size";
    
    private static final int DEFAULT_BUFFER_SIZE = 4096;
    
    private static final String LIST_SEP = ",";
    private static final String ENTRY_SEP = ",";
    
    private static final String KV_SEP = "=";
    private static final Logger logger = Logger.getLogger(MetricsConfiguration.class);
    
    /**
     * The buffer used for aggregating metrics counts before flushing to the context writer.
     *
     * @param conf
     *            the configuration
     * @return agg buffer size
     */
    public static int getAggBufferSize(Configuration conf) {
        return conf.getInt(METRICS_AGGREGATING_BUFFER_SIZE, DEFAULT_BUFFER_SIZE);
    }
    
    /**
     * Instantiates the configured receivers.
     *
     * @param conf
     *            the configuration
     * @return receivers
     */
    public static Collection<MetricsReceiver> getReceivers(Configuration conf) {
        Collection<MetricsReceiver> receivers = new LinkedList<>();
        
        String receiversStr = conf.get(RECEIVERS_CONFIG);
        if (StringUtils.isNotBlank(receiversStr)) {
            String[] receiversClasses = receiversStr.split(LIST_SEP);
            for (String receiverClass : receiversClasses) {
                try {
                    receiverClass = receiverClass.trim();
                    Class<? extends MetricsReceiver> clazz = (Class<? extends MetricsReceiver>) Class.forName(receiverClass);
                    receivers.add(clazz.newInstance());
                } catch (Exception e) {
                    logger.error("Could not instantiate receiver: " + receiverClass + ", skipping...", e);
                }
            }
        }
        
        return receivers;
    }
    
    /**
     * Gets the currently configured fields to produce metrics on.
     *
     * @param conf
     *            a configuration
     * @return fields
     */
    public static Set<String> getFieldNames(Configuration conf) {
        Set<String> fields = new TreeSet<>();
        
        String fieldsStr = conf.get(FIELDS_CONFIG);
        
        if (StringUtils.isNotBlank(fieldsStr)) {
            Collections.addAll(fields, fieldsStr.split(LIST_SEP));
        }
        
        return fields;
    }
    
    /**
     * Disables the metrics feature.
     *
     * @param conf
     *            a configuration
     */
    public static void disable(Configuration conf) {
        conf.setBoolean(METRICS_ENABLED_CONFIG, false);
    }
    
    /**
     * Gets the configured metrics labels to allow for collection.
     *
     * @param conf
     *            a configuration
     * @return labels
     */
    public static Multimap<String,String> getLabels(Configuration conf) {
        String labelsStr = conf.get(ENABLED_LABELS_CONFIG);
        Multimap<String,String> enabledLabels = HashMultimap.create();
        
        if (StringUtils.isNotBlank(labelsStr)) {
            String[] entries = labelsStr.trim().split(ENTRY_SEP);
            for (String entry : entries) {
                String[] tokens = entry.split(KV_SEP);
                enabledLabels.put(tokens[0].trim(), tokens[1].trim());
            }
        }
        
        return enabledLabels;
    }
    
    /**
     * Gets the configured number of shards.
     *
     * @param conf
     *            a configuration
     * @return number of shards to use
     */
    public static int getNumShards(Configuration conf) {
        return conf.getInt(NUM_SHARDS_CONFIG, -1);
    }
    
    /**
     * Gets the configured ingest metrics table name.
     *
     * @param conf
     *            a configuration
     * @return table name
     */
    public static String getTable(Configuration conf) {
        return conf.get(METRICS_TABLE_CONFIG);
    }
    
    /**
     * Gets the configured ingest metrics table priority.
     *
     * @param conf
     *            a configuration
     * @return table priority (defaults to 100)
     */
    public static int getTablePriority(Configuration conf) {
        try {
            return conf.getInt(METRICS_TABLE_PRIORITY, METRICS_TABLE_DEFAULT_PRIORITY);
        } catch (NumberFormatException e) {
            logger.error("Could not parse " + METRICS_TABLE_PRIORITY + " property as an Integer, using " + METRICS_TABLE_DEFAULT_PRIORITY);
            return METRICS_TABLE_DEFAULT_PRIORITY;
        }
    }
    
    /**
     * Checks if the ingest metrics feature is enabled.
     *
     * @param conf
     *            a configuration
     * @return true if explicitly enabled in the config, false, otherwise.
     */
    public static boolean isEnabled(Configuration conf) {
        boolean isEnabled = conf.getBoolean(METRICS_ENABLED_CONFIG, false);
        
        if (logger.isTraceEnabled()) {
            logger.trace("Metrics enabled = " + isEnabled);
        }
        
        return isEnabled;
    }
}
