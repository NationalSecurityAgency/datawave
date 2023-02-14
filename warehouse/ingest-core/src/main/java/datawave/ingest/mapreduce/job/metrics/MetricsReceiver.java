package datawave.ingest.mapreduce.job.metrics;

import com.google.common.collect.Multimap;
import datawave.ingest.data.config.NormalizedContentInterface;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.hadoop.conf.Configuration;

import java.util.Map;

/**
 * When you add a new metric, by default, nothing will happen. There needs to be a configured receiver that processes it.
 */
public interface MetricsReceiver<OK,OV> {
    
    /**
     * Sets up any configuration.
     *
     * @param conf
     *            the configuration
     * @param ingestDate
     *            String formatted date
     */
    void configure(Configuration conf, String ingestDate);
    
    /**
     * Ensures that the appropriate config exists on the table.
     *
     * @param tableName
     *            the table name
     * @param tops
     *            the table ops
     * @param conf
     *            the configuration
     * @throws Exception
     *             if there are issues
     */
    void configureTable(String tableName, TableOperations tops, Configuration conf) throws Exception;
    
    /**
     * Process the current metric and add any produced key/values to the given counters map. The counters map is provided to eliminate the need for more object
     * instantiation.
     *
     * @param store
     *            the metrics store
     * @param metric
     *            a metric
     * @param labels
     *            a map of labels
     * @param fields
     *            map of normalized fields
     * @param value
     *            the value
     */
    void process(MetricsStore<OK,OV> store, Metric metric, Map<String,String> labels, Multimap<String,NormalizedContentInterface> fields, long value);
    
    /**
     * Gets the metric that this receiver can handle.
     *
     * @return the metric for this receiver
     */
    Metric getMetric();
}
