package nsa.datawave.ingest.mapreduce.job.metrics;

import com.google.common.collect.Multimap;
import nsa.datawave.ingest.data.config.NormalizedContentInterface;
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
     * @param ingestDate
     *            String formatted date
     */
    void configure(Configuration conf, String ingestDate);
    
    /**
     * Ensures that the appropriate config exists on the table.
     *
     * @param tableName
     * @param tops
     * @param conf
     */
    void configureTable(String tableName, TableOperations tops, Configuration conf) throws Exception;
    
    /**
     * Process the current metric and add any produced key/values to the given counters map. The counters map is provided to eliminate the need for more object
     * instantiation.
     *
     * @param store
     * @param metric
     * @param labels
     * @param fields
     * @param value
     * @return true if the metric was processed by this handler, false, otherwise
     */
    void process(MetricsStore<OK,OV> store, Metric metric, Map<String,String> labels, Multimap<String,NormalizedContentInterface> fields, long value);
    
    /**
     * Gets the metric that this receiver can handle.
     *
     * @return
     */
    Metric getMetric();
}
