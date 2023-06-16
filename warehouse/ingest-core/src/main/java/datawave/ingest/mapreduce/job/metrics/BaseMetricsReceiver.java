package datawave.ingest.mapreduce.job.metrics;

import com.google.common.base.Preconditions;
import com.google.common.collect.Multimap;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.util.TextUtil;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * The basic functionality shared by all current MetricsReceivers.
 * <p>
 * Currently the metrics name is treated as the column family. It will have a combiner added to sum the values.
 * <p>
 * Each field's name/value will be appended with a delimiter (null character) and placed in the column qualifier.
 */
public abstract class BaseMetricsReceiver<OK,OV> implements MetricsReceiver<OK,OV> {

    public static final String QUAL_DELIM = "\u0000";

    public static final String ITER_NAME = "sum";
    public static final int ITER_PRIORITY = 15;

    private final Metric metric;
    private final String metricName;

    private String ingestDate;
    private Set<String> fieldNames;
    private int numShards;

    private String defaultVisibility = TextUtil.fromUtf8(new ColumnVisibility().getExpression());

    public BaseMetricsReceiver(Metric metric) {
        this.metric = metric;
        this.metricName = metric.toString();
    }

    @Override
    public void configure(Configuration conf, String ingestDate) {
        this.ingestDate = ingestDate;
        this.numShards = MetricsConfiguration.getNumShards(conf);

        this.fieldNames = MetricsConfiguration.getFieldNames(conf);
        String metricsTable = MetricsConfiguration.getTable(conf);

        Preconditions.checkArgument(numShards > 0);
        Preconditions.checkNotNull(ingestDate);
        Preconditions.checkArgument(StringUtils.isNotBlank(metricsTable));
    }

    @Override
    public void configureTable(String table, TableOperations tops, Configuration conf) throws Exception {
        IteratorSetting is = tops.getIteratorSetting(table, ITER_NAME, IteratorUtil.IteratorScope.scan);

        String metricName = metric.toString();

        if (is == null) {
            // create a fresh iterator
            Map<String,String> options = new TreeMap<>();
            options.put("type", "STRING");
            options.put("columns", metricName);

            is = new IteratorSetting(ITER_PRIORITY, ITER_NAME, SummingCombiner.class, options);

            tops.attachIterator(table, is);
        } else {
            // if iterator exists, piggyback on it
            String columns = is.getOptions().get("columns");

            if (!columns.contains(metricName)) {
                for (IteratorUtil.IteratorScope scope : IteratorUtil.IteratorScope.values()) {
                    String config = String.format("table.iterator.%s.%s.opt.columns", scope, ITER_NAME);
                    tops.setProperty(table, config, columns.concat("," + metricName));
                }
            }
        }
    }

    @Override
    public Metric getMetric() {
        return metric;
    }

    /*
     * Gets the metric in String form.
     */
    protected String getMetricName() {
        return metricName;
    }

    /*
     * Constructs a collection of keys that will be increased for this event. Override this if you need something more specific than field key/values.
     */
    protected Iterable<String> constructKeys(Metric metric, Map<String,String> labels, Multimap<String,NormalizedContentInterface> fields) {
        List<String> keys = new LinkedList<>();

        for (String field : getFieldNames()) {
            for (NormalizedContentInterface entry : fields.get(field)) {
                String fieldAndValue = field + QUAL_DELIM + entry.getEventFieldValue();
                String keyStr = KeyConverter.toString(getShardId(fieldAndValue), metricName, fieldAndValue, getVisibility());
                keys.add(keyStr);
            }
        }

        return keys;
    }

    /**
     * @return field names to record metrics on
     */
    protected Set<String> getFieldNames() {
        return fieldNames;
    }

    /**
     * @return number of shards to use
     */
    protected int getNumShards() {
        return numShards;
    }

    /**
     * Override this to add a column visibility to your keys.
     *
     * @return empty visibility
     */
    protected String getVisibility() {
        return defaultVisibility;
    }

    @Override
    public void process(MetricsStore<OK,OV> store, Metric metric, Map<String,String> labels, Multimap<String,NormalizedContentInterface> fields, long value) {
        for (String key : constructKeys(metric, labels, fields)) {
            store.increase(key, value);
        }
    }

    protected String getShardId(String key) {
        int index = (Integer.MAX_VALUE & key.hashCode()) % numShards;
        return ingestDate + "_" + index;
    }
}
