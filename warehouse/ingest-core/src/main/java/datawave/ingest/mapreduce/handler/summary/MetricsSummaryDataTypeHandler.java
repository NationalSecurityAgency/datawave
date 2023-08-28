package datawave.ingest.mapreduce.handler.summary;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.ingest.IngestHelperInterface;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.util.StringUtils;
import datawave.webservice.common.logging.ThreadConfigurableLogger;

/**
 * Creates MetricsSummary entries.
 *
 * Uses the supplied configuration to specify what fields to pull into the row, column family, and column qualifier The configured fields are pulled out of the
 * fields Multimap. Any fields that are not present are filled with empty values. For the case of multiple values present for a field, each unique value with
 * necessitate the creation of a new output row.
 */
public class MetricsSummaryDataTypeHandler<KEYIN> extends SummaryDataTypeHandler<KEYIN> {

    private static final Logger log = ThreadConfigurableLogger.getLogger(MetricsSummaryDataTypeHandler.class);

    // configuration keys
    public static final String METRICS_SUMMARY_PROP_PREFIX = "metrics-";
    public static final String METRICS_SUMMARY_DATATYPE = "metrics-summary";
    public static final char SUMMARY_FIELDS_SEPARATOR = ',';

    public static final String METRICS_SUMMARY_ROWID_FIELDS = METRICS_SUMMARY_DATATYPE + ".rowid.fields";
    public static final String METRICS_SUMMARY_COLF_FIELDS = METRICS_SUMMARY_DATATYPE + ".colf.fields";
    public static final String METRICS_SUMMARY_COLQ_FIELDS = METRICS_SUMMARY_DATATYPE + ".regex.colq.fields";
    public static final String METRICS_SUMMARY_LPRIORITY = METRICS_SUMMARY_PROP_PREFIX + "summary.table.loader.priority";
    public static final String METRICS_SUMMARY_TABLE_NAME = METRICS_SUMMARY_PROP_PREFIX + "summary.table.name";

    // constants
    private static final int EXPECTED_VALUES_PER_KEY = 1;
    public static final Value INCREMENT_ONE_VALUE = new Value("1".getBytes());

    private HandlerDelegate delegate = new HandlerDelegate();

    @Override
    public void setup(TaskAttemptContext context) {
        super.setup(context);
        delegate.setup(context);
    }

    @Override
    public String[] getTableNames(Configuration conf) {
        return delegate.getTableNames(conf);
    }

    @Override
    public int[] getTableLoaderPriorities(Configuration conf) {
        return delegate.getTableLoaderPriorities(conf);
    }

    @Override
    protected Multimap<BulkIngestKey,Value> createEntries(RawRecordContainer record, Multimap<String,NormalizedContentInterface> fields,
                    ColumnVisibility origVis, long timestamp, IngestHelperInterface iHelper) {
        return delegate.createEntries(record, fields, origVis, timestamp, iHelper);
    }

    /**
     * Inner class to enable extension and code sharing independent of parent class hierarchy
     */
    public static class HandlerDelegate {

        // internal state configured during setup
        private Text metricsSummaryTableName = null;
        private List<String> rowIdFields = null;
        private List<String> colFamFields = null;
        private List<Matcher> colQualFieldsRegexList;
        private MetricsSummaryFormatter metricsSummaryFormatter;

        public Text getMetricsSummaryTableName() {
            return metricsSummaryTableName;
        }

        public void setup(TaskAttemptContext context) {
            Configuration conf = context.getConfiguration();
            setMetricsSummaryFormatter(new MetricsSummaryFormatter());
            setTableName(conf);
            setKeyComponentDefinitions(conf);
        }

        public void setMetricsSummaryTableName(Text metricsSummaryTableName) {
            this.metricsSummaryTableName = metricsSummaryTableName;
        }

        public List<String> getRowIdFields() {
            return rowIdFields;
        }

        public void setRowIdFields(List<String> rowIdFields) {
            this.rowIdFields = rowIdFields;
        }

        public List<String> getColFamFields() {
            return colFamFields;
        }

        public void setColFamFields(List<String> colFamFields) {
            this.colFamFields = colFamFields;
        }

        public List<Matcher> getColQualFieldsRegexList() {
            return colQualFieldsRegexList;
        }

        public void setColQualFieldsRegexList(List<Matcher> colQualFieldsRegexList) {
            this.colQualFieldsRegexList = colQualFieldsRegexList;
        }

        public MetricsSummaryFormatter getMetricsSummaryFormatter() {
            return metricsSummaryFormatter;
        }

        public void setMetricsSummaryFormatter(MetricsSummaryFormatter metricsSummaryFormatter) {
            this.metricsSummaryFormatter = metricsSummaryFormatter;
        }

        public void setTableName(Configuration conf) {
            String tableName = conf.get(METRICS_SUMMARY_TABLE_NAME);
            if (tableName == null) {
                log.warn(METRICS_SUMMARY_TABLE_NAME + " not specified, no summary data will be created.");
            } else {
                this.metricsSummaryTableName = new Text(tableName);
            }
        }

        public void setKeyComponentDefinitions(Configuration conf) {
            rowIdFields = Arrays.asList(StringUtils.split(conf.get(METRICS_SUMMARY_ROWID_FIELDS), SUMMARY_FIELDS_SEPARATOR, true));
            colFamFields = Arrays.asList(StringUtils.split(conf.get(METRICS_SUMMARY_COLF_FIELDS), SUMMARY_FIELDS_SEPARATOR, true));

            String[] colQualFields = StringUtils.split(conf.get(METRICS_SUMMARY_COLQ_FIELDS), SUMMARY_FIELDS_SEPARATOR, true);
            Matcher[] mColQualFieldsRegex = new Matcher[colQualFields.length];

            int index = 0;
            for (String colQField : colQualFields) {
                mColQualFieldsRegex[index] = Pattern.compile(colQField).matcher("");
                ++index;
            }
            colQualFieldsRegexList = Lists.newArrayList(mColQualFieldsRegex);
        }

        public String[] getTableNames(Configuration conf) {
            List<String> tableNames = new ArrayList<>(4);
            String tableName = conf.get(METRICS_SUMMARY_TABLE_NAME);
            if (tableName != null) {
                tableNames.add(tableName);
            }
            return tableNames.toArray(new String[tableNames.size()]);
        }

        public int[] getTableLoaderPriorities(Configuration conf) {
            int[] loaderPriorities = new int[1];
            int index = 0;

            if (conf.get(METRICS_SUMMARY_TABLE_NAME) != null) {
                loaderPriorities[index] = conf.getInt(METRICS_SUMMARY_LPRIORITY, 20);
                ++index;
            }

            if (index != 1) {
                return Arrays.copyOf(loaderPriorities, index);
            }
            return loaderPriorities;
        }

        public Multimap<BulkIngestKey,Value> createEntries(RawRecordContainer record, Multimap<String,NormalizedContentInterface> fields,
                        ColumnVisibility origVis, long timestamp, IngestHelperInterface iHelper) {
            String hour = getHour(fields);
            if (hour == null) {
                return HashMultimap.create();
            }

            Set<Text> rowIds = Sets.newHashSet(metricsSummaryFormatter.format(rowIdFields, fields, hour));
            Set<Text> colFs = Sets.newHashSet(metricsSummaryFormatter.format(colFamFields, fields, null));
            Set<Text> colQs = Sets.newHashSet(metricsSummaryFormatter.getSummaryValuesRegex(colQualFieldsRegexList, fields));

            if (log.isTraceEnabled()) {
                log.trace("Creating Keys for...rowIds.size() [" + rowIds.size() + "] colFs.size() [" + colFs.size() + "] colQs.size() [" + colQs.size() + "]");
            }

            ColumnVisibility vis = new ColumnVisibility(origVis.flatten());

            @SuppressWarnings("unchecked")
            Set<List<Text>> cartesianProduct = Sets.cartesianProduct(rowIds, colFs, colQs);

            Multimap<BulkIngestKey,Value> values = ArrayListMultimap.create(cartesianProduct.size(), EXPECTED_VALUES_PER_KEY);

            for (List<Text> textComponents : cartesianProduct) {
                Text row = textComponents.get(0);
                Text cf = textComponents.get(1);
                Text cq = textComponents.get(2);
                Preconditions.checkArgument(textComponents.size() == 3);
                Key k = new Key(row, cf, cq, vis, timestamp);
                final BulkIngestKey bk = new BulkIngestKey(metricsSummaryTableName, k);
                values.put(bk, INCREMENT_ONE_VALUE);
            }

            if (log.isTraceEnabled()) {
                log.trace("Created [" + values.size() + "] keys for ingest");
            }

            return values;
        }

        private String getHour(Multimap<String,NormalizedContentInterface> fields) {
            Collection<NormalizedContentInterface> fieldValues = fields.get("CREATETIME");
            if (fieldValues == null || fieldValues.isEmpty()) {
                return null;
            }

            NormalizedContentInterface nci = (NormalizedContentInterface) fieldValues.toArray()[0];
            long time = Long.parseLong(nci.getEventFieldValue()) / 60 / 60;
            return String.valueOf(time);
        }
    }
}
