package datawave.metrics.analytic;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.hadoop.mapreduce.AccumuloInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import datawave.metrics.config.MetricsConfig;
import datawave.metrics.mapreduce.util.JobSetupUtil;
import datawave.metrics.util.Connections;
import datawave.util.time.DateHelper;

/**
 * This MapReduce job computes a by-day summary of query metrics.
 */
public class QueryMetricsSummaryLoader extends Configured implements Tool {
    private static final Logger log = Logger.getLogger(QueryMetricsSummaryLoader.class);
    private static final String USER = "USER";
    private static final String QUERY_LOGIC = "QUERY_LOGIC";
    private static final String NUM_RESULTS = "NUM_RESULTS";
    private static final String ELAPSED_TIME = "ELAPSED_TIME";
    private static final String WS_HOST = "HOST";
    private static final String ERROR_CODE = "ERROR_CODE";
    private static final String ERROR_MESSAGE = "ERROR_MESSAGE";
    private static final String CREATE_DATE = "CREATE_DATE";
    private static final String BEGIN_DATE = "BEGIN_DATE";

    private static final String QUERY_METRICS_REGEX = "^querymetrics.*";

    /**
     * Read information from the sharded query metrics table and output it in our daily metrics summary format.
     */
    public static class QueryMetricsMapper extends Mapper<Key,Value,Key,Value> {

        private final HashSet<String> uniqueUsers = new HashSet<>();

        private boolean useHourlyPrecision = false;

        private Text prevRow = null;
        private Text prevCF = null;

        private final List<Key> currentQueryMetric = new ArrayList<>();
        private Text currentRow = new Text();
        private Text currentCF = new Text();
        private DateTimeFormatter deltaDtf;

        @Override
        protected void setup(Mapper<Key,Value,Key,Value>.Context context) throws IOException, InterruptedException {
            super.setup(context);
            useHourlyPrecision = HourlyPrecisionHelper.checkForHourlyPrecisionOption(context.getConfiguration(), log);
            deltaDtf = DateTimeFormatter.ofPattern("yyyyMMdd HHmmss");
        }

        @Override
        protected void map(Key key, Value value, Context context) throws IOException, InterruptedException {

            currentRow = key.getRow();
            currentCF = key.getColumnFamily();

            if (prevRow == null) {
                // first
                prevRow = currentRow;
                prevCF = currentCF;
                currentQueryMetric.clear();
            } else if (!currentCF.equals(prevCF) || !currentRow.equals(prevRow)) {
                // Query metric is finished process last
                processQueryMetric(context);

                // Start on next query metric
                currentQueryMetric.clear();
                prevRow = currentRow;
                prevCF = currentCF;
            }

            currentQueryMetric.add(key);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);

            // Process the final query metric
            processQueryMetric(context);
        }

        private String getTimeUnit(Key key) {
            if (useHourlyPrecision) {
                return DateHelper.formatToHour(key.getTimestamp());
            } else {
                Text row = key.getRow();
                return new String(row.getBytes(), 0, row.find("_"));
            }
        }

        private void processQueryMetric(Context context) throws IOException, InterruptedException {

            // Metric fields
            String queryLogic = null;
            long elapsedTimeMs = 0L;

            Text cq = new Text();
            boolean errorReported = false;
            String outRow = null;
            String beginDate = null;
            String createDate = null;

            for (Key key : currentQueryMetric) {
                if (outRow == null) { // First
                    outRow = getTimeUnit(key);
                }

                key.getColumnQualifier(cq);
                String[] cqPair = cq.toString().split("\0");

                if (cqPair.length == 2) {
                    String field = cqPair[0];
                    String fieldValue = cqPair[1];

                    switch (field) {
                        case USER:
                            if (!uniqueUsers.contains(fieldValue)) {
                                context.write(makeKey(outRow, QueryMetricsColumns.QUERY_USERS_CF, QueryMetricsColumns.TOTAL_CQ), makeValue(1));
                                uniqueUsers.add(fieldValue);
                            }
                            break;
                        case NUM_RESULTS:
                            try {
                                long numResults = Long.parseLong(fieldValue);
                                if (numResults > 0) { // Count non-productive queries separately
                                    context.write(makeKey(outRow, QueryMetricsColumns.QUERY_RESULT_COUNT_CF, MetricsDailySummaryReducer.STATS_METRIC_VALUE),
                                                    makeValue(numResults));
                                    context.write(makeKey(outRow, QueryMetricsColumns.QUERY_RESULT_COUNT_CF, QueryMetricsColumns.TOTAL_CQ),
                                                    makeValue(numResults));
                                    context.write(makeKey(outRow, QueryMetricsColumns.QUERIES_EXECUTED_CF, QueryMetricsColumns.PRODUCTIVE_CQ), makeValue(1));

                                } else {
                                    context.write(makeKey(outRow, QueryMetricsColumns.QUERIES_EXECUTED_CF, QueryMetricsColumns.NON_PRODUCTIVE_CQ),
                                                    makeValue(1));
                                }
                                context.write(makeKey(outRow, QueryMetricsColumns.QUERIES_EXECUTED_CF, QueryMetricsColumns.TOTAL_CQ), makeValue(1));
                            } catch (NumberFormatException nfe) {
                                log.error("Failed to parse number for NUM_RESULTS =>" + fieldValue);
                            }
                            break;
                        case ELAPSED_TIME:
                            try {
                                elapsedTimeMs += Long.parseLong(fieldValue);
                                context.write(makeKey(outRow, QueryMetricsColumns.QUERY_RESPONSE_TIME_MS_CF, MetricsDailySummaryReducer.STATS_METRIC_VALUE),
                                                makeValue(elapsedTimeMs));
                            } catch (NumberFormatException nfe) {
                                log.error("Failed to parse number for QUERY_RESPONSE_TIME_MS =>" + fieldValue);
                            }

                            break;
                        case WS_HOST:
                            context.write(makeKey(outRow, QueryMetricsColumns.WEB_SERVER_HOST, fieldValue), makeValue(1));
                            break;
                        case QUERY_LOGIC:
                            queryLogic = fieldValue;
                            context.write(makeKey(outRow, QueryMetricsColumns.QUERY_LOGIC_CF, fieldValue.toLowerCase()), makeValue(1));
                            break;
                        case ERROR_CODE:
                            context.write(makeKey(outRow, QueryMetricsColumns.QUERIES_EXECUTED_CF, QueryMetricsColumns.ERROR_CODE_CQ), makeValue(1));
                            break;
                        case ERROR_MESSAGE:
                            // Only report that a query had an error once
                            if (errorReported) {
                                break;
                            }

                            context.write(makeKey(outRow, QueryMetricsColumns.QUERIES_EXECUTED_CF, QueryMetricsColumns.ERROR_CQ), makeValue(1));
                            errorReported = true;
                            break;
                        case BEGIN_DATE:
                            beginDate = fieldValue;
                            break;
                        case CREATE_DATE:
                            createDate = fieldValue;
                            break;
                    }

                } else {
                    log.error("Invalid key/value pair in column qualifier. " + cq);
                    return;
                }
            }

            if (queryLogic != null && !queryLogic.isEmpty()) {
                String field = QueryMetricsColumns.QUERY_RESPONSE_TIME_MS_CF + "\0" + queryLogic.toLowerCase();
                context.write(makeKey(outRow, field, MetricsDailySummaryReducer.STATS_METRIC_VALUE), makeValue(elapsedTimeMs));
            }
            // this will output an entry for the delta from when a query was executed to the beginning
            // of the data that was searched for. So a users executes a query on Jun 1 and searches
            // for data in the range of Jan 1 to Mar 15, the delta calculated here would be the number
            // of days between Jan 1st and Jun 1st.
            if (beginDate != null && createDate != null) {
                LocalDateTime beginDateTime = LocalDateTime.parse(beginDate, deltaDtf);
                LocalDateTime createDateTime = LocalDateTime.parse(createDate, deltaDtf);
                long days = ChronoUnit.DAYS.between(beginDateTime, createDateTime);
                String label = "DAYS_" + days;

                context.write(makeKey(outRow, QueryMetricsColumns.QUERY_DATA_ACCESS_AGE_CF, label), makeValue(1));
            }
        }

        private Key makeKey(String row, String cf, String cq) {
            return new Key(row, cf, cq);
        }

        private Value makeValue(String value) {
            return new Value(value.getBytes());
        }

        private Value makeValue(long value) {
            return makeValue(Long.toString(value));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = JobSetupUtil.configure(args, getConf(), log);

        JobSetupUtil.printConfig(getConf(), log);

        Job job = Job.getInstance(conf);
        Configuration jconf = job.getConfiguration();
        job.setJarByClass(this.getClass());

        boolean useHourlyPrecision = Boolean.valueOf(jconf.get(MetricsConfig.USE_HOURLY_PRECISION, MetricsConfig.DEFAULT_USE_HOURLY_PRECISION));

        if (useHourlyPrecision) {
            job.setJobName("QueryMetricsSummaries (hourly)");
        } else {
            job.setJobName("QueryMetricsSummaries");
        }

        try {
            Connections.initTables(conf);
        } catch (AccumuloException | AccumuloSecurityException e) {
            throw new IOException(e);
        }

        String inputTable = jconf.get(MetricsConfig.QUERY_METRICS_EVENT_TABLE, MetricsConfig.DEFAULT_QUERY_METRICS_EVENT_TABLE);
        String outputTable = HourlyPrecisionHelper.getOutputTable(jconf, useHourlyPrecision);

        String userName = jconf.get(MetricsConfig.WAREHOUSE_USERNAME);
        String password = jconf.get(MetricsConfig.WAREHOUSE_PASSWORD);
        String instance = jconf.get(MetricsConfig.WAREHOUSE_INSTANCE);
        String zookeepers = jconf.get(MetricsConfig.WAREHOUSE_ZOOKEEPERS, "localhost");
        Authorizations auths;
        try (AccumuloClient client = Connections.warehouseClient(jconf)) {
            auths = client.securityOperations().getUserAuthorizations(client.whoami());
        }
        Collection<Range> dayRanges = JobSetupUtil.computeShardedDayRange(jconf, log);
        Range timeRange = JobSetupUtil.computeTimeRange(jconf, log);
        long delta = Long.parseLong(timeRange.getEndKey().getRow().toString()) - Long.parseLong(timeRange.getStartKey().getRow().toString());
        int numDays = (int) Math.max(1, delta / TimeUnit.DAYS.toMillis(1));

        job.setMapperClass(QueryMetricsMapper.class);
        job.setMapOutputKeyClass(Key.class);
        job.setMapOutputValueClass(Value.class);
        job.setInputFormatClass(AccumuloInputFormat.class);

        // @formatter:off
        Properties clientProperties = Accumulo.newClientProperties()
                        .to(instance, zookeepers)
                        .as(userName, password)
                        .build();
        // @formatter:on

        IteratorSetting regex = new IteratorSetting(50, RegExFilter.class);
        regex.addOption(RegExFilter.COLF_REGEX, QUERY_METRICS_REGEX);

        // @formatter:off
        AccumuloInputFormat.configure()
                        .clientProperties(clientProperties)
                        .table(inputTable)
                        .auths(auths)
                        .ranges(dayRanges)
                        .autoAdjustRanges(false)
                        .addIterator(regex)
                        .store(job);
        // @formatter:on
        // Ensure all data for a day goes to the same reducer so that we aggregate it correctly before sending to Accumulo
        RowPartitioner.configureJob(job);

        // Configure the reducer and output format to write out our metrics
        MetricsDailySummaryReducer.configureJob(job, numDays, jconf.get(MetricsConfig.INSTANCE), jconf.get(MetricsConfig.ZOOKEEPERS),
                        jconf.get(MetricsConfig.USER), jconf.get(MetricsConfig.PASS), outputTable);

        job.submit();
        JobSetupUtil.changeJobPriority(job, log);

        job.waitForCompletion(true);

        return 0;
    }

    /**
     * Expects to receive args in the order of [config opts] [dates] ... where [dates] are the last two
     *
     * @param args
     *            the string arguments
     * @throws Exception
     *             if there is an issue
     */
    public static void main(String[] args) throws Exception {
        try {
            ToolRunner.run(new QueryMetricsSummaryLoader(), args);
        } catch (Exception e) {
            e.printStackTrace(); // Called from main()
        }
    }

    public static class QueryMetricsColumns {
        public static final String ERROR_CQ = "ERROR";
        public static final String ERROR_CODE_CQ = "ERROR_CODE";
        public static final String NON_PRODUCTIVE_CQ = "NON-PRODUCTIVE";
        public static final String PRODUCTIVE_CQ = "PRODUCTIVE";
        public static final String QUERY_LOGIC_CF = "QUERY_LOGIC";
        public static final String WEB_SERVER_HOST = "WEB_SERVER_HOST";
        public static final String QUERY_RESPONSE_TIME_MS_CF = "QUERY_RESPONSE_TIME_MS";
        public static final String QUERIES_EXECUTED_CF = "QUERIES_EXECUTED";
        public static final String QUERY_RESULT_COUNT_CF = "QUERY_RESULT_COUNT";
        public static final String TOTAL_CQ = "TOTAL";
        public static final String QUERY_USERS_CF = "QUERY_USERS";
        public static final String QUERY_DATA_ACCESS_AGE_CF = "QUERY_DATA_ACCESS_AGE";

        private QueryMetricsColumns() {}

    }
}
