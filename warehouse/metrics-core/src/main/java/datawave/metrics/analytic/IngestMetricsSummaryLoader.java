package datawave.metrics.analytic;

import static datawave.metrics.analytic.MetricsDailySummaryReducer.WeightedPair;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.hadoop.mapreduce.AccumuloInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.google.common.io.ByteStreams;

import datawave.ingest.metric.IngestInput;
import datawave.ingest.metric.IngestOutput;
import datawave.ingest.metric.IngestProcess;
import datawave.metrics.config.MetricsConfig;
import datawave.metrics.mapreduce.util.JobSetupUtil;
import datawave.metrics.util.Connections;
import datawave.util.time.DateHelper;

/**
 * This MapReduce job computes a by-day summary of ingest job activity. We look at each file that was marked as loaded during the specified range, and output
 * various stats for each ingest job (event count by job type, event count by datatype, etc) that loaded a file during the day. The stats are aggregated
 * together for a given day across all jobs. This job is designed to work on day boundaries, so that the job does all aggregation and if run multiple times will
 * produce the same output. This then allows the job to be run multiple times during a day so that we can see metrics for a day before the day is over. Each
 * time the job is re-run, we will see the up-to-minute stats for whatever metrics have been collected.
 */
public class IngestMetricsSummaryLoader extends Configured implements Tool {

    private static final Logger log = Logger.getLogger(IngestMetricsSummaryLoader.class);

    /**
     * Convert file latencies into daily metric values.
     */
    private static class IngestMetricsMapper extends Mapper<Key,Value,Key,Value> {

        /*
         * TODO: Determine whether this entire file should be removed or refactored, as it is currently not in use and its applicability toward general ingest
         * usage patterns is somewhat questionable
         */

        private Pattern radixRegex = Pattern.compile("TODO: <PATTERN-PLACEHOLDER>");
        private Set<String> processedJobs = new HashSet<>();
        private Scanner ingestScanner;
        private Text holder = new Text();
        private FileLatency fileLatency = new FileLatency();
        private boolean useHourlyPrecision = false;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            Configuration conf = context.getConfiguration();
            String user = conf.get(MetricsConfig.USER);
            String password = conf.get(MetricsConfig.PASS);
            String instance = conf.get(MetricsConfig.INSTANCE);
            String zookeepers = conf.get(MetricsConfig.ZOOKEEPERS);

            useHourlyPrecision = HourlyPrecisionHelper.checkForHourlyPrecisionOption(context.getConfiguration(), log);

            try (AccumuloClient client = Accumulo.newClient().to(instance, zookeepers).as(user, password).build()) {
                ingestScanner = client.createScanner(conf.get(MetricsConfig.INGEST_TABLE, MetricsConfig.DEFAULT_INGEST_TABLE), Authorizations.EMPTY);
            } catch (TableNotFoundException e) {
                throw new IOException(e);
            }
        }

        @Override
        protected void map(Key key, Value value, Context context) throws IOException, InterruptedException {
            key.getRow(holder);
            long loadTime = Long.parseLong(holder.toString());

            Date date = new Date(loadTime);
            String outRow = useHourlyPrecision ? DateHelper.formatToHour(date) : DateHelper.format(date);

            fileLatency.readFields(ByteStreams.newDataInput(value.get()));
            String jobName = findJobName(fileLatency);

            String columnQualifier = key.getColumnQualifier().toString().toUpperCase();

            String ingestLabel = extractIngestLabelOverride(columnQualifier);
            if (null == ingestLabel) {
                ingestLabel = columnQualifier;
            } else {
                ingestLabel = ingestLabel.toUpperCase();
            }

            writeTimeDurationForStatsAnalysis(context, outRow, fileLatency.getRawFileTransformDuration(), "RAW_FILE_" + ingestLabel + "_ASRAW_LATENCY_MS");
            writeTimeDurationForStatsAnalysis(context, outRow, fileLatency.getDelayRawFileTransformToIngest(),
                            "RAW_FILE_" + ingestLabel + "_DELAY_ASRAW_INGEST_MS");
            writeTimeDurationForStatsAnalysis(context, outRow, fileLatency.getIngestJobDuration(), "RAW_FILE_" + ingestLabel + "_INGEST_JOB_LATENCY_MS");
            if (fileLatency.hasLoaderPhase()) {
                writeTimeDurationForStatsAnalysis(context, outRow, fileLatency.getDelayIngestToLoader(), "RAW_FILE_" + ingestLabel + "_DELAY_INGEST_LOADER_MS");
                writeTimeDurationForStatsAnalysis(context, outRow, fileLatency.getLoaderDuration(), "RAW_FILE_" + ingestLabel + "_LOADER_LATENCY_MS");
            }
            writeTimeDurationForStatsAnalysis(context, outRow, fileLatency.getTotalLatency(), "RAW_FILE_" + ingestLabel + "_INGEST_LATENCY_MS");

            key.getColumnFamily(holder);
            Matcher m = radixRegex.matcher(holder.toString());
            if (m.matches()) {
                context.write(makeKey(outRow, "R_LABEL", m.group(1), "PUBLIC"), makeValue(fileLatency.getEventCount()));
            }

            // If we've already processed this job, then there's nothing else to do.
            // We can have multiple records per jobName, but they will all have the
            // same completion time, therefore the same row, and will therefore sent
            // to the same mapper, so having this cache in memory is ok to determine
            // whether or not we've already processed a job.
            if (processedJobs.contains(jobName))
                return;

            Counters ingestJobCounters = getCounters(jobName);
            writeCounterGroup(context, ingestJobCounters.getGroup("CB Insert"), outRow, "ROWS_LOADED_BY_TABLE");
            writeCounterGroup(context, ingestJobCounters.getGroup(IngestInput.class.getName()), outRow, "INGEST_PROBLEMS");
            writeCounterGroup(context, ingestJobCounters.getGroup(IngestOutput.class.getName()), outRow, "INGEST_OUTPUT");
            long eventCount = writeCounterGroup(context, ingestJobCounters.getGroup("EVENTS_PROCESSED"), outRow, "EVENTS_LOADED"); // events by datatype

            boolean isLive = ingestJobCounters.findCounter(IngestProcess.LIVE_INGEST).getValue() > 0;
            context.write(makeKey(outRow, "EVENTS_LOADED", "TOTAL"), makeValue(eventCount));

            String label = checkForIngestLabelOverride(ingestJobCounters);
            if (null == label) {
                label = isLive ? "LIVE" : "BULK";
            } else {
                label = label.toUpperCase();
            }

            context.write(makeKey(outRow, "EVENTS_LOADED", label), makeValue(eventCount));
        }

        private void writeTimeDurationForStatsAnalysis(Context context, String outRow, long totalLatency, String totalLatencyCF)
                        throws IOException, InterruptedException {
            WeightedPair valuePair = new WeightedPair(totalLatency, fileLatency.getEventCount());
            context.write(makeKey(outRow, totalLatencyCF, MetricsDailySummaryReducer.PERCENTILE_STATS_METRIC_VALUE), valuePair.toValue());
        }

        private String findJobName(FileLatency fileLatency) {
            for (Phase phase : fileLatency.getPhases()) {
                if (phase.name().startsWith("job_"))
                    return phase.name();
            }
            return "";
        }

        private long writeCounterGroup(Context context, CounterGroup group, String row, String cf) throws IOException, InterruptedException {
            long counterTotal = 0;
            for (Counter c : group) {
                String counterName = c.getName();
                String count = Long.toString(c.getValue());
                context.write(makeKey(row, cf, counterName), makeValue(count));
                counterTotal += c.getValue();
            }
            return counterTotal;
        }

        private Counters getCounters(String jobId) {
            Counters counters = new Counters();

            Range r = new Range("jobId\0" + jobId);
            ingestScanner.setRange(r);
            Map.Entry<Key,Value> entry = ingestScanner.iterator().next();
            try {
                counters.readFields(ByteStreams.newDataInput(entry.getValue().get()));
            } catch (IOException e) {
                System.err.println("Error parsing counters for job " + jobId);
                e.printStackTrace(System.err); // Called from main
                // ignore for now -- bad counters so we'll just return partial/empty ones
            }
            processedJobs.add(jobId);

            if (!processedJobs.contains(jobId)) {
                System.err.println("Couldn't find ingest counters for job " + jobId);
                processedJobs.add(jobId);
            }

            return counters;
        }

        private Key makeKey(String row, String cf, String cq) {
            return new Key(row, cf, cq);
        }

        private Key makeKey(String row, String cf, String cq, String cv) {
            return new Key(row, cf, cq, cv);
        }

        private Value makeValue(String value) {
            return new Value(value.getBytes());
        }

        private Value makeValue(long value) {
            return makeValue(Long.toString(value));
        }
    }

    private static String checkForIngestLabelOverride(Counters ingestJobCounters) {
        CounterGroup jobQueueName = ingestJobCounters.getGroup(IngestProcess.METRICS_LABEL_OVERRIDE.name());
        if (jobQueueName.size() > 0) {
            Counter myCounter = jobQueueName.iterator().next();
            return myCounter.getName();
        }
        return null;
    }

    // if the metrics label is overridden, it will be something like this: fifteen\u0000live or proto\u0000bulk
    public static String extractIngestLabelOverride(String fullLabel) {
        if (fullLabel.contains("\u0000")) {
            return fullLabel.split("\u0000")[0];
        }
        return null;
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
            job.setJobName("IngestMetricsSummaries (hourly)");
        } else {
            job.setJobName("IngestMetricsSummaries");
        }

        try {
            Connections.initTables(conf);
        } catch (AccumuloException | AccumuloSecurityException e) {
            throw new IOException(e);
        }

        String inputTable = jconf.get(MetricsConfig.FILE_GRAPH_TABLE, MetricsConfig.DEFAULT_FILE_GRAPH_TABLE);
        String outputTable = HourlyPrecisionHelper.getOutputTable(jconf, useHourlyPrecision);
        String userName = jconf.get(MetricsConfig.USER);
        String password = jconf.get(MetricsConfig.PASS);
        String instance = jconf.get(MetricsConfig.INSTANCE);
        String zookeepers = jconf.get(MetricsConfig.ZOOKEEPERS, "localhost");
        Range dayRange = JobSetupUtil.computeTimeRange(jconf, log);
        long delta = Long.parseLong(dayRange.getEndKey().getRow().toString()) - Long.parseLong(dayRange.getStartKey().getRow().toString());
        int numDays = (int) Math.max(1, delta / TimeUnit.DAYS.toMillis(1));

        job.setMapperClass(IngestMetricsMapper.class);
        job.setMapOutputKeyClass(Key.class);
        job.setMapOutputValueClass(Value.class);
        job.setInputFormatClass(AccumuloInputFormat.class);

        // @formatter:off
        Properties clientProperties = Accumulo.newClientProperties()
                        .to(instance, zookeepers)
                        .as(userName, password)
                        .build();

        AccumuloInputFormat.configure()
                        .clientProperties(clientProperties)
                        .table(inputTable)
                        .auths(Authorizations.EMPTY)
                        .ranges(Collections.singletonList(dayRange))
                        .store(job);
        // @formatter:on
        // Ensure all data for a day goes to the same reducer so that we aggregate it correctly before sending to Accumulo
        RowPartitioner.configureJob(job);

        // Configure the reducer and output format to write out our metrics
        MetricsDailySummaryReducer.configureJob(job, numDays, jconf.get(MetricsConfig.INSTANCE), jconf.get(MetricsConfig.ZOOKEEPERS), userName, password,
                        outputTable);

        job.submit();
        JobSetupUtil.changeJobPriority(job, log);

        job.waitForCompletion(true);

        return 0;
    }

    /**
     * Expects to receive args in the order of [config opts] [dates] ... where [dates] are the last two
     *
     * @param args
     *            string arguments
     */
    public static void main(String[] args) {
        try {
            ToolRunner.run(new IngestMetricsSummaryLoader(), args);
        } catch (Exception e) {
            e.printStackTrace(); // Called from main()
        }
    }
}
