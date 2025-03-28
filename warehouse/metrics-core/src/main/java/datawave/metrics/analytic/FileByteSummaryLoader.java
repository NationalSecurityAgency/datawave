package datawave.metrics.analytic;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
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
 *
 */
public class FileByteSummaryLoader extends Configured implements Tool {
    private static final Logger log = Logger.getLogger(FileByteSummaryLoader.class);
    private static String defaultVisibility = "PUBLIC";

    private static class FileByteMetricsMapper extends Mapper<Key,Value,Key,Value> {
        Text holder = new Text();

        /*
         * TODO: Determine whether this entire file should be removed or refactored, as it is currently not in use and its applicability toward general ingest
         * usage patterns is somewhat questionable
         */

        private Pattern radixRegex = Pattern.compile("TODO: <PATTERN-PLACEHOLDER>");

        @Override
        protected void map(Key key, Value value, Context context) throws IOException, InterruptedException {
            key.getRow(holder);
            String name = holder.toString().substring(12);
            long loadTime = key.getTimestamp();
            String outRow = DateHelper.format(new Date(loadTime));
            Matcher m = radixRegex.matcher(name);
            key.getColumnQualifier(holder);
            if (m.matches()) {
                context.write(makeKey(outRow, "GROOMER_RLABEL_FILES", m.group(1), defaultVisibility), makeValue("1"));
                context.write(makeKey(outRow, "GROOMER_RLABEL_BYTES", m.group(1), defaultVisibility), makeValue(holder.toString()));
            }
        }

        private Key makeKey(String row, String cf, String cq, String cv) {
            return new Key(row, cf, cq, cv);
        }

        private Value makeValue(String value) {
            return new Value(value.getBytes());
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = JobSetupUtil.configure(args, getConf(), log);

        JobSetupUtil.printConfig(getConf(), log);

        Job job = Job.getInstance(conf);
        Configuration jconf = job.getConfiguration();
        job.setJarByClass(this.getClass());
        job.setJobName("FileByteMetricsSummaries");

        try {
            Connections.initTables(conf);
        } catch (AccumuloException | AccumuloSecurityException e) {
            throw new IOException(e);
        }

        String inputTable = jconf.get(MetricsConfig.RAW_FILE_INDEX_TABLE, MetricsConfig.DEFAULT_RAW_FILE_INDEX_TABLE);
        String outputTable = jconf.get(MetricsConfig.METRICS_SUMMARY_TABLE, MetricsConfig.DEFAULT_METRICS_SUMMARY_TABLE);
        String userName = jconf.get(MetricsConfig.USER);
        String password = jconf.get(MetricsConfig.PASS);
        String instance = jconf.get(MetricsConfig.INSTANCE);
        String zookeepers = jconf.get(MetricsConfig.ZOOKEEPERS, "localhost");
        Range dayRange = JobSetupUtil.computeTimeRange(jconf, log);
        long delta = Long.parseLong(dayRange.getEndKey().getRow().toString()) - Long.parseLong(dayRange.getStartKey().getRow().toString());
        int numDays = (int) Math.max(1, delta / TimeUnit.DAYS.toMillis(1));

        defaultVisibility = jconf.get(MetricsConfig.DEFAULT_VISIBILITY, defaultVisibility);

        dayRange = JobSetupUtil.formatReverseSlashedTimeRange(dayRange, log);// convert millisecond epoc timestamp to /YYYY/MM/DD

        job.setMapperClass(FileByteMetricsMapper.class);
        job.setMapOutputKeyClass(Key.class);
        job.setMapOutputValueClass(Value.class);
        job.setInputFormatClass(AccumuloInputFormat.class);

        // @formatter:off
        Properties clientProperties = Accumulo.newClientProperties()
                        .to(instance.trim(), zookeepers.trim())
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

    public static void main(String[] args) {
        try {
            ToolRunner.run(new FileByteSummaryLoader(), args);
        } catch (Exception e) {
            e.printStackTrace(); // Called from main()
        }
    }

}
