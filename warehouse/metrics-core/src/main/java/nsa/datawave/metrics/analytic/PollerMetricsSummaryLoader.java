package nsa.datawave.metrics.analytic;

import com.google.common.io.ByteStreams;

import nsa.datawave.core.iterators.ColumnQualifierRangeIterator;
import nsa.datawave.ingest.poller.manager.OutputData;
import nsa.datawave.iterators.IteratorSettingHelper;
import nsa.datawave.metrics.config.MetricsConfig;
import nsa.datawave.metrics.mapreduce.util.JobSetupUtil;
import nsa.datawave.metrics.util.Connections;
import nsa.datawave.metrics.util.WritableUtil;
import nsa.datawave.poller.metric.InputFile;
import nsa.datawave.util.time.DateHelper;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * This MapReduce job produces by-day metrics for files that were received by the poller process. Note that we only output stats for files that were processed
 * during the day range, so if processing took a long time or crossed a day boundary, then we will include files that weren't actually received on the day in
 * question.
 */
public class PollerMetricsSummaryLoader extends Configured implements Tool {
    
    private static final Logger log = Logger.getLogger(PollerMetricsSummaryLoader.class);
    
    /**
     * Convert file latencies into daily metric values.
     */
    private static class PollerSummaryMapper extends Mapper<Key,Value,Key,Value> {
        private Text holder = new Text();
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }
        
        @Override
        protected void map(Key key, Value value, Context context) throws IOException, InterruptedException {
            
            key.getColumnQualifier(holder);
            long endTime = WritableUtil.parseLong(holder);
            String outRow = DateHelper.format(new Date(endTime));
            
            Counters counters = new Counters();
            counters.readFields(ByteStreams.newDataInput(value.get()));
            long recordCount = counters.findCounter(InputFile.RECORDS).getValue();
            long errorCount = counters.findCounter(InputFile.ERRORS).getValue();
            long rawFileSize = counters.findCounter(OutputData.UNCOMPRESSED_EVENT_BYTES).getValue();
            
            context.write(makeKey(outRow, "RAW_FILE_PROCESSED", "TOTAL"), makeValue("1"));
            context.write(makeKey(outRow, "RAW_FILE_EVENTS_RECEIVED", "TOTAL"), makeValue(recordCount));
            context.write(makeKey(outRow, "RAW_FILE_EVENTS_RECEIVED", MetricsDailySummaryReducer.STATS_METRIC_VALUE), makeValue(recordCount));
            context.write(makeKey(outRow, "RAW_FILE_EVENT_ERRORS", "TOTAL"), makeValue(errorCount));
            context.write(makeKey(outRow, "RAW_FILE_SIZE", "TOTAL"), makeValue(rawFileSize));
            context.write(makeKey(outRow, "RAW_FILE_SIZE", MetricsDailySummaryReducer.STATS_METRIC_VALUE), makeValue(rawFileSize));
        }
        
        private Key makeKey(String row, String cf, String cq) {
            return new Key(row, cf, cq);
        }
        
        private Value makeValue(String value) {
            return new Value(value.getBytes());
        }
        
        private Value makeValue(long value) {
            return new Value(Long.toString(value).getBytes());
        }
    }
    
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = JobSetupUtil.configure(args, getConf(), log);
        
        JobSetupUtil.printConfig(getConf(), log);
        
        Job job = Job.getInstance(conf);
        Configuration jconf = job.getConfiguration();
        job.setJarByClass(this.getClass());
        job.setJobName("PollerSummaries");
        
        try {
            Connections.initTables(conf);
        } catch (AccumuloException | AccumuloSecurityException e) {
            throw new IOException(e);
        }
        
        String inputTable = jconf.get(MetricsConfig.POLLER_TABLE, MetricsConfig.DEFAULT_POLLER_TABLE);
        String outputTable = jconf.get(MetricsConfig.METRICS_SUMMARY_TABLE, MetricsConfig.DEFAULT_METRICS_SUMMARY_TABLE);
        String userName = jconf.get(MetricsConfig.USER);
        String password = jconf.get(MetricsConfig.PASS);
        String instance = jconf.get(MetricsConfig.INSTANCE);
        String zookeepers = jconf.get(MetricsConfig.ZOOKEEPERS, "localhost");
        Range dayRange = JobSetupUtil.computeTimeRange(jconf, log);
        long delta = Long.parseLong(dayRange.getEndKey().getRow().toString()) - Long.parseLong(dayRange.getStartKey().getRow().toString());
        int numDays = (int) Math.max(1, delta / TimeUnit.DAYS.toMillis(1));
        
        // Only look at polled files in the range we specify. Note that this is still expensive since the poller metrics
        // table isn't organized very well for this operation.
        IteratorSetting cqRangeIterator = new IteratorSetting(IteratorSettingHelper.BASE_ITERATOR_PRIORITY + 22, "cqRange", ColumnQualifierRangeIterator.class);
        cqRangeIterator.addOption(ColumnQualifierRangeIterator.RANGE_NAME, ColumnQualifierRangeIterator.encodeRange(dayRange));
        
        job.setMapperClass(PollerSummaryMapper.class);
        job.setMapOutputKeyClass(Key.class);
        job.setMapOutputValueClass(Value.class);
        job.setInputFormatClass(AccumuloInputFormat.class);
        AccumuloInputFormat.setConnectorInfo(job, userName, new PasswordToken(password));
        AccumuloInputFormat.setZooKeeperInstance(job, ClientConfiguration.loadDefault().withInstance(instance).withZkHosts(zookeepers));
        AccumuloInputFormat.setInputTableName(job, inputTable);
        AccumuloInputFormat.setScanAuthorizations(job, Authorizations.EMPTY);
        AccumuloInputFormat.addIterator(job, cqRangeIterator);
        
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
     */
    public static void main(String[] args) {
        try {
            ToolRunner.run(new PollerMetricsSummaryLoader(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
