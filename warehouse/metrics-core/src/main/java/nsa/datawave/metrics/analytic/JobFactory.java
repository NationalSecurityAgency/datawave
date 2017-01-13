package nsa.datawave.metrics.analytic;

import nsa.datawave.metrics.config.MetricsConfig;
import nsa.datawave.metrics.util.Connections;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A simple wrapper that creates jobs to analyze metrics from any metrics process flow, such as live or bulk.
 * 
 * <b>Note</b> should deprecate or remove this class, as now I only create one type of job.
 * 
 */
public class JobFactory {
    
    private static final Logger log = Logger.getLogger(JobFactory.class);
    
    public Job createJob(Configuration c) throws IOException, AccumuloSecurityException {
        Job job = Job.getInstance(c);
        Configuration jconf = job.getConfiguration();
        job.setJarByClass(this.getClass());
        job.setJobName("MetricsCorrelator");
        
        try {
            Connections.initTables(c);
        } catch (AccumuloException | AccumuloSecurityException e) {
            throw new IOException(e);
        }
        
        ClientConfiguration zkConfig = ClientConfiguration.loadDefault().withInstance(jconf.get(MetricsConfig.INSTANCE))
                        .withZkHosts(jconf.get(MetricsConfig.ZOOKEEPERS, "localhost"));
        job.setMapperClass(MetricsCorrelatorMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongArrayWritable.class);
        job.setInputFormatClass(AccumuloInputFormat.class);
        AccumuloInputFormat.setConnectorInfo(job, jconf.get(MetricsConfig.USER), new PasswordToken(jconf.get(MetricsConfig.PASS)));
        AccumuloInputFormat.setInputTableName(job, jconf.get(MetricsConfig.INGEST_TABLE, MetricsConfig.DEFAULT_INGEST_TABLE));
        AccumuloInputFormat.setScanAuthorizations(job, Authorizations.EMPTY);
        AccumuloInputFormat.setZooKeeperInstance(job, zkConfig);
        
        AccumuloInputFormat.setAutoAdjustRanges(job, false);
        String start = jconf.get(MetricsConfig.START);
        String end = jconf.get(MetricsConfig.END);
        
        long from, until;
        if (start == null && end == null) {
            until = System.currentTimeMillis();
            from = until - TimeUnit.HOURS.toMillis(4);
            if (log.isDebugEnabled()) {
                log.info("Start and end both null; defaulting to last four hours.");
                log.info("Start: " + new Date(from) + ", End: " + new Date(until));
            }
        } else if (start == null) { // end != null given the previous if statement
            until = DateConverter.convert(end).getTime();
            from = 0;
            if (log.isDebugEnabled()) {
                log.info("Only end date specified; starting from the epoch and processing until end date.");
                log.info("Start: " + new Date(from) + ", End: " + new Date(until));
            }
        } else if (end == null) {
            until = System.currentTimeMillis();
            from = DateConverter.convert(start).getTime();
            if (log.isDebugEnabled()) {
                log.info("Only start date specified; ending at current time " + new Date(System.currentTimeMillis()));
                log.info("Start: " + new Date(from) + ", End: " + new Date(until));
            }
        } else {
            List<Date> dates = DateConverter.convert(start, end);
            from = dates.get(0).getTime();
            until = dates.get(1).getTime();
            if (log.isDebugEnabled()) {
                log.info("Both start and end dates specified!");
                log.info("Start: " + new Date(from) + ", End: " + new Date(until));
            }
        }
        
        int splits = inferSplits(until - from);
        AccumuloInputFormat.setRanges(job, createRanges(from, until, splits));
        
        job.setReducerClass(MetricsCorrelatorReducer.class);
        job.setNumReduceTasks(Math.max(splits / 6, 1));
        
        job.setOutputFormatClass(AccumuloOutputFormat.class);
        AccumuloOutputFormat.setZooKeeperInstance(job,
                        ClientConfiguration.loadDefault().withInstance(jconf.get(MetricsConfig.INSTANCE)).withZkHosts(jconf.get(MetricsConfig.ZOOKEEPERS)));
        AccumuloOutputFormat.setConnectorInfo(job, jconf.get(MetricsConfig.USER), new PasswordToken(jconf.get(MetricsConfig.PASS)));
        AccumuloOutputFormat.setCreateTables(job, true);
        AccumuloOutputFormat.setDefaultTableName(job, jconf.get(MetricsConfig.METRICS_TABLE, MetricsConfig.DEFAULT_METRICS_TABLE));
        
        return job;
    }
    
    /**
     * Splits the range into roughly <code>split</code> number of ranges.
     * 
     * @param from
     *            - beginning timestamp
     * @param until
     *            - ending timestamp
     * @param splits
     *            - splits hint
     */
    public ArrayList<Range> createRanges(long from, long until, int splits) {
        log.info("Creating ranges for " + new Date(from) + " (" + from + ") to " + new Date(until) + " (" + until + ").");
        log.info("Normalized from time to be " + new Date(from) + " (" + from + ").");
        ArrayList<Range> ranges = new ArrayList<>(splits + 1);
        long interval = (until - from) / splits;
        NumberFormat nf = NumberFormat.getIntegerInstance();
        nf.setMaximumIntegerDigits(13);
        nf.setMinimumIntegerDigits(13);
        nf.setGroupingUsed(false);
        if (interval > until - from) {
            ranges.add(new Range(nf.format(from), nf.format(until)));
        } else if (until >= from) {
            long step = from;
            String stepStr = nf.format(step);
            do {
                long nextStep = Math.min(step + interval, until);
                String nextStepStr = nf.format(nextStep);
                ranges.add(new Range(stepStr, nextStepStr));
                step = nextStep;
                stepStr = nextStepStr;
            } while (step < until);
        }
        return ranges;
    }
    
    /*
     * Will infer a split size based on the time range specified.
     */
    public static int inferSplits(long range) {
        return (int) Math.max(1l, range / TimeUnit.DAYS.toMillis(1));
    }
}
