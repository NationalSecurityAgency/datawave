package nsa.datawave.metrics.util;

import nsa.datawave.metrics.keys.IngestEntryKey;
import nsa.datawave.poller.metric.InputFile;
import nsa.datawave.poller.metric.OutputFile;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Class that uses the metrics table to print out latency information for each input file
 * 
 */
public class FileLatencySummary {
    
    public static final String NULL = "\0";
    
    private Options options;
    private Option instanceNameOpt, zookeeperOpt, usernameOpt, passwordOpt, pollerTableNameOpt, ingestTableNameOpt, loaderTableNameOpt;
    private Option rangeOpt;
    private String instanceName, zookeepers, username, password, pollerTableName, ingestTableName, loaderTableName;
    private Range ingestRange = new Range();
    
    public FileLatencySummary() {
        generateCommandLineOptions();
    }
    
    private void generateCommandLineOptions() {
        options = new Options();
        
        pollerTableNameOpt = new Option("pm", "pollerMetricsTable", true, "The poller metrics table name");
        pollerTableNameOpt.setRequired(true);
        options.addOption(pollerTableNameOpt);
        
        ingestTableNameOpt = new Option("im", "ingestMetricsTable", true, "The ingest metrics table name");
        ingestTableNameOpt.setRequired(true);
        options.addOption(ingestTableNameOpt);
        
        loaderTableNameOpt = new Option("lm", "loaderMetricsTable", true, "The loader metrics table name");
        loaderTableNameOpt.setRequired(true);
        options.addOption(loaderTableNameOpt);
        
        instanceNameOpt = new Option("i", "instance", true, "Accumulo instance name");
        instanceNameOpt.setArgName("name");
        instanceNameOpt.setRequired(true);
        options.addOption(instanceNameOpt);
        
        zookeeperOpt = new Option("zk", "zookeeper", true, "Comma-separated list of ZooKeeper servers");
        zookeeperOpt.setArgName("server[,server]");
        zookeeperOpt.setRequired(true);
        options.addOption(zookeeperOpt);
        
        usernameOpt = new Option("u", "username", true, "Accumulo username");
        usernameOpt.setArgName("name");
        usernameOpt.setRequired(true);
        options.addOption(usernameOpt);
        
        passwordOpt = new Option("p", "password", true, "Accumulo password");
        passwordOpt.setArgName("passwd");
        passwordOpt.setRequired(true);
        options.addOption(passwordOpt);
        
        rangeOpt = new Option("r", "range", true, "Time range, specified as startInMsSinceEpoch,endInMsSinceEpoch");
        rangeOpt.setArgName("range");
        rangeOpt.setRequired(false);
        options.addOption(rangeOpt);
    }
    
    private void parseConfig(String[] args) throws ParseException {
        CommandLine cl = new BasicParser().parse(options, args);
        instanceName = cl.getOptionValue(instanceNameOpt.getOpt());
        zookeepers = cl.getOptionValue(zookeeperOpt.getOpt());
        username = cl.getOptionValue(usernameOpt.getOpt());
        password = cl.getOptionValue(passwordOpt.getOpt());
        pollerTableName = cl.getOptionValue(pollerTableNameOpt.getOpt());
        ingestTableName = cl.getOptionValue(ingestTableNameOpt.getOpt());
        loaderTableName = cl.getOptionValue(loaderTableNameOpt.getOpt());
        
        if (cl.hasOption(rangeOpt.getOpt())) {
            String[] range = cl.getOptionValue(rangeOpt.getOpt()).split("\\s*,\\s*");
            ingestRange = new Range(range[0], range[1]);
        }
    }
    
    private synchronized void printCSV(String type, String jobType, String rawFileName, String sequenceFileName, String jobId, long fileTimestamp,
                    long pollerStartTime, long pollerEndTime, long jobStartTime, long jobEndTime, long loaderStartTime, long loaderEndTime,
                    long pollerErrorCount, long pollerInputFileEventCount, long pollerOutputFileEventCount, long jobEventCount) {
        System.out.println(StringUtils.join(
                        new String[] {type, jobType, rawFileName, sequenceFileName, jobId, Long.toString(fileTimestamp), Long.toString(pollerStartTime),
                                Long.toString(pollerEndTime), Long.toString(jobStartTime), Long.toString(jobEndTime), Long.toString(loaderStartTime),
                                Long.toString(loaderEndTime), Long.toString(pollerErrorCount), Long.toString(pollerInputFileEventCount),
                                Long.toString(pollerOutputFileEventCount), Long.toString(jobEventCount)}, ','));
    }
    
    public void run() throws Exception {
        
        // Get current user authorizations
        ZooKeeperInstance instance = new ZooKeeperInstance(ClientConfiguration.loadDefault().withInstance(instanceName).withZkHosts(zookeepers));
        Connector connector = instance.getConnector(username, new PasswordToken(password));
        Authorizations auths = connector.securityOperations().getUserAuthorizations(connector.whoami());
        
        BatchScanner ingestScanner = connector.createBatchScanner(this.ingestTableName, auths, 10);
        
        // Scanners per thread -- reuse them so we don't keep recreating connections.
        Map<Thread,BatchScanner> pollerScanners = Collections.synchronizedMap(new HashMap<Thread,BatchScanner>());
        Map<Thread,Scanner> loaderScanners = Collections.synchronizedMap(new HashMap<Thread,Scanner>());
        
        ThreadFactory threadFactory = new FileLatencyThreadFactory(pollerScanners, loaderScanners, connector, auths, pollerTableName, loaderTableName);
        ExecutorService svc = Executors.newFixedThreadPool(40, threadFactory);
        
        // Scan over ingest metrics table and create a task
        // to pull and print metrics for each ingest job.
        int taskCount = 0;
        ingestScanner.setRanges(Collections.singletonList(ingestRange));
        for (Entry<Key,Value> ingestMetrics : ingestScanner) {
            String jobEndTimeStr = ingestMetrics.getKey().getRow().toString();
            // We are looking for rows with numbers that represent the job end time.
            // There are other rows that start with "job" and "datatype".
            // We will check to see if the row is a number, if not then skip.
            if (!NumberUtils.isNumber(jobEndTimeStr))
                continue;
            
            svc.submit(new FileLatencyWorker(pollerScanners, loaderScanners, ingestMetrics));
            ++taskCount;
        }
        
        System.err.println("Collecting stats on " + taskCount + " ingest jobs...");
        svc.shutdown();
        svc.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
        
        System.err.println("Cleaning up...");
        for (Scanner scanner : loaderScanners.values()) {
            scanner.clearScanIterators();
        }
        for (BatchScanner bs : pollerScanners.values()) {
            bs.clearScanIterators();
            bs.close();
        }
        ingestScanner.clearScanIterators();
        ingestScanner.close();
        
        System.err.println("Done.");
    }
    
    private class FileLatencyWorker implements Runnable {
        private Map<Thread,BatchScanner> pollerScanners;
        private Map<Thread,Scanner> loaderScanners;
        private Entry<Key,Value> ingestEntry;
        
        public FileLatencyWorker(Map<Thread,BatchScanner> pollerScanners, Map<Thread,Scanner> loaderScanners, Entry<Key,Value> ingestEntry) {
            this.pollerScanners = pollerScanners;
            this.loaderScanners = loaderScanners;
            this.ingestEntry = ingestEntry;
        }
        
        @Override
        public void run() {
            BatchScanner pollerScanner = pollerScanners.get(Thread.currentThread());
            Scanner loaderScanner = loaderScanners.get(Thread.currentThread());
            
            String jobEndTimeStr = ingestEntry.getKey().getRow().toString();
            // We are looking for rows with numbers that represent the job end time.
            // There are other rows that start with "job" and "datatype".
            // We will check to see if the row is a number, if not then skip.
            if (!NumberUtils.isNumber(jobEndTimeStr))
                return;
            
            IngestEntryKey ingestKey = null;
            String jobId = "unknown";
            String jobType = "UNK";
            boolean isLiveJob = false;
            long jobStartTime = -1;
            long jobEndTime = -1;
            long jobEventCount = -1;
            String outputDir = null;
            try {
                ingestKey = new IngestEntryKey(ingestEntry.getKey());
                outputDir = ingestKey.getOutputDirectory();
                jobId = ingestKey.getJobId();
                jobType = ingestKey.getType();
                isLiveJob = ingestKey.isLive();
                jobEndTime = ingestKey.getTimestamp();
                jobStartTime = jobEndTime - ingestKey.getDuration();
                jobEventCount = ingestKey.getCount();
            } catch (Exception e) {
                System.err.println("Unable to parse ingest metrics key: " + e.getMessage());
                System.err.println("Original key was: " + ingestEntry.getKey().toString());
            }
            
            // Get the value
            try {
                ArrayWritable inputFilesArray = new ArrayWritable(Text.class);
                inputFilesArray.readFields(new DataInputStream(new ByteArrayInputStream(ingestEntry.getValue().get())));
                String[] sequenceFiles = inputFilesArray.toStrings();
                
                TreeSet<String> inputFiles = new TreeSet<>(Arrays.asList(sequenceFiles));
                
                ArrayList<Range> ranges = new ArrayList<>();
                for (String sequenceFile : sequenceFiles) {
                    ranges.add(new Range(new Text(sequenceFile)));
                }
                pollerScanner.setRanges(ranges);
                for (Entry<Key,Value> pollerMetrics : pollerScanner) {
                    String sequenceFile = pollerMetrics.getKey().getRow().toString();
                    inputFiles.remove(sequenceFile);
                    Counters c = new Counters();
                    c.readFields(new DataInputStream(new ByteArrayInputStream(pollerMetrics.getValue().get())));
                    long pollStartTime = c.findCounter(InputFile.POLLER_START_TIME).getValue();
                    long pollEndTime = c.findCounter(InputFile.POLLER_END_TIME).getValue();
                    long pollerErrorCount = c.findCounter(InputFile.ERRORS).getValue();
                    long pollerInputEventCount = c.findCounter(InputFile.RECORDS).getValue();
                    long pollerOutputEventCount = c.findCounter(OutputFile.RECORDS).getValue();
                    CounterGroup inputFileGroup = c.getGroup(InputFile.class.getSimpleName());
                    Counter fileCtr = inputFileGroup.iterator().next();
                    String rawFileName = fileCtr.getName();
                    long fileModificationTime = fileCtr.getValue();
                    
                    if (isLiveJob) {
                        printCSV("LIVE", jobType, rawFileName, sequenceFile, jobId, fileModificationTime, pollStartTime, pollEndTime, jobStartTime, jobEndTime,
                                        0L, 0L, pollerErrorCount, pollerInputEventCount, pollerOutputEventCount, jobEventCount);
                    } else {
                        // Need to get the loader metrics
                        long bulkLoadStartTime = -1L;
                        long bulkLoadEndTime = -1L;
                        loaderScanner.setRange(new Range(new Text(outputDir)));
                        if (loaderScanner.iterator().hasNext()) {
                            Entry<Key,Value> bulkLoadMetrics = loaderScanner.iterator().next();
                            try {
                                long bulkLoadDuration = Long.parseLong(bulkLoadMetrics.getKey().getColumnFamily().toString());
                                bulkLoadEndTime = Long.parseLong(bulkLoadMetrics.getKey().getColumnQualifier().toString());
                                bulkLoadStartTime = bulkLoadEndTime - bulkLoadDuration;
                            } catch (NumberFormatException e) {
                                System.err.println("Error processing bulk load times: " + e);
                            }
                        }
                        if (bulkLoadStartTime == -1) {
                            System.err.println("No bulk load start time found for job: " + jobId);
                        }
                        if (bulkLoadEndTime == -1) {
                            System.err.println("No bulk load end time found for job: " + jobId);
                        }
                        printCSV("BULK", jobType, rawFileName, sequenceFile, jobId, fileModificationTime, pollStartTime, pollEndTime, jobStartTime, jobEndTime,
                                        bulkLoadStartTime, bulkLoadEndTime, pollerErrorCount, pollerInputEventCount, pollerOutputEventCount, jobEventCount);
                    }
                }
                
                if (!inputFiles.isEmpty()) {
                    StringBuilder err = new StringBuilder();
                    err.append("Did not find metrics for the following files:\n");
                    for (String file : inputFiles) {
                        err.append('\t').append(file).append('\n');
                    }
                    System.err.print(err);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
    
    private static class FileLatencyThreadFactory implements ThreadFactory {
        private static final AtomicInteger poolNumber = new AtomicInteger(1);
        private final ThreadGroup group;
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;
        
        private Map<Thread,BatchScanner> pollerScanners;
        private Map<Thread,Scanner> loaderScanners;
        
        private Connector connector;
        private Authorizations auths;
        private String pollerTableName;
        private String loaderTableName;
        
        public FileLatencyThreadFactory(Map<Thread,BatchScanner> pollerScanners, Map<Thread,Scanner> loaderScanners, Connector connector, Authorizations auths,
                        String pollerTableName, String loaderTableName) {
            SecurityManager s = System.getSecurityManager();
            group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
            namePrefix = "pool-" + poolNumber.getAndIncrement() + "-thread-";
            
            this.pollerScanners = pollerScanners;
            this.loaderScanners = loaderScanners;
            this.connector = connector;
            this.auths = auths;
            this.pollerTableName = pollerTableName;
            this.loaderTableName = loaderTableName;
        }
        
        @Override
        public Thread newThread(Runnable r) {
            
            Thread t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0);
            if (t.isDaemon())
                t.setDaemon(false);
            if (t.getPriority() != Thread.NORM_PRIORITY)
                t.setPriority(Thread.NORM_PRIORITY);
            
            try {
                BatchScanner pollerScanner = connector.createBatchScanner(pollerTableName, auths, 4);
                pollerScanners.put(t, pollerScanner);
                Scanner loaderScanner = connector.createScanner(loaderTableName, auths);
                loaderScanners.put(t, loaderScanner);
            } catch (TableNotFoundException e) {
                throw new RuntimeException("Unable to create scanner", e);
            }
            
            return t;
        }
        
    }
    
    public static void main(String[] args) throws Exception {
        FileLatencySummary summary = new FileLatencySummary();
        // Parse command line args
        summary.parseConfig(args);
        System.out.println("TYPE,JOB_TYPE,RAW_FILE_NAME,SEQUENCE_FILE_NAME,JOB_ID,FILE_TIMESTAMP,POLLER_START_TIME,POLLER_END_TIME,JOB_START_TIME,JOB_END_TIME,BULK_IMPORT_START_TIME,BULK_IMPORT_END_TIME,POLLER_ERROR_COUNT,POLLER_INPUT_FILE_EVENT_COUNT,POLLER_OUTPUT_FILE_EVENT_COUNT,JOB_EVENT_COUNT");
        summary.run();
    }
    
}
