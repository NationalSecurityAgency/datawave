package nsa.datawave.metrics.analytic;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import nsa.datawave.ingest.metric.IngestInput;
import nsa.datawave.ingest.metric.IngestOutput;
import nsa.datawave.ingest.metric.IngestProcess;
import nsa.datawave.util.StringUtils;
import nsa.datawave.metrics.config.MetricsConfig;
import nsa.datawave.metrics.mapreduce.IngestMetricsMapper;
import nsa.datawave.metrics.mapreduce.util.TypeNameConverter;
import nsa.datawave.metrics.util.WritableUtil;
import nsa.datawave.poller.metric.InputFile;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.javatuples.Pair;

import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;

/**
 * This correlator is used to string together timing information for Datawave ingest metrics.
 * 
 * If we are scanning live metrics, then this mapper will output keys of the format <Timestamp>:<Type> and values that are arrays of long writable's. If we are
 * dealing with live metrics, the output value will be of length MetricsDataFormat.LIVE_LENGTH. If we are dealing with bulk metrics, the output value will be of
 * length MetricsDataFormat.BULK_LENGTH.
 * 
 * The format of output values are:
 * 
 * <b>Live</b>: Count | Poller Duration | Ingest Delay | Ingest Duration
 * 
 * <b>Bulk</b>: Count | Poller Duration | Ingest Delay | Ingest Duration | Loader Delay | Loader Duration
 * 
 */
public class MetricsCorrelatorMapper extends Mapper<Key,Value,Text,LongArrayWritable> {
    
    private static final Logger log = Logger.getLogger(MetricsCorrelatorMapper.class);
    
    private static final Text[] EMPTY_TEXT_ARRAY = new Text[0];
    
    private enum SetupState {
        SCANNERS_OK, SCANNERS_BAD
    }
    
    private enum Metrics {
        INIT_TIME, CLEANUP_TIME
    }
    
    private BatchScanner ibscan;
    private BatchScanner pscan;
    // private Scanner pscan;
    private BatchScanner fmscan;
    private Scanner lscan;
    private Scanner iscan;
    private BatchWriter fileGraph;
    private SetupState state = SetupState.SCANNERS_BAD;
    private Map<String,TimeSummary> bulkLoaderCache = new TreeMap<>();
    private boolean ignorePollerMetrics = false;
    private boolean ignoreFlagMakerMetrics = false;
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        /***
         * if (!ignorePollerMetrics) { pscan.close(); }
         ***/
        
        if (null != pscan) {
            pscan.close();
        }
        
        if (null != fmscan) {
            fmscan.close();
        }
        
        if (null != ibscan) {
            ibscan.close();
        }
        
        try {
            fileGraph.close();
        } catch (MutationsRejectedException e) {
            throw new IOException(e);
        }
        context.getCounter(Metrics.CLEANUP_TIME).increment(System.currentTimeMillis());
    }
    
    @Override
    protected void map(Key key, Value value, Context context) throws IOException, InterruptedException {
        byte[] vasb = value.get();
        
        if (log.isDebugEnabled()) {
            log.debug("Working on tuple: " + key.getRow() + " | " + key.getColumnFamily() + " | " + key.getColumnQualifier());
        }
        
        if (state == SetupState.SCANNERS_BAD) {
            return;
        }
        
        try {
            createLatencySummary(key, value, context);
        } catch (MutationsRejectedException e) {
            throw new IOException(e);
        }
        
    }
    
    public void createLatencySummary(Key key, Value value, Context context) throws IOException, InterruptedException, MutationsRejectedException {
        long timestamp = WritableUtil.parseLong(key.getRow());
        
        String tuple = key.getColumnFamily().toString();
        
        String[] vals = StringUtils.split(tuple, (char) 0x0);
        String dataType = vals[0];
        String countS = vals[1];
        String durationS = vals[2];
        
        // check to see if we have loader info
        String loaderDir = null;
        String[] jobInfo = StringUtils.split(key.getColumnQualifier().toString(), (char) 0x0);
        String jobId = jobInfo[0];
        if (jobInfo.length > 1) { // we have bulk metricsloaderDir = jobInfo[1];
            loaderDir = jobInfo[1];
        }
        
        final long ingestDuration = Long.parseLong(durationS);
        final Phase ingest = new Phase(jobId, timestamp - ingestDuration, timestamp);
        
        final LinkedList<Phase> fileFlow = Lists.newLinkedList();
        fileFlow.add(ingest);
        
        // if we have loader metrics, output an array of length 6, otherwise we only
        // need length 4
        LongWritable[] values = new LongWritable[loaderDir == null ? MetricsDataFormat.LIVE_LENGTH : MetricsDataFormat.BULK_LENGTH];
        int pos = 0;
        
        values[pos++] = new LongWritable(Long.parseLong(countS));
        
        long avgPEndTime = 0;
        
        final LinkedList<Pair<Phase,Long>> pollerPhases = Lists.newLinkedList();
        final LinkedList<Pair<Phase,Long>> flagMakerPhases = Lists.newLinkedList();
        
        Text[] inFiles = parseFileNames(value.get());
        
        Counters[] jobCounters = getIngestJobStats(jobId);
        
        if (inFiles.length == 0) {
            inFiles = findInputFiles(jobCounters);
        }
        
        if (!ignoreFlagMakerMetrics) {
            long recordsIngested = totalIngestedRecords(jobCounters);
            Pair<TimeSummary,? extends Collection<Pair<Phase,Long>>> bTuple = getFlagMakerStats(inFiles, recordsIngested);
            
            TimeSummary flagMakerStats = null;
            
            if (bTuple == null)
                return;
            
            flagMakerPhases.addAll(bTuple.getValue1());
            flagMakerStats = bTuple.getValue0();
        }
        
        if (ignorePollerMetrics) {
            values[pos++] = new LongWritable(0);
        } else {
            // create and add the poller duration
            
            Pair<TimeSummary,? extends Collection<Pair<Phase,Long>>> aTuple = getPollerStats(inFiles);
            
            if (aTuple == null)
                return;
            
            TimeSummary pollerStats = null;
            
            pollerPhases.addAll(aTuple.getValue1());
            pollerStats = aTuple.getValue0();
            
            long avgPDur = 0;
            
            if (pollerStats == null)
                return;
            
            avgPDur = pollerStats.duration;
            avgPEndTime = pollerStats.end;
            
            if (avgPDur <= 0 || avgPEndTime <= 0)
                return;
            
            values[pos++] = new LongWritable(avgPDur);
        }
        
        values[pos++] = new LongWritable(timestamp - ingestDuration - avgPEndTime); // ingestion delay
        
        // add the ingest duration
        values[pos++] = new LongWritable(ingestDuration);
        
        // if we have a loaderDir, get the stats for it
        String ingestType = "live";
        if (loaderDir != null) {
            ingestType = "bulk";
            TimeSummary loaderData = getLoaderStats(loaderDir);
            // means that the metrics data for the corresponding loader job isn't
            // available, so we'll skip it
            if (loaderData == null) {
                return;
            } else {
                final long loaderDuration = loaderData.duration;
                final long loaderEndTime = loaderData.end;
                final long loaderDelay = loaderEndTime - loaderDuration - timestamp;
                
                // add in a loader phase
                fileFlow.add(new Phase(loaderDir, loaderEndTime - loaderDuration, loaderEndTime));
                
                values[pos++] = new LongWritable(loaderDelay);
                values[pos++] = new LongWritable(loaderDuration);
            }
        }
        
        String label = findOverrideForMetricsLabel(jobCounters);
        if (label != null) {
            ingestType = label + '\u0000' + ingestType; // e.g. fifteen\x00live
        }
        
        LongArrayWritable outVal = new LongArrayWritable();
        outVal.set(values);
        
        context.write(new Text(timestamp + ":" + dataType + ":" + ingestType), outVal);
        
        // We currently have no way of differentiating between flag-maker phases and poller phases in the table
        // While the additional data is good, for instances where the poller is used, they have knowledge
        // of when the file touches down, and using the flag-maker metrics will give systems NOT using the poller
        // a point at which the file hit the DATAWAVE system.
        if (!pollerPhases.isEmpty()) {
            writeInputFileIndex(pollerPhases, fileFlow, ingestType);
        } else if (!flagMakerPhases.isEmpty()) {
            writeInputFileIndex(flagMakerPhases, fileFlow, ingestType);
        }
    }
    
    private Value getJobData(String jobId) {
        Value value = null;
        List<Range> ranges = new ArrayList<Range>();
        Text range = new Text("jobId" + '\u0000' + jobId);
        ranges.add(new Range(range));
        
        if (ranges.isEmpty()) {
            return null;
        }
        
        ibscan.setRanges(ranges);
        for (Entry<Key,Value> entry : ibscan) {
            if (value == null) {
                value = entry.getValue();
            }
        }
        return value;
    }
    
    /**
     * Check for the existence of an overridden queue name/metrics label, which means it should be labeled differently than bulk or live
     * 
     * @param ingestJobCounters
     * @return label for metrics, expected use case is the queue name, or null if none was found
     */
    private String findOverrideForMetricsLabel(Counters[] ingestJobCounters) {
        for (Counters counters : ingestJobCounters) {
            CounterGroup jobQueueName = counters.getGroup(IngestProcess.METRICS_LABEL_OVERRIDE.name());
            
            if (jobQueueName.size() > 0) {
                Counter myCounter = jobQueueName.iterator().next();
                return myCounter.getName();
            }
        }
        return null;
    }
    
    private void writeInputFileIndex(Collection<Pair<Phase,Long>> pollerInputFiles, Deque<Phase> ingestPhases, String ingestType) throws IOException,
                    MutationsRejectedException {
        final String loadTime = Long.toString(ingestPhases.peekLast().end());
        final Mutation m = new Mutation(loadTime);
        for (Pair<Phase,Long> fileAndCount : pollerInputFiles) {
            Phase inputFile = fileAndCount.getValue0();
            ingestPhases.addFirst(inputFile);
            long eventCount = fileAndCount.getValue1();
            FileLatency value = new FileLatency(ingestPhases, eventCount, inputFile.name());
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            value.write(new DataOutputStream(baos));
            m.put(inputFile.name(), ingestType, new Value(baos.toByteArray()));
            ingestPhases.pollFirst();
        }
        if (m.size() > 0) {
            fileGraph.addMutation(m);
            fileGraph.flush();
        }
    }
    
    private class TimeSummary {
        long end, duration;
        
        TimeSummary(long end, long duration) {
            this.end = end;
            this.duration = duration;
        }
    }
    
    /**
     * Parses the names of files that were used an inputs to the ingest map reduce job.
     * 
     * @throws IOException
     */
    public Text[] parseFileNames(byte[] serializedList) throws IOException {
        ArrayWritable array = new ArrayWritable(Text.class);
        array.readFields(new DataInputStream(new ByteArrayInputStream(serializedList)));
        Writable[] inFiles = array.get();
        ArrayList<Text> tFiles = new ArrayList<>(inFiles.length);
        
        for (Writable w : inFiles) {
            tFiles.add((Text) w);
        }
        
        return tFiles.toArray(new Text[tFiles.size()]);
    }
    
    /**
     * Scans the poller table for the given input files in the file array.
     * 
     * @return a TimeSummary of the input files; null if no input files
     */
    @SuppressWarnings("deprecation")
    public Pair<TimeSummary,? extends Collection<Pair<Phase,Long>>> getPollerStats(Text[] inFiles) throws IOException {
        List<Range> ranges = new ArrayList<Range>();
        for (Text inFile : inFiles) {
            ranges.add(new Range(inFile));
        }
        
        if (ranges.isEmpty()) {
            return null;
        }
        
        LinkedList<Pair<Phase,Long>> inFlows = Lists.newLinkedList();
        pscan.setRanges(ranges);
        long longestDur = 0, latestEnd = 0;
        for (Entry<Key,Value> entry : pscan) {
            Key k = entry.getKey();
            final long endTime = WritableUtil.parseLong(k.getColumnQualifier());
            if (endTime > latestEnd) {
                latestEnd = endTime;
            }
            final long duration = WritableUtil.parseLong(k.getColumnFamily());
            if (duration > longestDur) {
                longestDur = duration;
            }
            if (log.isTraceEnabled()) {
                log.trace("Are the timestamps equal? " + Long.toString(endTime).equals(k.getColumnQualifier().toString()));
                log.trace("Are the durations equal?" + Long.toString(duration).equals(k.getColumnFamily().toString()));
            }
            
            try {
                Counters counters = new Counters();
                counters.readFields(new DataInputStream(new ByteArrayInputStream(entry.getValue().get())));
                Counter inFileRecord = counters.getGroup("InputFile").iterator().next();
                String inFile = inFileRecord.getName();
                inFlows.add(Pair.with(new Phase(inFile, inFileRecord.getValue(), endTime), counters.findCounter(InputFile.RECORDS).getValue()));
            } catch (IOException e) {
                throw new IOException("ERROR processing KEY=" + k, e);
            }
        }
        
        return inFlows.isEmpty() ? null : Pair.with(new TimeSummary(latestEnd, longestDur), inFlows);
    }
    
    /**
     * Scans the poller table for the given input files in the file array.
     * 
     * @return a TimeSummary of the input files; null if no input files
     */
    public Pair<TimeSummary,? extends Collection<Pair<Phase,Long>>> getFlagMakerStats(Text[] inFiles, final long totalEventsProcessed) throws IOException {
        log.info("in getFlagMakerStats");
        ArrayList<Range> ranges = new ArrayList<>(inFiles.length);
        for (Text inFile : inFiles) {
            ranges.add(new Range(inFile, inFile));
        }
        
        if (ranges.isEmpty()) {
            return null;
        }
        
        LinkedList<Pair<Phase,Long>> inFlows = Lists.newLinkedList();
        
        fmscan.setRanges(ranges);
        
        long longestDur = 0, latestEnd = 0;
        
        for (Entry<Key,Value> entry : fmscan) {
            Key k = entry.getKey();
            final long endTime = WritableUtil.parseLong(k.getColumnQualifier());
            if (endTime > latestEnd) {
                latestEnd = endTime;
            }
            final long duration = WritableUtil.parseLong(k.getColumnFamily());
            if (duration > longestDur) {
                longestDur = duration;
            }
            if (log.isTraceEnabled()) {
                log.trace("Are the timestamps equal? " + Long.toString(endTime).equals(k.getColumnQualifier().toString()));
                log.trace("Are the durations equal?" + Long.toString(duration).equals(k.getColumnFamily().toString()));
            }
            
            Counters counters = new Counters();
            serializeWritable(entry.getValue().get(), counters);
            Counter inFileRecord = counters.getGroup("InputFile").iterator().next();
            String inFile = inFileRecord.getName();
            inFlows.add(Pair.with(new Phase(inFile, inFileRecord.getValue(), endTime), 0L));
        }
        
        for (Pair<Phase,Long> inFlow : inFlows) {
            inFlow.setAt1(totalEventsProcessed / inFlows.size());
        }
        
        return inFlows.isEmpty() ? null : Pair.with(new TimeSummary(latestEnd, longestDur), inFlows);
    }
    
    public static Writable serializeWritable(final byte[] bytes, final Writable w) throws IOException {
        if (bytes == null || bytes.length == 0) {
            throw new IllegalArgumentException("Can't build a writable with empty " + "bytes array");
        }
        if (w == null) {
            throw new IllegalArgumentException("Writable cannot be null");
        }
        DataInputBuffer in = new DataInputBuffer();
        try {
            in.reset(bytes, bytes.length);
            w.readFields(in);
            return w;
        } finally {
            in.close();
        }
    }
    
    /**
     * Attempts to find the input files associated with {@code jobId} and data type {@code dataType}.
     */
    
    // Sometimes we'd still to get that information and not care about something so specific
    private Counters[] getIngestJobStats(String jobId) {
        return getIngestJobStats(jobId, null);
    }
    
    private Counters[] getIngestJobStats(String jobId, String dataType) {
        
        if (dataType != null) {
            dataType = dataType.toLowerCase();
        }
        
        List<Counters> counterList = new ArrayList<>();
        iscan.setRange(new Range("jobId\0" + jobId));
        
        for (Entry<Key,Value> entry : iscan) {
            Counters counters = new Counters();
            try {
                counters.readFields(ByteStreams.newDataInput(entry.getValue().get()));
                counterList.add(counters);
            } catch (IOException e) {
                log.error("Unable to parse counters: " + e.getMessage(), e);
                continue;
            }
        }
        
        return counterList.toArray(new Counters[counterList.size()]);
    }
    
    private Text[] findInputFiles(Counters[] counters) {
        
        ArrayList<Text> inFiles = new ArrayList<>();
        for (Counters cs : counters) {
            
            for (Counter fileNameCtr : cs.getGroup(IngestInput.FILE_NAME.name())) {
                String fileName = IngestMetricsMapper.extractFileName(fileNameCtr.getName());
                inFiles.add(new Text(fileName));
            }
        }
        
        return inFiles.toArray(new Text[inFiles.size()]);
    }
    
    private long totalIngestedRecords(Counters[] jobCounters) {
        long total = 0;
        
        for (Counters counters : jobCounters) {
            for (Counter eventsProcessed : counters.getGroup(IngestOutput.EVENTS_PROCESSED.name())) {
                total += eventsProcessed.getValue();
            }
        }
        
        return total;
    }
    
    /**
     * Returns a pair of longs, marking the end timestamp of the requested loader job and the duration of the job (first and second members of the pair,
     * respectively).
     * 
     * @return - null if no entry found for directory - A pair whose first entry is the end time of the job and second entry is the duration
     */
    public TimeSummary getLoaderStats(String loaderDir) {
        if (bulkLoaderCache.containsKey(loaderDir)) {
            return bulkLoaderCache.get(loaderDir);
        } else {
            lscan.setRange(new Range(loaderDir, loaderDir));
            // only need one value
            Iterator<Entry<Key,Value>> itr = lscan.iterator();
            if (itr.hasNext()) {
                Key k = itr.next().getKey();
                long duration = WritableUtil.parseLong(k.getColumnFamily());
                long endTime = WritableUtil.parseLong(k.getColumnQualifier());
                TimeSummary retVal = new TimeSummary(endTime, duration);
                bulkLoaderCache.put(loaderDir, retVal);
                return retVal;
            } else {
                return null;
            }
        }
    }
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        log.info("in setup");
        context.getCounter(Metrics.INIT_TIME).increment(System.currentTimeMillis());
        Configuration conf = context.getConfiguration();
        
        String user = conf.get(MetricsConfig.USER);
        String password = conf.get(MetricsConfig.PASS);
        String instance = conf.get(MetricsConfig.INSTANCE);
        String zookeepers = conf.get(MetricsConfig.ZOOKEEPERS);
        ignorePollerMetrics = conf.getBoolean(MetricsConfig.IGNORE_POLLER_METRICS, ignorePollerMetrics);
        ignoreFlagMakerMetrics = conf.getBoolean(MetricsConfig.IGNORE_FLAGMAKER_METRICS, ignoreFlagMakerMetrics);
        
        ZooKeeperInstance inst = new ZooKeeperInstance(ClientConfiguration.loadDefault().withInstance(instance).withZkHosts(zookeepers));
        try {
            Connector con = inst.getConnector(user, new PasswordToken(password));
            
            /***
             * if (!ignorePollerMetrics) { pscan = con.createBatchScanner(conf.get(MetricsConfig.POLLER_TABLE, MetricsConfig.DEFAULT_POLLER_TABLE),
             * Authorizations.EMPTY, 3); }
             ***/
            
            pscan = con.createBatchScanner(conf.get(MetricsConfig.POLLER_TABLE, MetricsConfig.DEFAULT_POLLER_TABLE), Authorizations.EMPTY, 3);
            fmscan = con.createBatchScanner(conf.get(MetricsConfig.FLAGMAKER_TABLE, MetricsConfig.DEFAULT_FLAGMAKER_TABLE), Authorizations.EMPTY, 3);
            
            iscan = con.createScanner(conf.get(MetricsConfig.INGEST_TABLE, MetricsConfig.DEFAULT_INGEST_TABLE), Authorizations.EMPTY);
            ibscan = con.createBatchScanner(conf.get(MetricsConfig.INGEST_TABLE, MetricsConfig.DEFAULT_INGEST_TABLE), Authorizations.EMPTY, 3);
            lscan = con.createScanner(conf.get(MetricsConfig.LOADER_TABLE, MetricsConfig.DEFAULT_LOADER_TABLE), Authorizations.EMPTY);
            
            BatchWriterConfig bwCfg = new BatchWriterConfig().setMaxLatency(1024L, TimeUnit.MILLISECONDS).setMaxMemory(2 * 1024L).setMaxWriteThreads(4);
            fileGraph = con.createBatchWriter(conf.get(MetricsConfig.FILE_GRAPH_TABLE, MetricsConfig.DEFAULT_FILE_GRAPH_TABLE), bwCfg);
        } catch (Exception e) {
            throw new IOException(e.getMessage());
        }
        
        state = SetupState.SCANNERS_OK;
        log.info("setup state = " + state);
    }
    
}
