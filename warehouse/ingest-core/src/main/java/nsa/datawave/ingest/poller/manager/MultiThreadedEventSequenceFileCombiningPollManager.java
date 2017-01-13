package nsa.datawave.ingest.poller.manager;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.ServiceLoader;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import nsa.datawave.common.io.Files;
import nsa.datawave.ingest.data.RawRecordContainer;
import nsa.datawave.ingest.data.config.ConfigurationHelper;
import nsa.datawave.ingest.input.reader.EventRecordReader;
import nsa.datawave.ingest.time.Now;
import nsa.datawave.ingest.util.ThreadUtil;
import nsa.datawave.poller.manager.SequenceFileCombiningPollManager;
import nsa.datawave.poller.manager.io.CountingSequenceFileOutputStream;
import nsa.datawave.poller.manager.mapreduce.StandaloneStatusReporter;
import nsa.datawave.poller.manager.mapreduce.StandaloneTaskAttemptContext;
import nsa.datawave.poller.metric.InputFile;
import nsa.datawave.poller.metric.OutputFile;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.log4j.Logger;
import org.apache.log4j.NDC;

/**
 * Same functionality as the SequenceFileCombiningPollManager but writes to the output in a different thread. Note: this has not been fully tested.
 */
public class MultiThreadedEventSequenceFileCombiningPollManager extends SequenceFileCombiningPollManager {
    public static final String DATAWAVE_INGEST_HOME = "DATAWAVE_INGEST_HOME";
    
    /**
     * An instance of this filter can be used to filter out events that are not to be used. This must be thread safe.
     */
    public static interface EventFilter {
        /**
         * This is called when the event is to be queued. If this method returns false, then the event will not be queued.
         *
         * @param e
         *            event
         * @return true if the event is to be queued, false otherwise
         * @throws Exception
         */
        public boolean queue(Object key, RawRecordContainer e) throws Exception;
    }
    
    private final Object writerLock = new Object();
    
    /**
     * This is the worker used to actual write out the event
     */
    public class EventWriterWorker implements Runnable {
        private long start = 0;
        private File inputFile = null;
        private CountingSequenceFileOutputStream out = null;
        private StandaloneTaskAttemptContext<?,?,?,?> ctx = null;
        private Object key = null;
        private RawRecordContainer event = null;
        private long recordIndex = -1;
        
        public EventWriterWorker(File inputFile, CountingSequenceFileOutputStream out, StandaloneTaskAttemptContext<?,?,?,?> ctx, Object key,
                        RawRecordContainer event, int recordIndex, long start) {
            super();
            this.inputFile = inputFile;
            this.out = out;
            this.ctx = ctx;
            this.key = key;
            this.event = event;
            this.recordIndex = recordIndex;
            this.start = start;
        }
        
        @Override
        public void run() {
            boolean written = false;
            synchronized (writerLock) {
                try {
                    // even if the event is null, wrote to the output stream to count it as "skipped"
                    if (event != null && event.getDate() > currentOutGzipFileDate) {
                        currentOutGzipFileDate = event.getDate();
                    }
                    out.write(ctx, key, event);
                    
                    // Invoke the processors
                    if (null != event) {
                        for (PollerEventProcessor p : processors) {
                            p.process(event);
                        }
                    }
                    
                    written = true;
                } catch (Throwable e) {
                    ctx.getCounter(InputFile.ERRORS).increment(1);
                    log.error("Error translating record " + recordIndex + " from file " + inputFile.getAbsolutePath(), e);
                }
            }
            
            try {
                if (event != null && written) {
                    ctx.getCounter(OutputData.UNCOMPRESSED_EVENT_BYTES).increment(event.getDataOutputSize());
                    ctx.getCounter(OutputFile.RECORDS).increment(1);
                }
            } catch (Exception e) {
                log.error("Error sending metrics for record " + recordIndex + " from file " + inputFile.getAbsolutePath(), e);
            }
        }
        
    }
    
    private static final Logger log = Logger.getLogger(MultiThreadedEventSequenceFileCombiningPollManager.class);
    
    protected ThreadPoolExecutor executor = null;
    
    protected BlockingQueue<Runnable> workQueue = null;
    
    protected int threads = 10;
    
    protected int queueSize = 1000;
    
    protected long workUnits = 0;
    
    protected CompressionCodec cc = new GzipCodec();
    protected CompressionType ct = CompressionType.BLOCK;
    
    protected Option metricsDirOption = null;
    private Option uncompressedThresholdOption = null;
    private Option datawaveIngestHomeOption = null;
    private Option processorsOption = null;
    
    protected String metricsDirectory = null;
    
    protected static Now now = Now.getInstance();
    
    protected int uncompressedEventDataThreshold = 0;
    final protected LinkedList<Double> eventBytesPerByte = new LinkedList<>();
    
    protected long currentUncompressedEventData = 0;
    final protected ArrayList<PollerEventProcessor> processors = new ArrayList<>();
    
    /**
     * This method can be overridden to supply an event filter if desired
     *
     * @return an event filter
     */
    protected EventFilter getEventFilter() {
        return null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see nsa.datawave.poller.manager.FileCombiningPollManager#getConfigurationOptions()
     */
    @Override
    public Options getConfigurationOptions() {
        // Get options from super class
        Options o = super.getConfigurationOptions();
        
        // Add our own options
        metricsDirOption = new Option("md", "MetricsDirectory", true, "Metrics Directory");
        metricsDirOption.setRequired(true);
        metricsDirOption.setArgs(1);
        metricsDirOption.setType(String.class);
        o.addOption(metricsDirOption);
        
        uncompressedThresholdOption = new Option("ut", "uncompressedEventMBThreshold", true, "Threshold on uncompressed event data in MB");
        uncompressedThresholdOption.setRequired(false);
        uncompressedThresholdOption.setArgs(1);
        uncompressedThresholdOption.setType(String.class);
        o.addOption(uncompressedThresholdOption);
        
        datawaveIngestHomeOption = new Option("dih", "datawaveIngestHome", true, "Alternate mechanism for defining Datawave Ingest Home replacement value");
        datawaveIngestHomeOption.setRequired(false);
        datawaveIngestHomeOption.setArgs(1);
        datawaveIngestHomeOption.setType(String.class);
        o.addOption(datawaveIngestHomeOption);
        
        processorsOption = new Option("proc", "processors", true, "List of event processor class names");
        processorsOption.setRequired(false);
        processorsOption.setArgs(1);
        processorsOption.setType(String.class);
        o.addOption(processorsOption);
        
        // Get the configuration options from all of the PollerEventProcessor implementations using the
        // ServiceLoader facility
        ServiceLoader<PollerEventProcessor> availableProcessors = ServiceLoader.load(PollerEventProcessor.class);
        for (PollerEventProcessor processor : availableProcessors) {
            for (Option option : processor.getConfigurationOptions()) {
                // Mark the option as optional even though it may be required for the processor. We are adding
                // the option for *all* processors here because the CommandLine object won't contain options
                // that it is not aware of.
                option.setRequired(false);
                o.addOption(option);
            }
        }
        
        return o;
    }
    
    @Override
    public void configure(CommandLine cl) throws Exception {
        super.configure(cl);
        
        this.uncompressedEventDataThreshold = MB * Integer.parseInt(getLastOptionValue(cl, uncompressedThresholdOption.getOpt(), "0"));
        this.metricsDirectory = getLastOptionValue(cl, metricsDirOption.getOpt());
        File dir = new File(this.metricsDirectory);
        Files.ensureDir(dir, true);
        
        workQueue = new ArrayBlockingQueue<>(queueSize);
        executor = new ThreadPoolExecutor(threads, threads, 500, TimeUnit.MILLISECONDS, workQueue);
        executor.prestartAllCoreThreads();
        
        // Need to interpolate the configuration and replace DATAWAVE_INGEST_HOME.
        String ingestHomeValue = getLastOptionValue(cl, datawaveIngestHomeOption.getOpt());
        
        if (StringUtils.isEmpty(ingestHomeValue)) {
            ingestHomeValue = System.getenv(DATAWAVE_INGEST_HOME);
        }
        
        if (null == ingestHomeValue) {
            throw new IllegalArgumentException("DATAWAVE_INGEST_HOME must be set in the environment.");
        }
        
        // Create a new configuration from the old one and replace the value.
        Configuration config = ConfigurationHelper.interpolate(this.conf, "\\$\\{DATAWAVE_INGEST_HOME\\}", ingestHomeValue);
        
        // Overwrite the job object with the new configuration
        this.job = new JobContextImpl(config, new JobID());
        
        String processorClassNames = cl.getOptionValue(processorsOption.getOpt());
        if (null != processorClassNames) {
            String[] classes = processorClassNames.split(",");
            for (String clazz : classes) {
                Class<?> c = Class.forName(clazz);
                PollerEventProcessor proc = (PollerEventProcessor) c.newInstance();
                proc.getConfigurationOptions();
                proc.configure(cl, config);
                this.processors.add(proc);
            }
        }
    }
    
    @Override
    protected void resetCounters(final StandaloneTaskAttemptContext<?,?,?,?> ctx) {
        super.resetCounters(ctx);
        StandaloneStatusReporter reporter = ctx.getReporter();
        clearCounters(reporter.getCounters(), OutputData.values());
    }
    
    @Override
    protected void completedOutputFile(final StandaloneTaskAttemptContext<?,?,?,?> ctx, File inputFile) throws IOException {
        updateStats(ctx, inputFile);
        updateMetrics(ctx.getReporter(), metricsDirectory, inputFile.getName(), ct, cc);
        super.completedOutputFile(ctx, inputFile);
    }
    
    @Override
    protected void completedInputFile(final StandaloneTaskAttemptContext<?,?,?,?> ctx, File inputFile) throws IOException {
        super.completedInputFile(ctx, inputFile);
        // if this input file did not contribute to the current output file, then the last completedOutputFile call was sufficient
        if (ctx.getCounter(OutputFile.RECORDS).getValue() != 0) {
            updateStats(ctx, inputFile);
            updateMetrics(ctx.getReporter(), metricsDirectory, inputFile.getName(), ct, cc);
        }
    }
    
    private void updateStats(final StandaloneTaskAttemptContext<?,?,?,?> ctx, File inputFile) {
        // if we actually output any records this round
        long outputRecords = ctx.getCounter(OutputFile.RECORDS).getValue();
        if (outputRecords > 0) {
            // update our event bytes counter and event bytes per bytes average
            long uncompressedEventBytes = ctx.getCounter(OutputData.UNCOMPRESSED_EVENT_BYTES).getValue();
            currentUncompressedEventData += uncompressedEventBytes;
            
            // We may have not output all of the input records, so determine the percentage of
            // the input file actually used
            long inputRecords = ctx.getCounter(InputFile.RECORDS).getValue();
            double percentRecordsUsed = (double) outputRecords / inputRecords;
            double currentEventBytesPerByte = (double) uncompressedEventBytes / (percentRecordsUsed * inputFile.length());
            if (eventBytesPerByte.size() > 100) {
                eventBytesPerByte.removeFirst();
            }
            eventBytesPerByte.addLast(currentEventBytesPerByte);
            log.info("Wrote " + uncompressedEventBytes + " event bytes from " + outputRecords + "/" + inputRecords + " of " + inputFile.length()
                            + " bytes with a ratio of " + currentEventBytesPerByte + " event bytes per file byte");
        }
    }
    
    @Override
    protected boolean processKeyValues(File inputFile, RecordReader<?,?> reader, InputSplit split, StandaloneTaskAttemptContext<?,?,?,?> ctx,
                    CountingSequenceFileOutputStream out) throws Exception {
        boolean complete = false;
        if (reader instanceof EventRecordReader) {
            long start = now.get();
            EventFilter filter = getEventFilter();
            EventRecordReader eReader = (EventRecordReader) reader;
            eReader.setInputDate(inputFile.lastModified());
            long startBytes = counting.getByteCount();
            // Read in each "record", then write it to the output stream.
            boolean done = false;
            int recordIndex = 0;
            boolean first = true;
            int splitCheckCount = 0;
            try {
                while (!done) {
                    long startKeyValue = System.currentTimeMillis();
                    if (!reader.nextKeyValue()) {
                        if (first) {
                            // if we did not have any records for this file, then we should remove
                            // this workfile as contributing
                            completeContributingFile(inputFile);
                        }
                        complete = true;
                        done = true;
                        continue;
                    }
                    recordIndex++;
                    
                    // if we are shutting down, then abort processing here
                    if (closed) {
                        throw new IOException("Unable to complete processing as we are shutting down");
                    }
                    
                    RawRecordContainer e = eReader.getEvent();
                    first = false;
                    ctx.getCounter(InputFile.RECORDS).increment(1);
                    // if the filter tells us to skip this event, then write a null event which will count it as "skipped"
                    if (filter != null && !filter.queue(reader.getCurrentKey(), e)) {
                        e = null;
                    } else {
                        // make a copy to ensure no threading issues
                        e = e.copy();
                    }
                    EventWriterWorker worker = new EventWriterWorker(inputFile, out, ctx, reader.getCurrentKey(), e, recordIndex, startKeyValue);
                    boolean taken = workQueue.offer(worker);
                    int loopcount = 0;
                    while (!taken) {
                        loopcount++;
                        if (loopcount % 1000 == 0) {
                            log.debug("Queue full, sleeping...");
                        }
                        Thread.sleep(100);
                        taken = workQueue.offer(worker);
                    }
                    workUnits++;
                    
                    // if we need a new file independent of the input file, then stop with a false (incomplete) flag
                    splitCheckCount++;
                    if (splitCheckCount >= splitCheckFrequency) {
                        splitCheckCount = 0;
                        if (allowSplitting && needNewFile(ctx, null)) {
                            complete = false;
                            done = true;
                        }
                    }
                }
            } finally {
                // Need to wait for the background threads to complete.
                long time = ThreadUtil.waitForThreads(log, executor, "EventWriterWorkers", executor.getMaximumPoolSize(), workUnits, start);
                log.info(this.getClass().getName() + " background threads took " + (time / 1000.0) + " seconds");
            }
            // Now that all workers are done pushing events, tell processors to complete the current file
            if (complete) {
                for (PollerEventProcessor p : this.processors) {
                    p.finishFile();
                }
            }
            long endBytes = counting.getByteCount();
            ctx.getCounter(OutputData.COMPRESSED_BYTES).increment(endBytes - startBytes);
        } else {
            throw new IOException("Expected the reader to be an EventRecordReader when using the " + this.getClass().getSimpleName());
        }
        return complete;
    }
    
    /**
     * Logic to determine if a new gzip output file is needed or not.
     *
     * @param nextFile
     *            the next file
     * @return whether or not a new gzip output file is needed
     */
    @Override
    protected boolean needNewFile(final StandaloneTaskAttemptContext<?,?,?,?> ctx, final File nextFile) {
        long uncompressedEventBytes = currentUncompressedEventData + (ctx == null ? 0 : ctx.getCounter(OutputData.UNCOMPRESSED_EVENT_BYTES).getValue());
        
        if (uncompressedEventDataThreshold > 0 && !eventBytesPerByte.isEmpty() && null != nextFile) {
            double eventBytesPerByteAvg = 0.0d;
            for (Double d : eventBytesPerByte) {
                eventBytesPerByteAvg += d;
            }
            eventBytesPerByteAvg /= eventBytesPerByte.size();
            long approxEventBytes = Math.round(nextFile.length() * eventBytesPerByteAvg);
            if (log.isDebugEnabled()) {
                log.debug("Calculated approx " + eventBytesPerByteAvg + " event bytes per event using " + eventBytesPerByte.size() + " samples");
                log.debug("  Resulting in approx " + approxEventBytes + " event bytes for a file of " + nextFile.length() + " bytes");
            }
            if ((approxEventBytes + uncompressedEventBytes) > uncompressedEventDataThreshold) {
                log.info("Need new file, current output is " + uncompressedEventBytes + " event data bytes, next file size is " + approxEventBytes
                                + " records, threshold is " + uncompressedEventDataThreshold);
                return true;
            }
            
            if (log.isDebugEnabled()) {
                log.debug("Do not need new file, current output is " + uncompressedEventBytes + " event data bytes, next file size is " + approxEventBytes
                                + " records, threshold is " + uncompressedEventDataThreshold);
            }
        } else if (uncompressedEventDataThreshold > 0) {
            if (uncompressedEventBytes >= uncompressedEventDataThreshold) {
                log.info("Need new file, current output is " + uncompressedEventBytes + " event data bytes, threshold is " + uncompressedEventDataThreshold);
                return true;
            }
            if (log.isDebugEnabled()) {
                log.debug("Do not need new file, current output is " + uncompressedEventBytes + " event data bytes, threshold is "
                                + uncompressedEventDataThreshold);
            }
        }
        
        // checking the record count against the INPUT_RECORDS as the (OUTPUT_)RECORDS count may not be updated yet by the event workers.
        long recordCount = currentRecords + (ctx == null ? 0 : ctx.getCounter(InputFile.RECORDS).getValue());
        if (recThreshold > 0 && recordCount >= recThreshold) {
            log.info("Need new file, current output is " + recordCount + " records, threshold is " + recThreshold);
            return true;
        }
        
        return super.needNewFile(ctx, nextFile);
    }
    
    @Override
    public void close() throws IOException {
        super.close();
        
        for (PollerEventProcessor p : this.processors) {
            p.close();
        }
        
        ThreadUtil.shutdownAndWait(executor, 1, TimeUnit.MINUTES);
    }
    
    @Override
    protected void finishCurrentFile(boolean closing) throws IOException {
        currentUncompressedEventData = 0;
        super.finishCurrentFile(closing);
    }
}
