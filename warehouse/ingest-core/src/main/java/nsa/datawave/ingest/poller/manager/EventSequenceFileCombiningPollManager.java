package nsa.datawave.ingest.poller.manager;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.ServiceLoader;

import nsa.datawave.common.cl.OptionBuilder;
import nsa.datawave.common.io.Files;
import nsa.datawave.ingest.data.RawRecordContainer;
import nsa.datawave.ingest.data.config.ConfigurationHelper;
import nsa.datawave.ingest.input.reader.EventRecordReader;
import nsa.datawave.poller.manager.SequenceFileCombiningPollManager;
import nsa.datawave.poller.manager.io.CountingSequenceFileOutputStream;
import nsa.datawave.poller.manager.mapreduce.StandaloneStatusReporter;
import nsa.datawave.poller.manager.mapreduce.StandaloneTaskAttemptContext;
import nsa.datawave.poller.metric.InputFile;
import nsa.datawave.poller.metric.OutputFile;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Subclass of the SequenceFileCombiningPollManager that works in concert with the EventRecordReader. Instead of outputting reader.getCurrentKey() and
 * reader.getCurrentValue() from the RecordReader to the SequenceFile, it outputs reader.getCurrentKey() and reader.getEvent().
 *
 * 
 */
public class EventSequenceFileCombiningPollManager extends SequenceFileCombiningPollManager {
    private static final Logger log = LoggerFactory.getLogger(EventSequenceFileCombiningPollManager.class);
    
    private static final CompressionCodec cc = new GzipCodec();
    private static final CompressionType ct = CompressionType.BLOCK;
    
    private static final String MD_OPT = "md";
    private static final String UT_OPT = "ut";
    private static final String DH_OPT = "dih";
    private static final String PS_OPT = "proc";
    
    public static final String DATAWAVE_INGEST_HOME = "DATAWAVE_INGEST_HOME";
    
    protected final ArrayList<PollerEventProcessor> processors = new ArrayList<>();
    protected LinkedList<Double> eventBytesPerByte = new LinkedList<>();
    
    private String metricsDirectory;
    
    protected int uncompressedEventDataThreshold;
    protected long currentUncompressedEventData;
    
    @Override
    public Options getConfigurationOptions() {
        final OptionBuilder optBuilder = new OptionBuilder();
        optBuilder.type = String.class;
        optBuilder.args = 1;
        
        final Options opts = super.getConfigurationOptions();
        opts.addOption(optBuilder.create(UT_OPT, "uncompressedEventMBThreshold", "Threshold on uncompressed event data"));
        opts.addOption(optBuilder.create(DH_OPT, "datawaveIngestHome", "Alternate mechanism for defining Datawave Ingest Home replacement value"));
        opts.addOption(optBuilder.create(PS_OPT, "processors", "List of event processor class names"));
        
        optBuilder.required = true;
        opts.addOption(optBuilder.create(MD_OPT, "MetricsDirectory", "Metrics Directory"));
        
        // Get the configuration options from all of the PollerEventProcessor implementations using the
        // ServiceLoader facility
        final ServiceLoader<PollerEventProcessor> availableProcessors = ServiceLoader.load(PollerEventProcessor.class);
        for (final PollerEventProcessor processor : availableProcessors) {
            for (Option option : processor.getConfigurationOptions()) {
                // Mark the option as optional even though it may be required for the processor. We are adding
                // the option for *all* processors here because the CommandLine object won't contain options
                // that it is not aware of.
                option.setRequired(false);
                opts.addOption(option);
            }
        }
        
        return opts;
    }
    
    @Override
    public void configure(final CommandLine cl) throws Exception {
        super.configure(cl);
        
        uncompressedEventDataThreshold = MB * Integer.parseInt(getLastOptionValue(cl, UT_OPT, "0"));
        metricsDirectory = getLastOptionValue(cl, MD_OPT);
        
        final String ingestHomeValue = getLastOptionValue(cl, DH_OPT, System.getenv(DATAWAVE_INGEST_HOME));
        
        Files.ensureDir(metricsDirectory, true);
        Files.ensureDir(ingestHomeValue);
        
        final Configuration config = ConfigurationHelper.interpolate(conf, "\\$\\{DATAWAVE_INGEST_HOME\\}", ingestHomeValue);
        job = new JobContextImpl(config, new JobID());
        
        String processorClassNames = cl.getOptionValue(PS_OPT);
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
    
    @Override
    protected boolean processKeyValues(final File file, final RecordReader<?,?> recReader, final InputSplit split,
                    final StandaloneTaskAttemptContext<?,?,?,?> ctx, final CountingSequenceFileOutputStream out) throws Exception {
        if (!(recReader instanceof EventRecordReader)) {
            throw new IOException("Expected an EventRecordReader when using a " + this.getClass().getSimpleName());
        }
        
        final EventRecordReader evtReader = (EventRecordReader) recReader;
        evtReader.setInputDate(file.lastModified());
        
        final long startBytes = counting.getByteCount();
        
        boolean complete = false;
        try {
            complete = processKeyValues(file, recReader, evtReader, ctx, out);
        } catch (Exception ex) {
            handleProcessingError(ctx, file, ex);
            throw new IOException("nextKeyValue call threw an exception", ex);
        }
        
        ctx.getCounter(OutputData.COMPRESSED_BYTES).increment(counting.getByteCount() - startBytes);
        
        return complete;
    }
    
    /**
     * Logic to determine if a new gzip output file is needed or not.
     *
     * @param nextFile
     *            the next input file
     * @return whether or not a new gzip output file is needed
     */
    @Override
    protected boolean needNewFile(final StandaloneTaskAttemptContext<?,?,?,?> ctx, final File nextFile) {
        long uncompressedEventBytes = currentUncompressedEventData + (ctx == null ? 0 : ctx.getCounter(OutputData.UNCOMPRESSED_EVENT_BYTES).getValue());
        if (uncompressedEventDataThreshold > 0) {
            if (!eventBytesPerByte.isEmpty() && nextFile != null) {
                final double eventBytesPerByteAvg = avgCompRatio(eventBytesPerByte);
                final long approxEventBytes = Math.round(nextFile.length() * eventBytesPerByteAvg);
                
                if ((approxEventBytes + uncompressedEventBytes) > uncompressedEventDataThreshold) {
                    log.info("Need new file, current output is {} event data bytes, next file size is {} records, threshold is {}", uncompressedEventBytes,
                                    approxEventBytes, uncompressedEventDataThreshold);
                    
                    return true;
                }
            } else {
                if (uncompressedEventBytes >= uncompressedEventDataThreshold) {
                    log.info("Need new file, current output is {} event data bytes, threshold is {}", uncompressedEventBytes, uncompressedEventDataThreshold);
                    return true;
                }
            }
        }
        
        return super.needNewFile(ctx, nextFile);
    }
    
    @Override
    public void close() throws IOException {
        super.close();
        for (PollerEventProcessor p : processors)
            p.close();
    }
    
    @Override
    protected void finishCurrentFile(final boolean closing) throws IOException {
        currentUncompressedEventData = 0;
        super.finishCurrentFile(closing);
    }
    
    private void updateStats(final StandaloneTaskAttemptContext<?,?,?,?> ctx, final File inputFile) {
        long outputRecords = ctx.getCounter(OutputFile.RECORDS).getValue();
        if (outputRecords <= 0)
            return;
        
        final long uncompressedEventBytes = ctx.getCounter(OutputData.UNCOMPRESSED_EVENT_BYTES).getValue();
        currentUncompressedEventData += uncompressedEventBytes;
        
        final long inputRecords = ctx.getCounter(InputFile.RECORDS).getValue();
        final double percentRecordsUsed = (double) outputRecords / inputRecords;
        final double currentEventBytesPerByte = (double) uncompressedEventBytes / (percentRecordsUsed * inputFile.length());
        
        if (eventBytesPerByte.size() > 100)
            eventBytesPerByte.removeFirst();
        eventBytesPerByte.addLast(currentEventBytesPerByte);
        
        log.info("Wrote {} event bytes from {}/{} of {} bytes with a ratio of {} event bytes per file byte.", uncompressedEventBytes, outputRecords,
                        inputRecords, inputFile.length(), currentEventBytesPerByte);
    }
    
    private boolean processKeyValues(final File file, final RecordReader<?,?> rReader, final EventRecordReader eReader,
                    final StandaloneTaskAttemptContext<?,?,?,?> ctx, final CountingSequenceFileOutputStream out) throws IOException, InterruptedException {
        long keyStart = System.currentTimeMillis();
        
        boolean emptyFile = true;
        int splitCheckCount = 0;
        while (rReader.nextKeyValue()) {
            try {
                final RawRecordContainer event = eReader.getEvent();
                emptyFile = false;
                processEvent(ctx, event, out);
                
                // if we need a new file independent of the input file, then stop with a false (incomplete) flag
                splitCheckCount++;
                if (splitCheckCount >= splitCheckFrequency) {
                    splitCheckCount = 0;
                    if (allowSplitting && needNewFile(ctx, null)) {
                        return false;
                    }
                }
                
            } catch (Exception ex) {
                handleProcessingError(ctx, file, ex);
            }
            
            keyStart = System.currentTimeMillis();
        }
        
        if (emptyFile) {
            // if we did not have any records for this file, then we should remove
            // this workfile as contributing
            completeContributingFile(file);
        }
        
        for (PollerEventProcessor p : processors) {
            p.finishFile();
        }
        
        return true;
    }
    
    private void processEvent(final StandaloneTaskAttemptContext<?,?,?,?> ctx, final RawRecordContainer event, final CountingSequenceFileOutputStream out)
                    throws IOException, InterruptedException {
        ctx.getCounter(InputFile.RECORDS).increment(1);
        
        final Object key = reader.getCurrentKey();
        out.write(ctx, key, event);
        
        // Invoke the processors
        if (null != event) {
            for (PollerEventProcessor p : processors)
                p.process(event);
        }
        
        if (event.getDate() > currentOutGzipFileDate)
            currentOutGzipFileDate = event.getDate();
        
        ctx.getCounter(OutputData.UNCOMPRESSED_EVENT_BYTES).increment(event.getDataOutputSize());
        ctx.getCounter(OutputFile.RECORDS).increment(1);
    }
    
    private void handleProcessingError(final StandaloneTaskAttemptContext<?,?,?,?> ctx, final File file, final Exception ex) {
        ctx.getCounter(InputFile.ERRORS).increment(1);
        log.error("Error translating record {} from file {}", ctx.getCounter("Input", "Total").getValue(), file.getAbsolutePath(), ex);
    }
}
