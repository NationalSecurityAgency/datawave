package nsa.datawave.poller.manager;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import nsa.datawave.common.cl.OptionBuilder;
import nsa.datawave.common.io.Files;
import nsa.datawave.data.hash.UID;
import static nsa.datawave.data.hash.UIDConstants.*;
import nsa.datawave.poller.manager.io.CountingSequenceFileOutputStream;
import nsa.datawave.poller.manager.mapreduce.StandaloneStatusReporter;
import nsa.datawave.poller.manager.mapreduce.StandaloneTaskAttemptContext;
import nsa.datawave.poller.metric.InputFile;
import nsa.datawave.poller.metric.OutputFile;
import nsa.datawave.util.time.DateHelper;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.log4j.NDC;
import org.sadun.util.polling.DirectoryPoller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SequenceFileCombiningPollManager extends FileCombiningPollManager {
    private static final Logger log = LoggerFactory.getLogger(SequenceFileCombiningPollManager.class);
    
    private static final String RT_OPT = "rt";
    private static final String CF_OPT = "cf";
    private static final String IKC_OPT = "ikc";
    private static final String IVC_OPT = "ivc";
    private static final String OKC_OPT = "okc";
    private static final String OVC_OPT = "ovc";
    
    private static final String NOSPLIT_OPT = "nosplit";
    private static final String SCF_OPT = "scf";
    
    private final LinkedList<Double> bytesPerRecord = new LinkedList<>();
    
    private Class<?> outKClass;
    private Class<?> outVClass;
    protected Class<?> kClass;
    protected Class<?> vClass;
    
    protected CountingSequenceFileOutputStream myOut;
    protected RawLocalFileSystem rawFS;
    protected RecordReader<?,?> reader;
    protected InputFormat<?,?> format;
    protected Configuration conf;
    protected JobContext job;
    
    protected int recThreshold;
    protected long currentRecords;
    
    // do we allow input files to be split among output files
    protected boolean allowSplitting = true;
    // how often (in events) do we check whether the input file should be split (i.e. we clip the output file and start another)
    protected int splitCheckFrequency = 1;
    
    /** Default constructor */
    public SequenceFileCombiningPollManager() {}
    
    @Override
    public Options getConfigurationOptions() {
        final OptionBuilder builder = new OptionBuilder();
        builder.type = String.class;
        builder.args = 0;
        builder.required = false;
        final Options opts = super.getConfigurationOptions();
        
        opts.addOption(builder.create(NOSPLIT_OPT, "NoSplitting", "Disable input file splitting"));
        
        builder.args = 1;
        opts.addOption(builder.create(RT_OPT, "recordThreshold", "Threshold on records per output sequence file"));
        opts.addOption(builder.create(SCF_OPT, "splitCheckFrequency", "The frequency in events at which we check for whether we should split the input file."));
        
        builder.required = true;
        opts.addOption(builder.create(IKC_OPT, "InputKeyClass", "Input Key Class"));
        opts.addOption(builder.create(IVC_OPT, "InputValueClass", "Input Value Class"));
        opts.addOption(builder.create(OKC_OPT, "OutputKeyClass", "Output Key Class"));
        opts.addOption(builder.create(OVC_OPT, "OutputValueClass", "Output Value Class"));
        
        builder.valSeparator = ',';
        builder.args = Option.UNLIMITED_VALUES;
        opts.addOption(builder.create(CF_OPT, "confFiles", "HDFS MapReduce configuration Files"));
        
        return opts;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public void configure(CommandLine cl) throws Exception {
        super.configure(cl);
        
        final String keyClazz = getLastOptionValue(cl, IKC_OPT);
        final String valueClazz = getLastOptionValue(cl, IVC_OPT);
        final String outKeyClazz = getLastOptionValue(cl, OKC_OPT);
        final String outValueClazz = getLastOptionValue(cl, OVC_OPT);
        
        if (cl.hasOption(NOSPLIT_OPT)) {
            allowSplitting = false;
        }
        
        splitCheckFrequency = Integer.parseInt(getLastOptionValue(cl, SCF_OPT, "1"));
        
        final String[] cFiles = cl.getOptionValues(CF_OPT);
        recThreshold = Integer.parseInt(getLastOptionValue(cl, RT_OPT, "0"));
        conf = new Configuration();
        
        for (final String name : cFiles) {
            try {
                conf.addResource(new URL(name));
            } catch (MalformedURLException e) {
                conf.addResource(name);
            }
        }
        
        if (log.isTraceEnabled()) {
            for (final Entry<String,String> p : conf) {
                log.trace("Property {} -> {}", p.getKey(), p.getValue());
            }
        } else {
            // Ensure that the Configuration Resources are parsed
            // and integrated into internal properties data structure
            conf.size();
        }
        
        kClass = Class.forName(keyClazz);
        vClass = Class.forName(valueClazz);
        outKClass = Class.forName(outKeyClazz);
        outVClass = Class.forName(outValueClazz);
        
        rawFS = new RawLocalFileSystem();
        rawFS.setConf(conf);
        
        job = new JobContextImpl(conf, new JobID());
        format = getInputFormat(conf.get("file.input.format"));
        
        // Assign options used to configure with the UID builder. If a non-negative
        // thread index is defined, add a thread-unique option and value since the
        // previously parsed command-line options are fixed and not thread-safe.
        final Option[] options;
        int threadIndex = getThreadIndex();
        if (threadIndex >= 0) {
            final Option[] clOptions = cl.getOptions();
            options = Arrays.copyOf(clOptions, clOptions.length + 1);
            final OptionBuilder builder = new OptionBuilder();
            builder.type = String.class;
            builder.args = 1;
            builder.required = false;
            final Option option = builder.create(THREAD_INDEX_OPT, THREAD_INDEX_OPT,
                            "Thread index value from 0-63, inclusive (only required for uidType SnowflakeUID)");
            option.getValuesList().add(Integer.toString(threadIndex));
            options[options.length - 1] = option;
        }
        // Otherwise, just use the command-line options without modification
        else {
            options = cl.getOptions();
        }
        
        // Update the configuration using the default UID builder
        UID.builder().configure(conf, options);
    }
    
    /**
     * A reporter in which the counters can be cleared
     */
    public static class ClearableStandaloneStatusReporter extends StandaloneStatusReporter {
        
        private Counters c = new Counters();
        
        public ClearableStandaloneStatusReporter() {
            super();
        }
        
        public Counters getCounters() {
            return c;
        }
        
        @Override
        public Counter getCounter(Enum<?> name) {
            return c.findCounter(name);
        }
        
        @Override
        public Counter getCounter(String group, String name) {
            return c.findCounter(group, name);
        }
        
        public void clearCounters() {
            c = new Counters();
        }
        
    }
    
    protected void resetCounters(final StandaloneTaskAttemptContext<?,?,?,?> ctx) {
        StandaloneStatusReporter reporter = ctx.getReporter();
        clearCounters(reporter.getCounters().getGroup("Output"));
        clearCounters(reporter.getCounters().getGroup("OutputFile"));
        clearCounters(reporter.getCounters(), OutputFile.values());
        clearCounters(reporter.getCounters(), InputFile.values());
        reporter.getCounter(InputFile.POLLER_START_TIME).setValue(System.currentTimeMillis());
    }
    
    protected void clearCounters(CounterGroup group) {
        for (Iterator<Counter> it = group.iterator(); it.hasNext();) {
            it.next();
            it.remove();
        }
    }
    
    protected void clearCounters(Counters c, Enum<?>[] enumerations) {
        for (Enum<?> enumeration : enumerations) {
            c.findCounter(enumeration).setValue(0);
        }
    }
    
    @Override
    protected long processFile(final File file, final OutputStream out) throws Exception {
        final StandaloneTaskAttemptContext<?,?,?,?> ctx = new StandaloneTaskAttemptContext<Object,Object,Object,Object>(job.getConfiguration(),
                        new ClearableStandaloneStatusReporter());
        return processFile(ctx, file, out);
    }
    
    protected long processFile(final StandaloneTaskAttemptContext<?,?,?,?> ctx, final File file, final OutputStream out) throws Exception {
        
        final long fileLen = file.length();
        final long bytesWritten = counting.getByteCount();
        
        final File inputFile = handleCompressed(file);
        
        NDC.push(String.format("%s:%s", dataType, file.getName()));
        try {
            final FileSplit is = fileSplit(inputFile);
            
            reader = format.createRecordReader(is, ctx);
            
            ctx.getCounter("InputFile", inputFile.getName()).increment(inputFile.lastModified());
            ctx.putIfAbsent(InputFile.POLLER_START_TIME, System.currentTimeMillis());
            
            try {
                reader.initialize(is, ctx);
                boolean complete = false;
                while (!complete) {
                    complete = processKeyValues(inputFile, reader, is, ctx, myOut);
                    // if we did not complete processing this file, it is because we need to cycle the output file
                    if (!complete) {
                        updateStats(ctx, fileLen, bytesWritten);
                        cycleOutputFile(inputFile);
                        
                        completedOutputFile(ctx, inputFile);
                    }
                }
            } finally {
                reader.close();
            }
            
            updateStats(ctx, fileLen, bytesWritten);
            completedInputFile(ctx, inputFile);
        } finally {
            NDC.pop();
            if (!inputFile.equals(file))
                Files.ensureMv(inputFile, file);
        }
        
        return file.length();
    }
    
    /***
     * Called after an output file has been completed and stats have been updated, but before
     * 
     * @param ctx
     */
    protected void completedOutputFile(final StandaloneTaskAttemptContext<?,?,?,?> ctx, File workFile) throws IOException {
        resetCounters(ctx);
    }
    
    /***
     * Called after an input file has been completed and stats have been updated, but before
     * 
     * @param ctx
     */
    protected void completedInputFile(final StandaloneTaskAttemptContext<?,?,?,?> ctx, File workFile) throws IOException {}
    
    /**
     * Process all key value pairs from the record reader and write them to the output stream.
     *
     * @param inputFile
     *            input file
     * @param reader
     *            record reader
     * @param ctx
     *            task attempt context
     * @param out
     *            Counting SequenceFile OutputStream
     * @return true is file is completely processed, false if there is more data to read and process
     * @throws InterruptedException
     * @throws IOException
     */
    protected boolean processKeyValues(final File inputFile, final RecordReader<?,?> reader, final InputSplit split,
                    final StandaloneTaskAttemptContext<?,?,?,?> ctx, final CountingSequenceFileOutputStream out) throws Exception {
        
        boolean emptyFile = true;
        int splitCheckCount = 0;
        while (true) {
            try {
                if (!reader.nextKeyValue())
                    break;
                emptyFile = false;
                
                ctx.getCounter(InputFile.RECORDS).increment(1);
                out.write(ctx, reader.getCurrentKey(), reader.getCurrentValue());
                ctx.getCounter(OutputFile.RECORDS).increment(1);
                
                // if we need a new file independent of the input file, then stop with a false (incomplete) flag
                splitCheckCount++;
                if (splitCheckCount >= splitCheckFrequency) {
                    splitCheckCount = 0;
                    if (allowSplitting && needNewFile(ctx, null)) {
                        return false;
                    }
                }
                
            } catch (Exception e) {
                ctx.getCounter(InputFile.ERRORS).increment(1);
                log.error("Error translating record " + ctx.getCounter("Input", "Total").getValue() + " from file " + inputFile.getAbsolutePath(), e);
            }
        }
        if (emptyFile) {
            // if we did not have any records for this file, then we should remove
            // this workfile as contributing
            completeContributingFile(inputFile);
        }
        
        // we have completed the input file, return true
        return true;
    }
    
    /**
     * If we are allowing splitting of files, then we can no longer check that the complete input file went into the output file
     */
    protected boolean checkOutput(final DirectoryPoller poller, final long cLen, final long fLen) {
        if (allowSplitting) {
            return true;
        } else {
            return super.checkOutput(poller, cLen, fLen);
        }
    }
    
    /**
     * Creates a new output file.
     *
     * @throws IOException
     */
    @Override
    protected void createNewOutputFile() throws IOException {
        startTime = System.currentTimeMillis();
        currentFileContributors.clear();
        currentOutGzipFileDate = -1;
        bytesRead = 0;
        
        try {
            currentOutGzipFile = new File(outFilename());
            log.info("Creating new output file " + currentOutGzipFile.getName());
            
            myOut = new CountingSequenceFileOutputStream(rawFS, job, file2path(currentOutGzipFile), currentOutGzipFile, outKClass, outVClass);
            out = myOut;
            counting = myOut;
            currentRecords = 0;
            outGzipFiles.add(currentOutGzipFile.getName());
            log.info("New set of output files is " + outGzipFiles);
        } catch (IOException e) {
            IOUtils.closeQuietly(counting);
            out = null;
            
            throw new IOException("Error setting up new output file", e);
        }
    }
    
    /**
     * Logic to determine if a new gzip output file is needed or not.
     *
     * @param nextFile
     *            the next input file.
     * @return whether or not a new file is needed.
     */
    @Override
    protected boolean needNewFile(final File nextFile) {
        // turning off anticipatory turnover of output files if allowing splitting by not passing the nextFile in
        return needNewFile(null, (allowSplitting ? null : nextFile));
    }
    
    protected boolean needNewFile(final StandaloneTaskAttemptContext<?,?,?,?> ctx, File nextFile) {
        long recordCount = currentRecords + (ctx == null ? 0 : ctx.getCounter(OutputFile.RECORDS).getValue());
        if (recThreshold > 0 && !bytesPerRecord.isEmpty() && null != nextFile) {
            final double bytesPerRecordAvg = avgCompRatio(bytesPerRecord);
            final long approxRecords = Math.round(nextFile.length() / bytesPerRecordAvg);
            
            log.debug("Calculated approx {} bytes per record using {} samples", bytesPerRecordAvg, bytesPerRecord.size());
            log.debug("  Resulting in approx {} records for a file of {} bytes", approxRecords, nextFile.length());
            
            if ((approxRecords + recordCount) > recThreshold) {
                log.info("Need new file, current output is {} records, next file size is {} records, threshold is {}", recordCount, approxRecords, recThreshold);
                return true;
            }
            
            log.debug("Do not need new file, current output is {} records, next file size is {} records, threshold is ", recordCount, approxRecords,
                            recThreshold);
        } else if (recThreshold > 0) {
            if (recordCount >= recThreshold) {
                log.info("Need new file, current output is {} records, threshold is {}", recordCount, recThreshold);
                return true;
            }
            
            log.debug("Do not need new file, current output is {} records, threshold is {}", recordCount, recThreshold);
        }
        
        return super.needNewFile(nextFile);
    }
    
    @Override
    public void close() throws IOException {
        super.close();
        
        if (null != reader)
            reader.close();
        if (null != rawFS)
            rawFS.close();
        writeProvenanceReports();
    }
    
    @Override
    protected void finishCurrentFile(final boolean closing) throws IOException {
        currentRecords = 0;
        super.finishCurrentFile(closing);
    }
    
    /**
     * updateMetrics
     * <p>
     * This logic had been duplicated in two different derived classes, in Ingest-Base project. Moved here to make maintenance easier.
     *
     * @param reporter
     *            the reporter to use.
     * @param metricsDirectory
     *            the directory to store metrics in.
     * @param inputFileName
     *            the name of the input file.
     * @param ct
     *            Compression Type
     * @param cc
     *            Compression Codec
     * @throws IOException
     */
    protected void updateMetrics(final StandaloneStatusReporter reporter, final String metricsDirectory, final String inputFileName, final CompressionType ct,
                    final CompressionCodec cc) throws IOException {
        if (reporter == null)
            return;
        logCounters(reporter);
        
        final Counters c = reporter.getCounters();
        if (c == null || c.countCounters() <= 0)
            return;
        
        final String baseFileName = metricsDirectory + File.separator + inputFileName;
        
        String fileName = baseFileName;
        Path finishedMetricsFile = file2path(fileName);
        Path src = file2path(fileName + ".working");
        
        int count = 0;
        
        while (true) {
            while (rawFS.exists(finishedMetricsFile) || !rawFS.createNewFile(src)) {
                count++;
                
                fileName = baseFileName + '.' + count;
                finishedMetricsFile = file2path(fileName);
                src = file2path(fileName + ".working");
            }
            
            if (!rawFS.exists(finishedMetricsFile))
                break;
            rawFS.delete(src, false);
        }
        
        final Writer writer = SequenceFile.createWriter(conf, Writer.file(src), Writer.compression(ct, cc), Writer.keyClass(NullWritable.class),
                        Writer.valueClass(Counters.class));
        writer.append(NullWritable.get(), c);
        writer.close();
        
        if (!rawFS.rename(src, finishedMetricsFile))
            log.error("Could not rename metrics file to completed name. Failed file will persist until manually removed.");
        
        // remove the crc file if any
        Path crcPath = new Path(src.getParent(), "." + src.getName() + ".crc");
        if (rawFS.exists(crcPath)) {
            rawFS.delete(crcPath, false);
        }
    }
    
    /**
     * Creates an instance of an InputFormat.
     *
     * @param fmtClass
     *            Full classname for the input format.
     * @return a new instance of the InputFormat.
     * @throws Exception
     */
    private InputFormat<?,?> getInputFormat(String fmtClass) throws Exception {
        return (InputFormat<?,?>) Class.forName(fmtClass).newInstance();
    }
    
    /**
     * If the data is compressed and the file does not end in ".gz"; the original file is moved such that it does end in ".gz".
     *
     * @param file
     *            The file to check and possibly move.
     * @return the original file, or a new File if the original needed to be moved.
     * @throws IOException
     */
    private File handleCompressed(final File file) throws IOException {
        if (compressedInput && !file.getName().endsWith(".gz")) {
            final File newFile = new File(localWorkDir, file.getName() + ".gz");
            Files.ensureMv(file, newFile);
            
            log.info("Moved {} to {} to process as compressed input");
            
            return newFile;
        }
        
        return file;
    }
    
    /**
     * Updates counters and compression ratio data.
     *
     * @param ctx
     *            The context
     * @param fileLen
     *            The length of the original input file
     * @param bytesWritten
     *            The number of bytes written to the current output file
     */
    private void updateStats(final StandaloneTaskAttemptContext<?,?,?,?> ctx, final long fileLen, final long bytesWritten) {
        final long outputRecords = ctx.getCounter(OutputFile.RECORDS).getValue();
        currentRecords += outputRecords;
        
        final long inputRecords = ctx.getCounter(InputFile.RECORDS).getValue();
        final double currentBytesPerRecord = (double) fileLen / (double) inputRecords;
        
        if (bytesPerRecord.size() > 100)
            bytesPerRecord.removeFirst();
        bytesPerRecord.addLast(currentBytesPerRecord);
        
        log.info("Read {} records from {} bytes with a ratio of {} bytes per record.", inputRecords, fileLen, currentBytesPerRecord);
        
        final long lastValue = ctx.getCounter(InputFile.POLLER_END_TIME).getValue();
        ctx.getCounter(InputFile.POLLER_END_TIME).increment(System.currentTimeMillis() - lastValue);
        
        if (counting.getByteCount() > bytesWritten) {
            ctx.getCounter("OutputFile", currentOutGzipFile.getName()).increment(1);
        }
    }
    
    /**
     * Logs all of the counters from the StatusReporter.
     *
     * @param reporter
     *            StatusReporter with the counters.
     */
    protected void logCounters(final StandaloneStatusReporter reporter) {
        if (!log.isInfoEnabled())
            return;
        String groupName;
        
        for (final CounterGroup g : reporter.getCounters()) {
            groupName = g.getDisplayName();
            for (final Counter ctr : g)
                log.info("[COUNTER] {}:{}->{}", groupName, ctr.getDisplayName(), ctr.getValue());
        }
    }
    
    /**
     * Utility for creating a new FileSplit.
     *
     * @param file
     *            The file to create the FileSplit for.
     * @return thw new FileSplit
     */
    private FileSplit fileSplit(final File file) {
        return new FileSplit(file2path(file), 0, file.length(), new String[] {hostName});
    }
    
    /**
     * Utility for converting a filename into a {@link Path}.
     *
     * @param filename
     *            the filename to convert
     * @return a new Path
     */
    private Path file2path(final String filename) {
        return file2path(new File(filename));
    }
    
    /**
     * Utility for converting a {@link File} into a {@link Path}.
     *
     * @param file
     *            the File to convert.
     * @return a new Path
     */
    private Path file2path(final File file) {
        return new Path(file.toURI().toString());
    }
    
    /** @return the new name for the "current output file". */
    private String outFilename() {
        final String dateTime = DateHelper.formatToTimeExactToSeconds(new Date());
        final String hex = Integer.toHexString(instance) + Long.toHexString(rand.nextLong());
        
        return String.format("%s%s%s_%s_%s_%s.seq", localWorkDir, File.separator, dataType, dateTime, hostName, hex);
    }
    
}
