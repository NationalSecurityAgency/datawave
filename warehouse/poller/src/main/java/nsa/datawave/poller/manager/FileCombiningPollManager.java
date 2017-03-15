package nsa.datawave.poller.manager;

import nsa.datawave.common.cl.OptionBuilder;
import nsa.datawave.common.io.Files;
import nsa.datawave.ingest.util.io.GzipDetectionUtil;
import nsa.datawave.poller.filter.CompletedFileFilter;
import nsa.datawave.poller.manager.io.SecurityMarkingLoadException;
import nsa.datawave.poller.util.PollerUtils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.CountingOutputStream;
import org.apache.commons.lang.math.JVMRandom;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.NDC;
import org.sadun.util.polling.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetAddress;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.google.common.collect.Sets;

import static com.google.common.base.Predicates.equalTo;
import static com.google.common.base.Predicates.not;

/**
 * Implementation of poll manager that combines input files into compressed files of BLOCK_SIZE size and then copies them into HDFS. The gzip output file is
 * created in the workDir directory and then moved to the hdfsDestDir directory. If successful, the gzip file is workDir is removed and all contributors to the
 * gzip file are removed from the queue directory. If this poll manager is stopped and a gzip output file is currently in progress, it will finish the gzip
 * output file and place it in the completed directory.
 */
public class FileCombiningPollManager extends ConfiguredPollManager implements RecoverablePollManager {
    private static final Logger log = LoggerFactory.getLogger(FileCombiningPollManager.class);
    protected static final AtomicInteger instanceCounter = new AtomicInteger(0);
    // all managers are initialized in the same thread, so we don't need to push datatype more than once
    private static final AtomicBoolean dataTypePushed = new AtomicBoolean(false);
    
    protected static final int MB = 1024 * 1024;
    private static final int BUF_SIZE = 1024 * 8;
    private static final long DEFAULT_BLOCK_SIZE = 64L * MB;
    
    private static final String A_OPT = "a";
    private static final String AR_OPT = "ar";
    private static final String B_OPT = "b";
    private static final String E_OPT = "e";
    private static final String H_OPT = "h";
    private static final String L_OPT = "l";
    private static final String MF_OPT = "mf";
    private static final String MFL_OPT = "mfl";
    private static final String R_OPT = "r";
    private static final String T_OPT = "t";
    private static final String W_OPT = "w";
    
    private final LinkedList<Double> compressionFactor = new LinkedList<>();
    private final ScheduledThreadPoolExecutor timer = new ScheduledThreadPoolExecutor(1);
    private final AtomicBoolean checkedCompressed = new AtomicBoolean();
    private final Semaphore sem = new Semaphore(1);
    
    protected final Map<File,File> currentFileContributors = new HashMap<>();
    protected final JVMRandom rand = new JVMRandom();
    
    protected CountingOutputStream counting;
    protected OutputStream out;
    protected Path hdfsTempDir;
    protected FileSystem fs;
    protected File currentInputDirectory;
    protected File currentOutGzipFile;
    // this will hold the list of output gzip files used to process the current work file
    protected final List<String> outGzipFiles = new ArrayList<String>();
    
    protected String hostName;
    protected String localWorkDir;
    protected String localErrorDir;
    protected String localArchiveDir;
    protected String hdfsDestDir;
    protected String dataType;
    protected String completeDir;
    
    volatile protected long currentOutGzipFileDate = -1;
    protected long startTime;
    protected long bytesRead;
    protected File stillWorkingFile = null;
    protected long maxLatency;
    
    protected final int instance;
    protected long blockSizeBytes = DEFAULT_BLOCK_SIZE;
    protected int maxMerge;
    
    protected volatile boolean closed;
    protected volatile boolean processingFile;
    protected boolean archiveRaw;
    protected boolean compressedInput;
    private boolean configured;
    
    private int compressionFailures;
    private int outputFailures;
    /* package */int maxFailures = 10;
    private volatile boolean isTerminating = false;
    
    private class NewFileTimerTask implements Runnable {
        private DirectoryPoller poller;
        
        public NewFileTimerTask(DirectoryPoller poller) {
            this.poller = poller;
        }
        
        public void run() {
            if (!processingFile && out != null && needNewFile(null) && sem.tryAcquire()) {
                try {
                    if (instanceCounter.intValue() > 1)
                        NDC.push("Manager #" + instance);
                    log.info("Latency timer expired, closing current file.");
                    
                    finishCurrentFile(false);
                    // clear out the captured list of output files
                    outGzipFiles.clear();
                    log.info("Reset set of output files");
                } catch (Exception e) {
                    // we need to terminate the poller
                    log.error("Failed while closing current sequence file", e);
                    poller.shutdown();
                } finally {
                    sem.release();
                    if (instanceCounter.intValue() > 1)
                        NDC.pop();
                }
            }
            
            timer.schedule(this, maxLatency, TimeUnit.MILLISECONDS);
        }
    }
    
    /**
     * Initializes FileCombiningPollManager, setting a unique instance number for the JVM.
     */
    public FileCombiningPollManager() {
        instance = instanceCounter.incrementAndGet();
    }
    
    /**
     * @return configuration options for the FileCombiningPollManager
     */
    public Options getConfigurationOptions() {
        final Options opt = super.getConfigurationOptions();
        final OptionBuilder builder = new OptionBuilder();
        opt.addOption(builder.create(AR_OPT, "archiveRaw", "if supplied, files are archived to the specified archive directory"));
        
        builder.args = 1;
        builder.type = Integer.class;
        opt.addOption(builder.create(B_OPT, "blockSizeMB", "the block size in MB (default is to use hadoop configuration)"));
        opt.addOption(builder.create(MF_OPT, "maxFilesToMerge", "the max number of files to merge"));
        opt.addOption(builder.create(MFL_OPT, "maxFailures", "the maximum number of failures the Poller can take before self-terminating."));
        
        builder.type = String.class;
        opt.addOption(builder.create(A_OPT, "archiveDir", "directory for archived raw files if the archiveRaw flag is supplied"));
        
        builder.required = true;
        opt.addOption(builder.create(T_OPT, "datatype", "Datatype"));
        opt.addOption(builder.create(H_OPT, "hdfsDestDir", "Base destination directory in HDFS"));
        opt.addOption(builder.create(E_OPT, "errorDir", "Local error directory"));
        opt.addOption(builder.create(W_OPT, "workDir", "local work directory"));
        opt.addOption(builder.create(R_OPT, "completedDir", "directory for completed files that have not been copied into HDFS"));
        
        builder.type = Long.class;
        opt.addOption(builder.create(L_OPT, "latency", "ms to wait before closing the output file"));
        
        configured = true;
        return opt;
    }
    
    /**
     * Configures the instance with CommandLine Options
     *
     * @param cl
     *            the command-line to "parse"
     * @throws Exception
     */
    @Override
    public void configure(final CommandLine cl) throws Exception {
        super.configure(cl);
        if (!configured)
            throw new IllegalStateException("datatype is null, please call getConfigurationOptions()");
        if (cl == null)
            throw new IllegalArgumentException("command line cannot be null");
        
        hostName = InetAddress.getLocalHost().getHostName();
        archiveRaw = cl.hasOption(AR_OPT);
        
        completeDir = getLastOptionValue(cl, R_OPT);
        dataType = getLastOptionValue(cl, T_OPT);
        hdfsDestDir = getLastOptionValue(cl, H_OPT);
        localArchiveDir = getLastOptionValue(cl, A_OPT);
        localErrorDir = getLastOptionValue(cl, E_OPT);
        localWorkDir = getLastOptionValue(cl, W_OPT);
        
        hdfsTempDir = new Path(hdfsDestDir + "/temp");
        
        maxLatency = Long.parseLong(getLastOptionValue(cl, L_OPT));
        maxMerge = Integer.parseInt(getLastOptionValue(cl, MF_OPT, "0"));
        maxFailures = Integer.parseInt(getLastOptionValue(cl, MFL_OPT, "" + maxFailures));
        
        if (!dataTypePushed.getAndSet(true))
            NDC.push(dataType);
        if (instanceCounter.intValue() > 1)
            NDC.push("Manager #" + instance);
        
        setupFilesystem(cl);
        logConfig();
        
        Files.ensureDir(completeDir, true);
        Files.ensureDir(localErrorDir, true);
        Files.ensureDir(localWorkDir, true);
        
        if (archiveRaw) {
            if (localArchiveDir == null)
                throw new RuntimeException("Specified that raw files are to be archived, but not archive directory supplied");
            Files.ensureDir(localArchiveDir, true);
        }
        
        sendCompleted();
        reset();
        
        if (instanceCounter.intValue() > 1)
            NDC.pop();
    }
    
    @Override
    public void cycleEnded(final CycleEndEvent event) {
        if (instanceCounter.intValue() > 1)
            NDC.push("Manager #" + instance);
        log.trace("Poll cycle ended");
        
        if (out != null && needNewFile(null) && sem.tryAcquire()) {
            try {
                finishCurrentFile(false);
            } catch (Exception e) {
                throw new RuntimeException("Failure while ending poller cycle", e);
            } finally {
                sem.release();
            }
        }
        
        if (timer.getQueue().size() == 0) {
            log.info("Submitting timer task to close file in {}ms", maxLatency);
            timer.schedule(new NewFileTimerTask(event.getPoller()), maxLatency, TimeUnit.MILLISECONDS);
        }
        
        if (instanceCounter.intValue() > 1)
            NDC.pop();
    }
    
    /**
     * This method will safely close the current output file (if needed) and open a new one. It is assumed this is being called on an input file boundary.
     */
    public void cycleOutputFileIfNeeded(File workFile) throws IOException {
        if (out == null || needNewFile(workFile)) {
            outGzipFiles.clear();
            cycleOutputFile(workFile);
        } else {
            currentFileContributors.put(workFile, currentInputDirectory);
        }
    }
    
    /**
     * This method will safely close the current output file and open a new one. This method can be called on or off an input file boundary.
     *
     * @param workFile
     * @throws IOException
     */
    public void cycleOutputFile(File workFile) throws IOException {
        if (out != null) {
            finishCurrentFile(false);
        }
        if (!retryingCreateNewOutputFile())
            throw new IOException("Error setting up new output file, see log");
        currentFileContributors.put(workFile, currentInputDirectory);
    }
    
    @Override
    public void fileFound(final FileFoundEvent event) {
        
        fileFound(event.getFile(), event.getPoller());
    }
    
    protected void fileFound(final File evtFile, final DirectoryPoller poller) {
        final String originalFile = evtFile.getName();
        
        if (isTerminating) {
            log.info("Termination in progress, ignoring new file: " + originalFile);
            return;
        }
        
        try {
            sem.acquire();
        } catch (InterruptedException iEx) {
            throw new RuntimeException(iEx);
        }
        
        try {
            if (instanceCounter.intValue() > 1)
                NDC.push("Manager #" + instance);
            
            final File workFile;
            
            try {
                checkCompression(evtFile);
                workFile = moveToWorkFile(evtFile);
                
                compressionFailures = 0;
            } catch (IOException ioEx) {
                log.warn("IO Error with " + evtFile.getAbsolutePath(), ioEx);
                
                final String msg = Files.mv(evtFile, new File(localErrorDir, originalFile));
                if (msg != null)
                    log.error("Error moving file: {}", msg);
                
                if (++compressionFailures >= maxFailures)
                    terminate("Too many Compression Failures: " + compressionFailures, poller);
                
                return;
            }
            
            // if null was returned, then there is nothing to process...return gracefully
            if (workFile == null) {
                return;
            }
            
            if (poller.isShuttingDown() || closed) {
                final String msg = Files.mv(workFile, evtFile);
                if (msg != null)
                    log.error("Poller shutting down and failed to move file: {}", msg);
                return;
            }
            
            timer.getQueue().clear();
            
            processingFile = true;
            
            try {
                cycleOutputFileIfNeeded(workFile);
                log.info("Processing {} into output file {}", workFile.getName(), currentOutGzipFile.getName());
                
                final boolean succeeded = handleFile(workFile, poller);
                
                if (succeeded) {
                    log.info("Finished processing {} into output file {}", originalFile, outGzipFiles.toString());
                    addReceivedFileReport(originalFile, workFile);
                    addSpawnFileReport(originalFile, workFile, outGzipFiles);
                    
                } else {
                    log.error("Failed processing {} into output file {}", originalFile, currentOutGzipFile.getName());
                    
                    if (poller.isShuttingDown() || closed) {
                        final String msg = Files.mv(workFile, evtFile);
                        if (msg != null)
                            log.error("Poller shutting down and failed to move file: {}", msg);
                    } else {
                        final String msg = Files.mv(workFile, new File(localErrorDir, workFile.getName()));
                        if (msg != null)
                            log.error("Error moving file: {}", msg);
                    }
                }
                // clean up the output files
                outGzipFiles.clear();
                outGzipFiles.add(currentOutGzipFile.getName());
                log.info("Reset set of output files to " + outGzipFiles);
            } finally {
                processingFile = false;
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed while processing " + originalFile, e);
        } finally {
            if (instanceCounter.intValue() > 1)
                NDC.pop();
            sem.release();
        }
    }
    
    /**
     * Recover any file not yet completed in the work queue directory, and clean up the work directory
     */
    @Override
    public void recover(final File queueDir) {
        recoverWorkQueue(queueDir);
        cleanWorkDir();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        if (closed)
            return;
        closed = true;
        
        try {
            sem.acquire();
        } catch (InterruptedException ie) {
            log.error("Interrupted trying to acquire semaphore....continuing to close up anyway");
        }
        
        try {
            timer.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
            timer.shutdown();
            
            if (out != null) {
                finishCurrentFile(true);
                fs.close();
            }
            
            log.info("Close called on manager #" + instance);
            
        } finally {
            writeProvenanceReports();
            super.close();
            sem.release();
        }
    }
    
    /**
     * Determine a work file name and rename the queue file. In the process, a ".gz" will be appended if the file is supposed to be compressed. In addition the
     * work file will be dedupped if another file of the same name exists.
     *
     * @param queuedFile
     *            the queue file to work on
     * @return the work file
     * @throws IOException
     */
    protected File moveToWorkFile(File queuedFile) throws IOException {
        final String dirPattern = "%s/queue%s";
        boolean compressedExt = queuedFile.getName().endsWith(".gz");
        String filename = queuedFile.getName() + (!compressedExt && compressedInput ? ".gz" : "");
        int count = 0;
        File workFile = null;
        
        do {
            do {
                String directory = String.format(dirPattern, localWorkDir, ++count);
                workFile = new File(directory, filename);
                File f = new File(directory);
                if (f.exists() && !f.isDirectory()) {
                    log.error(directory + " exists, but is not a directory");
                    continue;
                } else if (!f.exists()) {
                    f.mkdir();
                }
            } while (workFile.exists());
            
            final String msg = Files.mv(queuedFile, workFile);
            if (msg == null)
                break;
            
            log.error("Could not rename the queued file: {} to: {}, error: {}", queuedFile, workFile, msg);
            
            if (!queuedFile.exists()) {
                return null;
            }
        } while (true);
        
        return workFile;
    }
    
    /**
     * This is the inverse function of moveToWorkFile(queuedFile).
     *
     * @param workFile
     *            work file
     * @return the queued file
     */
    
    /**
     * A convenience method to get the last specified value on the command line for an option instead of the first which CommandLine.getOptionValue returns.
     *
     * @param cl
     *            command-line to "parse"
     * @param opt
     *            option to look for
     * @return The last option value for opt
     */
    protected String getLastOptionValue(CommandLine cl, String opt) {
        return PollerUtils.getLastOptionValue(cl, opt);
    }
    
    /**
     * A convenience method to get the last specified value on the command line for an option instead of the first which CommandLine.getOptionValue returns.
     *
     * @param cl
     *            command-line to "parse"
     * @param opt
     *            option to look for
     * @param dfVal
     *            default value
     * @return The last option value for opt, or dfVal if missing
     */
    protected String getLastOptionValue(CommandLine cl, String opt, String dfVal) {
        return PollerUtils.getLastOptionValue(cl, opt, dfVal);
    }
    
    /**
     * Do something with the file. In this class, it copies byte for byte. This method is called from the fileFound method for every file.
     *
     * @param inputFile
     *            Input File
     * @param outStream
     *            OutputStream
     * @return number of bytes processed
     */
    protected long processFile(File inputFile, OutputStream outStream) throws Exception {
        final InputStream inStream = inputStream(inputFile);
        if (inStream == null)
            return -1;
        try {
            return IOUtils.copyLarge(inStream, outStream);
        } finally {
            IOUtils.closeQuietly(inStream);
        }
    }
    
    /**
     * Finish the current output file.
     *
     * @param closing
     * @throws IOException
     */
    protected void finishCurrentFile(final boolean closing) throws IOException {
        finishCurrentFile(closing, stillWorkingFile);
    }
    
    /**
     * Completes current file and copies into HDFS. If we are closing, then it is possible that this method is being called from a JVM shutdown hook and the JVM
     * is shutting down. In this case we cannot copy the completed file into HDFS because the FileSystem class tries to register a JVM shutdown hook itself
     * which fails because the JVM is shutting down. If closing is true, then we will leave the file where it is.
     *
     * @param closing
     *            : true if we are closing the app
     * @param stillWorkingFile
     *            : a file that has not been completed yet
     */
    protected void finishCurrentFile(final boolean closing, final File stillWorkingFile) throws IOException {
        try {
            if (out != null) {
                out.flush();
                out.close();
            }
        } catch (IOException e) {
            if (!currentOutGzipFile.delete()) {
                final File errorDirFile = new File(localErrorDir, currentOutGzipFile.getName());
                final String msg = Files.mv(currentOutGzipFile, errorDirFile);
                
                if (msg == null)
                    log.info("Unable to delete partially completed file, placed in error directory: {}", currentOutGzipFile.getName());
                else
                    log.error("Unable to delete or rename completed input file: {}", msg);
            }
            
            reinputContributors(currentFileContributors.keySet());
            
            log.error("Unable to close gzip output file properly, deleted file {} and moved contributors to input directory",
                            currentOutGzipFile.getAbsolutePath());
            
            reset();
            throw new IOException("Unable to close the current gzip file", e);
        } finally {
            IOUtils.closeQuietly(out);
            out = null;
        }
        
        if (counting != null)
            handleCounting(closing, stillWorkingFile);
        reset();
    }
    
    /**
     * Copies completed gzip file to HDFS.
     *
     * @param src
     *            hdfs source Path
     * @param dst
     *            hdfs destination Path
     * @throws IOException
     *             should anything unexpected happen
     */
    /* package */void copyFileToHDFS(final Path src, final Path dst) throws IOException {
        if (!fs.exists(dst.getParent()) && !fs.mkdirs(dst.getParent()))
            throw new IOException("Error creating directory path: " + dst.getParent().toString());
        
        try {
            final Path tmpPath = new Path(hdfsTempDir, dst.getName());
            log.info("Copying file {} to HDFS temp dir", src.toString());
            
            // using a create a IOUtils.copyBytes instead of fs.copyFromLocalFile so that we can force the destination block size and replication
            int bufferSize = fs.getConf().getInt("io.file.buffer.size", BUF_SIZE);
            short replication = (short) (fs.getConf().getInt("dfs.replication", fs.getDefaultReplication(tmpPath)));
            long blockSize = fs.getConf().getLong("dfs.blocksize", fs.getDefaultBlockSize(tmpPath));
            InputStream in = null;
            OutputStream out = null;
            try {
                FileSystem srcFS = fs.getLocal(fs.getConf());
                in = srcFS.open(src);
                FileSystem dstFS = fs;
                out = dstFS.create(tmpPath, true, bufferSize, replication, blockSize);
                org.apache.hadoop.io.IOUtils.copyBytes(in, out, bufferSize, true);
            } catch (IOException e) {
                org.apache.hadoop.io.IOUtils.closeStream(out);
                org.apache.hadoop.io.IOUtils.closeStream(in);
                throw e;
            }
            
            /*** verify the file was uploaded correctly ***/
            final FileStatus[] status = fs.listStatus(tmpPath);
            
            if (status == null || status.length == 0 || status.length > 1) {
                log.error("Got an unexpected file status after uploading to HDFS, status.length = {}", status == null ? 0 : status.length);
                throw new IOException("Got an unexpected file status after uploading to HDFS, status.length = " + (status == null ? 0 : status.length));
            }
            
            /** verify the file length **/
            final long expectedLen = new File(src.toUri()).length();
            final long hdfsLen = status[0].getLen();
            
            if (hdfsLen != expectedLen) {
                final String msg = String.format("Got an unexpected file length after uploading to HDFS: expected %s but got %s", expectedLen, hdfsLen);
                log.error(msg);
                throw new IOException(msg);
            } else {
                log.info("Verified {} bytes of {} uploaded to {}", hdfsLen, src.toString(), hdfsTempDir);
            }
            
            if (!fs.rename(tmpPath, dst)) {
                final String msg = String.format("Completed gzip file remains in temp directory: %s", tmpPath.toString());
                log.error(msg);
                throw new IOException(msg);
            }
            
            if (currentOutGzipFile != null && !currentOutGzipFile.delete()) {
                log.error("Unable to delete the gzip file from local disk: {}", currentOutGzipFile.getAbsolutePath());
            } else {
                // remove the crc file if any
                File file = new File(currentOutGzipFile.getParent() + File.separatorChar + '.' + currentOutGzipFile.getName() + ".crc");
                if (file.exists()) {
                    file.delete();
                }
            }
        } catch (IOException e) {
            throw new IOException("Error copying local gzip file to HDFS", e);
        }
    }
    
    protected boolean retryingCreateNewOutputFile() {
        boolean success = false;
        
        for (int i = 0; i < 10; i++) {
            try {
                createNewOutputFile();
                success = true;
                break;
            } catch (IOException e) {
                log.error("Error setting up new output file", e);
                
                try {
                    Thread.sleep(250l);
                } catch (InterruptedException e1) {
                    log.trace("Ignored error", e1);
                }
            }
        }
        
        return success;
    }
    
    /**
     * This method is used to remove the current work file as contributing to the current output file, and will dispose of the file. This may be used when the
     * file is deemed empty.
     */
    protected void completeContributingFile(File workFile) {
        cleanupContributor(workFile);
        // remove this file from the current list of contributors
        currentFileContributors.remove(workFile);
        // and remove the current output file as being used by this file
        outGzipFiles.remove(currentOutGzipFile.getName());
        log.info("Reduced set of output files to " + outGzipFiles);
    }
    
    /**
     * Sets up new gzip output file in the local work directory.
     */
    protected void createNewOutputFile() throws IOException {
        currentOutGzipFile = new File(localWorkDir, outFilename(System.currentTimeMillis()));
        log.info("Creating new output file " + currentOutGzipFile.getName());
        
        startTime = System.currentTimeMillis();
        currentFileContributors.clear();
        currentOutGzipFileDate = -1;
        bytesRead = 0;
        
        try {
            counting = new CountingOutputStream(new FileOutputStream(currentOutGzipFile));
            out = new GZIPOutputStream(counting);
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
     *            the next input file
     * @return whether or not a new output file is needed
     */
    protected boolean needNewFile(final File nextFile) {
        return checkMaxMerge(nextFile) || checkTimeout() || checkBlockBytes() || checkCompressionBytes(nextFile);
    }
    
    protected static double avgCompRatio(final LinkedList<Double> compRatioList) {
        double sum = 0d;
        for (final double d : compRatioList)
            sum += d;
        return sum / compRatioList.size();
    }
    
    /**
     * Checks the input file to see whether or not it's compressed. The first file to come through here sets the precendent for whether or not subsequent files
     * should be compressed or not.
     *
     * @throws IOException
     *             if given file does not match the expected compression status.
     */
    private void checkCompression(final File evtFile) throws IOException {
        final boolean isCompressed = GzipDetectionUtil.isCompressed(evtFile);
        // ignore files that are of 0 length
        if (evtFile.length() != 0) {
            if (checkedCompressed.compareAndSet(false, true))
                compressedInput = isCompressed;
            
            if (isCompressed != compressedInput) {
                final String expected = compressedInput ? "Compressed" : "Uncompressed";
                final String received = isCompressed ? "Compressed" : "Uncompressed";
                
                throw new IOException(String.format("Expected %s input. Received %s instead.", expected, received));
            }
        }
    }
    
    private void terminate(final String reason, final DirectoryPoller poller) {
        isTerminating = true;
        // release the semaphore so close can be called by the poller
        sem.release();
        
        log.error(reason);
        poller.shutdown();
    }
    
    /**
     * @return true if the maximum number of files to merge has been reached.
     */
    private boolean checkMaxMerge(final File nextFile) {
        // if we have a maxMerge configured, and we are about to process the next file and we already have maxMerge file contributors
        final boolean needNew = maxMerge > 0 && nextFile != null && currentFileContributors.size() >= maxMerge;
        if (needNew)
            log.info("Need new file, have {} file contributors, threshold is {}", currentFileContributors.size(), maxMerge);
        
        return needNew;
    }
    
    /**
     * @return true if "maxLatency" of time has elapsed.
     */
    private boolean checkTimeout() {
        final boolean needNew = System.currentTimeMillis() > (startTime + maxLatency);
        if (needNew)
            log.info("Need new file, passed latency threshold of {}", maxLatency);
        
        return needNew;
    }
    
    /**
     * @return true if we are currently above "blockSizeBytes".
     */
    private boolean checkBlockBytes() {
        final boolean needNew = counting.getByteCount() >= blockSizeBytes;
        if (needNew)
            log.info("Need new file, passed block size threshold of {}", blockSizeBytes);
        
        return needNew;
    }
    
    /**
     * @return true if the next file would take us over "blockSizeBytes"
     */
    protected boolean checkCompressionBytes(final File nextFile) {
        if (nextFile == null || compressionFactor.isEmpty())
            return false;
        
        final double compressionFactorAvg = avgCompRatio(compressionFactor);
        final double approxCompressedBytes = nextFile.length() * compressionFactorAvg;
        
        final boolean needNew = (approxCompressedBytes + counting.getByteCount()) > blockSizeBytes;
        
        if (needNew)
            log.info("Need new file, current output is {} bytes, next file size is {} bytes, threshold is {}", counting.getByteCount(), approxCompressedBytes,
                            blockSizeBytes);
        
        return needNew;
    }
    
    protected void setupFilesystem(final CommandLine cl) throws IOException {
        final Configuration conf = new Configuration(true);
        
        if (cl.hasOption(B_OPT)) {
            blockSizeBytes = MB * Integer.parseInt(getLastOptionValue(cl, B_OPT));
            conf.setLong("dfs.blocksize", blockSizeBytes);
            
            log.info("The block size was specified to be {}m", blockSizeBytes / MB);
        } else if (conf.get("dfs.blocksize") != null) {
            blockSizeBytes = conf.getLong("dfs.blocksize", DEFAULT_BLOCK_SIZE);
            log.info("Using the hadoop default block size of {}m", blockSizeBytes / MB);
        } else {
            log.warn("Could not find the configured block size for HDFS, using the hardcoded default of {}m", blockSizeBytes / MB);
        }
        
        conf.set("fs.defaultFS", hdfsDestDir);
        fs = FileSystem.get(conf);
        log.info("HDFS FileSystem configured: {}", fs.toString());
        
        if (!(fs.exists(hdfsTempDir) || fs.mkdirs(hdfsTempDir)))
            throw new RuntimeException("Unable to make temp directory " + hdfsTempDir);
    }
    
    private void logConfig() {
        if (!log.isInfoEnabled())
            return;
        
        log.info("PollManager Configuration - DataType: {}, hdfsDestDir: {}, hdfsTempDir: {}, error dir: {}, work dir: {}, completed dir: {},"
                        + "latency (ms): {}, blockSize (mb): {}, compressed input: {}, max files to merge (<=0 means no max): {}, archive raw data: {}",
                        dataType, hdfsDestDir, hdfsTempDir, localErrorDir, localWorkDir, completeDir, maxLatency, blockSizeBytes / MB, compressedInput,
                        maxMerge, archiveRaw);
    }
    
    /**
     * Copies any completes files to HDFS.
     *
     * @throws IOException
     */
    private void sendCompleted() throws IOException {
        for (final File f : new File(completeDir).listFiles(new CompletedFileFilter())) {
            log.info("Found completed file: {}", f.getAbsolutePath());
            
            final int underscore = f.getName().indexOf("_");
            
            final String datatypeName = f.getName().substring(0, underscore);
            final String year = f.getName().substring(underscore + 1, underscore + 5);
            final String month = f.getName().substring(underscore + 5, underscore + 7);
            final String day = f.getName().substring(underscore + 7, underscore + 9);
            
            final Path src = new Path(f.toURI().toString());
            final Path dst = new Path(String.format("%s/%s/%s/%s/%s/%s", hdfsDestDir, datatypeName, year, month, day, f.getName()));
            
            currentOutGzipFile = f;
            currentOutGzipFileDate = -1;
            copyFileToHDFS(src, dst);
        }
    }
    
    /**
     * Resets the FileCombiningPollManager's state; preparing it for the next output file.
     */
    protected void reset() {
        currentOutGzipFileDate = -1;
        currentOutGzipFile = null;
        counting = null;
        out = null;
    }
    
    private void recoverWorkQueue(final File queueDir) {
        int count = 0;
        File workQueue = new File(this.localWorkDir, "queue" + (++count));
        while (workQueue.exists()) {
            int succ = 0;
            int fail = 0;
            
            final File[] files = workQueue.listFiles();
            if (files != null) {
                for (final File workFile : files) {
                    final File queueFile = new File(queueDir, workFile.getName());
                    final String msg = Files.mv(workFile, queueFile);
                    
                    if (msg == null) {
                        succ++;
                    } else {
                        fail++;
                        log.error("Unable to move old queue file {} back into {}", workFile, queueDir);
                    }
                }
                
                if (succ > 0)
                    log.info("Cleaned up {} files left in work queue directory {}", succ, workQueue);
                if (fail > 0)
                    log.error("Failed to clean up {} files left in work queue directory {}", workQueue);
            }
            workQueue = new File(this.localWorkDir, "queue" + (++count));
        }
    }
    
    private void cleanWorkDir() {
        final File workDirFile = new File(this.localWorkDir);
        int succ = 0;
        int fail = 0;
        
        final File[] files = workDirFile.listFiles();
        if (files == null)
            return;
        
        for (final File workFile : files) {
            if (workFile.isFile()) {
                if (!workFile.delete()) {
                    fail++;
                    log.error("Unable to remove old work file {}", workFile);
                } else {
                    succ++;
                }
            }
        }
        
        if (succ > 0)
            log.info("Cleaned up {} files left in work directory {}", succ, workDirFile);
        if (fail > 0)
            log.info("Failed to clean up {} files left in work directory {}", fail, workDirFile);
    }
    
    /**
     * This will close out the current sequence file, and move it to hdfs. In addition the current contributors will be cleaned up.
     *
     * @param closing
     *            True if the app is closing in which case we skip uploading to hdfs
     * @param stillWorkingFile
     *            A file we are still reading and hence should not be cleaned up in the current contributors list
     */
    private void handleCounting(final boolean closing, final File stillWorkingFile) {
        final long bytesWritten = counting.getByteCount();
        
        if (bytesWritten > 0) {
            updateStats(bytesWritten);
            
            if (closing) {
                if (!currentOutGzipFile.renameTo(new File(completeDir, currentOutGzipFile.getName())))
                    log.error("Error moving completed gzip output file to completed directory: {}", currentOutGzipFile.getAbsolutePath());
            } else
                sendToHdfs();
        } else {
            log.info("Did not write any bytes out of {} to {} and hence dropping.", bytesRead, currentOutGzipFile.getAbsolutePath());
            if (!currentOutGzipFile.delete()) {
                log.trace("Failed to delete: {}", currentOutGzipFile);
            }
        }
        
        cleanupContributors(stillWorkingFile);
    }
    
    private void updateStats(final long bytesWritten) {
        final double currentCompressionFactor = bytesWritten / (double) bytesRead;
        
        log.info("Wrote {} out of {} to {} with a compression factory of {}", bytesWritten, bytesRead, currentOutGzipFile.getAbsolutePath(),
                        currentCompressionFactor);
        
        if (compressionFactor.size() > 100)
            compressionFactor.removeFirst();
        compressionFactor.addLast(currentCompressionFactor);
    }
    
    private void sendToHdfs() {
        final String[] components = currentOutGzipFile.getName().split("_");
        final String dtName = components[0];
        final String dateStr = finishedDateDir(components[1]);
        
        final Path src = new Path(currentOutGzipFile.toURI().toString());
        final Path dst = new Path(String.format("%s/%s/%s/%s", hdfsDestDir, dtName, dateStr, currentOutGzipFile.getName()));
        
        try {
            copyFileToHDFS(src, dst);
        } catch (IOException e) {
            log.warn("Error copying local gzip file to HDFS, moving file to completed directory to be picked up next round", e);
            
            if (!currentOutGzipFile.renameTo(new File(completeDir, currentOutGzipFile.getName())))
                log.error("Error moving completed gzip output file to completed directory: {}", currentOutGzipFile.getAbsolutePath());
        }
    }
    
    private String finishedDateDir(final String origDate) {
        final SimpleDateFormat origFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        final SimpleDateFormat destFormat = new SimpleDateFormat("yyyy/MM/dd");
        final Date time;
        
        if (currentOutGzipFileDate > 0) {
            time = new Date(currentOutGzipFileDate);
        } else {
            try {
                time = origFormat.parse(origDate);
            } catch (ParseException pEx) {
                throw new RuntimeException(pEx);
            }
        }
        
        return destFormat.format(time);
    }
    
    /**
     * @return the new name for the "current output file".
     */
    private String outFilename(final long time) {
        final String dateTime = (new SimpleDateFormat("yyyyMMddHHmmss")).format(new Date(time));
        final String hex = Integer.toHexString(instance) + Long.toHexString(rand.nextLong());
        
        return String.format("%s_%s_%s_%s.seq", dataType, dateTime, hostName, hex);
    }
    
    private void reinputContributors(final Set<File> files) {
        for (final File file : files) {
            String msg = Files.mv(file, new File(currentFileContributors.get(file), file.getName()));
            
            if (msg != null) {
                log.error("Unable to rename completed input file: {}", msg);
                
                msg = Files.mv(file, new File(localErrorDir, file.getName()));
                if (msg != null)
                    log.error("Unable to rename completed input file to error dir: {}", msg);
            }
        }
    }
    
    /**
     * Cleanup the current output file contributors except for the stillWorkingFile if specified
     *
     * @param stillWorkingFile
     */
    private void cleanupContributors(File stillWorkingFile) {
        for (final File f : currentFileContributors.keySet()) {
            if (stillWorkingFile == null || !f.equals(stillWorkingFile)) {
                cleanupContributor(f);
            }
        }
    }
    
    private void cleanupContributor(File f) {
        if (archiveRaw) {
            final File archiveDirFile = new File(localArchiveDir, f.getName());
            final String msg = Files.mv(f, archiveDirFile);
            
            if (msg != null)
                log.error("Unable to rename completed input file: {}", msg);
        } else if (!f.delete()) {
            final File errorDirFile = new File(localErrorDir, f.getName() + ".delete");
            final String msg = Files.mv(f, errorDirFile);
            
            if (msg == null)
                log.info("Unable to delete completed file: {} can be deleted", errorDirFile.getAbsolutePath());
            else
                log.error("Unable to delete or rename completed input file: {}", msg);
        }
    }
    
    private boolean handleFile(final File workFile, final DirectoryPoller poller) {
        boolean success = true;
        
        try {
            final long fileLength = workFile.length();
            bytesRead += fileLength;
            
            final long copiedLength;
            
            stillWorkingFile = workFile;
            try {
                copiedLength = processFile(workFile, out);
            } finally {
                stillWorkingFile = null;
            }
            
            success = checkOutput(poller, copiedLength, fileLength);
        } catch (final SecurityMarkingLoadException clEx) {
            log.error("Error loading configured SecurityMarkings file", clEx);
            reinputContributors(currentFileContributors.keySet());
            
            IOUtils.closeQuietly(out);
            out = null;
            
            if (!currentOutGzipFile.delete())
                log.error("Unable to delete current file: {}", currentOutGzipFile.getAbsolutePath());
            
            terminate(clEx.getMessage(), poller);
            success = false;
        } catch (final Exception e) {
            log.error("Error copying input stream while processing {}", workFile, e);
            reinputContributors(Sets.filter(currentFileContributors.keySet(), not(equalTo(workFile))));
            
            IOUtils.closeQuietly(out);
            out = null;
            
            if (!currentOutGzipFile.delete())
                log.error("Unable to delete current file: {}", currentOutGzipFile.getAbsolutePath());
            
            success = false;
        } catch (final Error err) {
            log.error("Fatal Error copying input stream while processing {}", workFile, err);
            reinputContributors(currentFileContributors.keySet());
            
            IOUtils.closeQuietly(out);
            out = null;
            
            if (!currentOutGzipFile.delete())
                log.error("Unable to delete current file: {}", currentOutGzipFile.getAbsolutePath());
            
            success = false;
            terminate(err.getMessage(), poller);
            // throw the error up the chain to force this poller to terminate
            throw new FileProcessingError(err);
        }
        
        return success;
    }
    
    protected boolean checkOutput(final DirectoryPoller poller, final long cLen, final long fLen) {
        if (cLen < 0) {
            log.error("Unable to process input file");
            return false;
        }
        if ((!compressedInput && (cLen != fLen)) || (cLen == 0 && fLen > 0)) {
            if (++outputFailures >= maxFailures)
                terminate("Too many Zero Output Failures: " + outputFailures, poller);
            
            log.error("Did not copy whole file: copied {} of {}", cLen, fLen);
            return false;
        }
        
        return true;
    }
    
    /**
     * @param workFile
     *            The File to use as the basis for the InputStream.
     * @return a new InputStream
     */
    private InputStream inputStream(final File workFile) {
        InputStream is = null;
        FileInputStream fis = null;
        
        try {
            fis = new FileInputStream(workFile);
            // files of 0 length need to be opened without compression
            // regardless of what we expect the compression to be.
            is = (compressedInput && workFile.length() > 0) ? new GZIPInputStream(fis, BUF_SIZE) : new FileInputStream(workFile);
        } catch (IOException e) {
            IOUtils.closeQuietly(fis);
            IOUtils.closeQuietly(is);
            log.error("Error reading input file", e);
        }
        
        return is;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String getDatatype() {
        return dataType;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void cycleStarted(final CycleStartEvent event) {
        log.trace("Poll cycle started");
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void exceptionDeletingTargetFile(final File file) {
        log.error("Error deleting file: {}", file.getAbsolutePath());
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void exceptionMovingFile(final File src, final File dst) {
        log.error("Error moving file: {} to {}", src.getAbsolutePath(), dst.getAbsolutePath());
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void directoryLookupStarted(final DirectoryLookupStartEvent event) {
        log.trace("directory lookup started");
        currentInputDirectory = event.getDirectory();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void directoryLookupEnded(final DirectoryLookupEndEvent event) {
        log.trace("directory lookup ended");
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void fileMoved(FileMovedEvent event) {
        log.trace("File {} moved to {}", event.getOriginalPath(), event.getPath());
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void fileSetFound(final FileSetFoundEvent fsfEvt) {}
    
}
