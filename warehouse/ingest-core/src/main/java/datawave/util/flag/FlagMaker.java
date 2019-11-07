package datawave.util.flag;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import datawave.ingest.mapreduce.StandaloneTaskAttemptContext;
import datawave.metrics.util.flag.FlagFile;
import datawave.util.flag.config.ConfigUtil;
import datawave.util.flag.config.FlagDataTypeConfig;
import datawave.util.flag.config.FlagMakerConfig;
import datawave.util.flag.processor.DateUtils;
import datawave.util.flag.processor.FlagDistributor;
import datawave.util.flag.processor.SizeValidator;
import datawave.util.flag.processor.UnusableFileException;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.net.Socket;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.DecimalFormat;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

/**
 * 
 */
public class FlagMaker implements Runnable, Observer, SizeValidator {
    
    private static final CompressionCodec cc = new GzipCodec();
    private static final CompressionType ct = CompressionType.BLOCK;
    
    private static final Logger log = LoggerFactory.getLogger(FlagMaker.class);
    // our yyyy/mm/dd pattern for most things.
    public static final Pattern pattern = Pattern.compile(".*/([0-9]{4}(/[0-9]{2}){2})(?:/.*|$)");
    private static final String DATE_FORMAT_STRING = "yyyy" + File.separator + "MM" + File.separator + "dd";
    
    private static final String COUNTER_LIMIT_HADOOP_2 = "mapreduce.job.counters.max";
    private static final String COUNTER_LIMIT_HADOOP_1 = "mapreduce.job.counters.limit";
    private static final int COUNTERS_PER_INPUT_FILE = 2;
    
    /**
     * Directory cache will serve as a place holder the directories in HDFS that were created. This will cut down on the number of RPC calls to the NameNode
     */
    private final Cache<Path,Path> directoryCache;
    // Executor will be used for directory lookups
    private ExecutorService executor;
    private final FlagMakerConfig fmc;
    final FlagDistributor fd;
    private volatile boolean running = true;
    private FlagSocket flagSocket;
    private final DecimalFormat df = new DecimalFormat("#0.00");
    private DateUtils util = new DateUtils();
    private StandaloneTaskAttemptContext<?,?,?,?> ctx;
    
    protected JobConf config;
    
    public FlagMaker(FlagMakerConfig fmconfig) {
        this.fmc = fmconfig;
        this.config = new JobConf(new Configuration());
        
        this.fmc.validate();
        // configure the executor per the FlagMakerConfig input
        this.executor = Executors.newFixedThreadPool(this.fmc.getMaxHdfsThreads());
        this.fd = this.fmc.getFlagDistributor();
        
        // build the cache per the default configuration.
        // @formatter:off
        this.directoryCache = CacheBuilder.newBuilder()
                .maximumSize(fmc.getDirectoryCacheSize())
                .expireAfterWrite(fmc.getDirectoryCacheTimeout(), TimeUnit.MILLISECONDS)
                .concurrencyLevel(fmc.getMaxHdfsThreads())
                .build();
        // @formatter:on
    }
    
    public static void main(String... args) throws Exception {
        FlagMakerConfig flagMakerConfig = getFlagMakerConfig(args);
        
        boolean shutdown = false;
        for (int i = 0; i < args.length; i++) {
            if ("-shutdown".equals(args[i])) {
                shutdown = true;
            }
        }
        
        if (shutdown) {
            shutdown(flagMakerConfig.getSocketPort());
            System.exit(0);
        }
        
        try {
            FlagMaker m = createFlagMaker(flagMakerConfig);
            m.run();
        } catch (IllegalArgumentException ex) {
            System.err.println("" + ex.getMessage());
            printUsage();
            System.exit(1);
        }
        
    }
    
    private static FlagMaker createFlagMaker(FlagMakerConfig fc) {
        try {
            Class<? extends FlagMaker> c = (Class<? extends FlagMaker>) Class.forName(fc.getFlagMakerClass());
            Constructor<? extends FlagMaker> constructor = c.getConstructor(FlagMakerConfig.class);
            return constructor.newInstance(fc);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Subclasses of FlagMaker must implement a constructor that takes a FlagMakerConfig", e);
        } catch (Exception e) {
            throw new RuntimeException("Failed to instantiate FlagMaker of type " + fc.getFlagMakerClass(), e);
        }
    }
    
    static FlagMakerConfig getFlagMakerConfig(String[] args) throws JAXBException, IOException {
        String flagConfig = null;
        String baseHDFSDirOverride = null;
        String extraIngestArgsOverride = null;
        String flagFileDirectoryOverride = null;
        String flagMakerClass = null;
        for (int i = 0; i < args.length; i++) {
            if ("-flagConfig".equals(args[i])) {
                flagConfig = args[++i];
                log.info("Using flagConfig of {}", flagConfig);
            } else if ("-baseHDFSDirOverride".equals(args[i])) {
                baseHDFSDirOverride = args[++i];
                log.info("Will override baseHDFSDir with {}", baseHDFSDirOverride);
            } else if ("-extraIngestArgsOverride".equals(args[i])) {
                extraIngestArgsOverride = args[++i];
                log.info("Will override extraIngestArgs with {}", extraIngestArgsOverride);
            } else if ("-flagFileDirectoryOverride".equals(args[i])) {
                flagFileDirectoryOverride = args[++i];
                log.info("Will override flagFileDirectory with {}", flagFileDirectoryOverride);
            } else if ("-flagMakerClass".equals(args[i])) {
                flagMakerClass = args[++i];
                log.info("will override flagMakerClass with {}", flagMakerClass);
            }
        }
        if (flagConfig == null) {
            flagConfig = "FlagMakerConfig.xml";
            log.warn("No flag config file specified, attempting to use default file: {}", flagConfig);
        }
        
        FlagMakerConfig xmlObject = ConfigUtil.getXmlObject(FlagMakerConfig.class, flagConfig);
        if (null != baseHDFSDirOverride) {
            xmlObject.setBaseHDFSDir(baseHDFSDirOverride);
        }
        if (null != flagFileDirectoryOverride) {
            xmlObject.setFlagFileDirectory(flagFileDirectoryOverride);
        }
        if (null != extraIngestArgsOverride) {
            for (FlagDataTypeConfig flagDataTypeConfig : xmlObject.getFlagConfigs()) {
                flagDataTypeConfig.setExtraIngestArgs(extraIngestArgsOverride);
            }
            xmlObject.getDefaultCfg().setExtraIngestArgs(extraIngestArgsOverride);
        }
        if (null != flagMakerClass) {
            xmlObject.setFlagMakerClass(flagMakerClass);
        }
        log.debug(xmlObject.toString());
        return xmlObject;
        
    }
    
    private static void shutdown(int port) throws IOException {
        try (Socket s = new Socket("localhost", port); PrintWriter pw = new PrintWriter(s.getOutputStream(), true)) {
            pw.write("shutdown");
            pw.flush();
        }
    }
    
    private static void printUsage() {
        System.out.println("To run the Flag Maker: ");
        System.out.println("datawave.ingest.flag.FlagMaker -flagConfig [path to xml config]");
        System.out.println("Optional arguments:");
        System.out.println("\t\t-shutdown\tDescription: shuts down the flag maker using configured socketPort");
        System.out.println("\t\t-baseHDFSDirOverride [HDFS Path]\tDescription: overrides baseHDFSDir in xml");
        System.out.println("\t\t-extraIngestArgsOverride [extra ingest args]\tDescription: overrides extraIngestArgs value in xml config");
        System.out.println("\t\t-flagFileDirectoryOverride [local path]\tDescription: overrides flagFileDirectory value in xml config");
    }
    
    @Override
    public void run() {
        log.info(this.getClass().getSimpleName() + " run() starting");
        startSocket();
        try {
            while (running) {
                try {
                    processFlags();
                    Thread.sleep(fmc.getSleepMilliSecs());
                } catch (Exception ex) {
                    log.error("An unexpected exception occurred. Exiting", ex);
                    running = false;
                }
            }
        } finally {
            executor.shutdown();
        }
        log.info(this.getClass().getSimpleName() + " Exiting.");
    }
    
    /**
     * 
     * @throws IOException
     */
    protected void processFlags() throws IOException {
        FileSystem fs = getHadoopFS();
        log.trace("Querying for files on {}", fs.getUri().toString());
        for (FlagDataTypeConfig fc : fmc.getFlagConfigs()) {
            long startTime = System.currentTimeMillis();
            String dataName = fc.getDataName();
            fd.setup(fc);
            log.trace("Checking for files for {}", dataName);
            
            loadFilesForDistributor(fc, fs);
            
            while (fd.hasNext(shouldOnlyCreateFullFlags(fc)) && running) {
                Collection<InputFile> inFiles = fd.next(this);
                if (null == inFiles || inFiles.isEmpty()) {
                    throw new IllegalStateException(fd.getClass().getName()
                                    + " has input files but returned zero candidates for flagging. Please validate configuration");
                }
                writeFlagFile(fc, inFiles);
            }
        }
    }
    
    /**
     * Adds all input files for the data type to the {@link FlagDistributor}.
     * 
     * @param fc
     *            flag datatype configuration data
     * @param fs
     *            hadoop filesystem
     * @throws IOException
     *             error condition finding files in hadoop
     */
    void loadFilesForDistributor(FlagDataTypeConfig fc, FileSystem fs) throws IOException {
        for (String folder : fc.getFolder()) {
            String folderPattern = folder + "/" + fmc.getFilePattern();
            log.debug("searching for " + fc.getDataName() + " files in " + folderPattern);
            FileStatus[] files = fs.globStatus(new Path(folderPattern));
            if (files == null || files.length == 0) {
                continue;
            }
            
            // remove the base directory from the folder
            if (folder.startsWith(this.fmc.getBaseHDFSDir())) {
                folder = folder.substring(this.fmc.getBaseHDFSDir().length());
                if (folder.startsWith(File.separator)) {
                    folder = folder.substring(File.separator.length());
                }
            }
            
            // add the files
            for (FileStatus status : files) {
                if (status.isDirectory()) {
                    log.warn("Skipping subdirectory " + status.getPath());
                } else {
                    try {
                        this.fd.addInputFile(new InputFile(folder, status, this.fmc.getBaseHDFSDir(), this.fmc.isUseFolderTimestamp()));
                        logFileInfo(fc, status);
                    } catch (UnusableFileException e) {
                        log.warn("Skipping unusable file " + status.getPath(), e);
                    }
                }
            }
        }
    }
    
    protected void logFileInfo(FlagDataTypeConfig fc, FileStatus status) {
        log.info("File {} : {}", fc.getDataName(), status);
    }
    
    private boolean shouldOnlyCreateFullFlags(FlagDataTypeConfig fc) {
        return !hasTimeoutOccurred(fc) || isBacklogExcessive(fc);
    }
    
    private boolean isBacklogExcessive(FlagDataTypeConfig fc) {
        if (fc.getFlagCountThreshold() == FlagMakerConfig.UNSET) {
            log.trace("Not evaluating flag file backlog.  getFlagCountThreshold = {}", FlagMakerConfig.UNSET);
            return false;
        }
        int sizeOfFlagFileBacklog = countFlagFileBacklog(fc);
        if (sizeOfFlagFileBacklog >= fc.getFlagCountThreshold()) {
            log.debug("Flag file backlog is excessive: sizeOfFlagFileBacklog: {}, flagCountThreshold: {}", sizeOfFlagFileBacklog, fc.getFlagCountThreshold());
            return true;
        }
        return false;
    }
    
    private boolean hasTimeoutOccurred(FlagDataTypeConfig fc) {
        long now = System.currentTimeMillis();
        // fc.getLast indicates when the flag file creation timeout will occur
        boolean hasTimeoutOccurred = (now >= fc.getLast());
        if (!hasTimeoutOccurred) {
            log.debug("Still waiting for timeout.  now: {}, last: {}, (now-last): {}", now, fc.getLast(), (now - fc.getLast()));
        }
        return hasTimeoutOccurred;
    }
    
    private long getTimestamp(Path path, long fileTimestamp) {
        if (fmc.isUseFolderTimestamp()) {
            // if using the folder timestamp, then pull the day out of the folder timestamp
            try {
                return util.getFolderTimestamp(path.toString());
            } catch (Exception e) {
                log.warn("Path does not contain yyyy/mm/dd...using file timestamp for {}", path);
                return fileTimestamp;
            }
            
        } else {
            return fileTimestamp;
        }
    }
    
    /**
     * Determine the number of unprocessed flag files in the flag directory
     * 
     * @param fc
     * @return the flag found for this ingest pool
     */
    private int countFlagFileBacklog(final FlagDataTypeConfig fc) {
        final MutableInt fileCounter = new MutableInt(0);
        final FileFilter fileFilter = new WildcardFileFilter("*_" + fc.getIngestPool() + "_" + fc.getDataName() + "_*.flag");
        final FileVisitor<java.nio.file.Path> visitor = new SimpleFileVisitor<java.nio.file.Path>() {
            
            @Override
            public FileVisitResult visitFile(java.nio.file.Path path, BasicFileAttributes attrs) throws IOException {
                if (fileFilter.accept(path.toFile())) {
                    fileCounter.increment();
                }
                return super.visitFile(path, attrs);
            }
        };
        try {
            Files.walkFileTree(Paths.get(fmc.getFlagFileDirectory()), visitor);
        } catch (IOException e) {
            // unable to get a flag count....
            log.error("Unable to get flag file count", e);
            return -1;
        }
        return fileCounter.intValue();
    }
    
    //@formatter:off
    /**
     * Write the flag file. This is done in several steps to ensure we can easily recover if we are killed somewhere in-between.
     *
     * <ul>
     *     <li>move the files to the flagging directory for those that do not already exist in flagging, flagged, or loaded</li>
     *     <li>create the flag.generating file for those files we were able to move, and do not exist elsewhere</li>
     *     <li>set the timestamp of the flag file to that of the most recent file</li>
     *     <li>move the flagging files to the flagged directory</li>
     *     <li>move the generating file to the final flag file form</li>
     *     <li>create the cleanup file</li>
     * </ul>
     *
     * Using these steps, a cleanup of an abnormal termination is as follows:
     * <ul>
     *     <li>move all flagging files to the base directory</li>
     *     <li>for all flag.generating files, move the flagged files to the base directory</li>
     *     <li>remove the flag.generating files.</li>
     * </ul>
     * 
     * @param fc flag configuration
     * @param inFiles input files to write to flag file
     * @throws IOException
     */
    //@formatter:on
    void writeFlagFile(final FlagDataTypeConfig fc, Collection<InputFile> inFiles) throws IOException {
        File flagFile = null;
        final FileSystem fs = getHadoopFS();
        long now = System.currentTimeMillis();
        final FlagMetrics metrics = new FlagMetrics(fs, fc.isCollectMetrics());
        List<Future<InputFile>> futures = Lists.newArrayList();
        
        try {
            // first lets create the dest directories, and move the files into the flagging directory
            final AtomicLong latestTime = new AtomicLong(-1);
            
            for (final InputFile e : inFiles) {
                // Create directories and move to flagging
                final FlagEntryMover mover = new FlagEntryMover(directoryCache, fs, e);
                final Future<InputFile> exec = executor.submit(mover);
                futures.add(exec);
            }
            
            HashSet<InputFile> flagging = new HashSet<>();
            // if no files moved, then abort
            if (!processResults(futures, flagging)) {
                log.warn("No pending files were moved to the flagging directory. Please investigate.");
                return;
            }
            
            Path first = flagging.iterator().next().getCurrentDir();
            String baseName = fmc.getFlagFileDirectory() + File.separator + df.format(now / 1000) + "_" + fc.getIngestPool() + "_" + fc.getDataName() + "_"
                            + first.getName() + "+" + flagging.size();
            flagFile = write(flagging, fc, baseName);
            for (InputFile entry : flagging) {
                long time = entry.getTimestamp();
                metrics.updateCounter(InputFile.class.getSimpleName(), entry.getFileName(), time);
                latestTime.set(Math.max(entry.getTimestamp(), latestTime.get()));
            }
            
            // now set the modification time of the flag file
            if (fmc.isSetFlagFileTimestamp()) {
                if (!flagFile.setLastModified(latestTime.get())) {
                    log.warn("unable to set last modified time for flagfile (" + flagFile.getAbsolutePath() + ")");
                }
            }
            
            // Now we have files moved into the flagging directory, and a flag.generating file created
            
            // move the files to the flagged directory
            for (InputFile entry : flagging) {
                final SimpleMover mover = new SimpleMover(directoryCache, entry, InputFile.TrackedDir.FLAGGED_DIR, fs);
                final Future<InputFile> exec = executor.submit(mover);
                futures.add(exec);
            }
            HashSet<InputFile> flagged = new HashSet<>();
            if (!processResults(futures, flagged)) {
                throw new IOException("Files went to flagging, but not flagged. Investigate");
            }
            
            for (InputFile entry : flagged) {
                metrics.updateCounter(FlagFile.class.getSimpleName(), entry.getCurrentDir().getName(), System.currentTimeMillis());
            }
            
            File f2 = new File(baseName + ".flag");
            if (!flagFile.renameTo(f2)) {
                throw new IOException("Failed to rename" + flagFile.toString() + " to " + f2);
            }
            flagFile = f2;
            
            // after we write a file, set the timeout to the forceInterval
            fc.setLast(now + fc.getTimeoutMilliSecs());
            
            metrics.writeMetrics(this.fmc.getFlagMetricsDirectory(), new Path(baseName).getName());
        } catch (IOException ex) {
            log.error("Unable to complete flag file ", ex);
            moveFilesBack(inFiles, futures, fs);
            if (flagFile != null) {
                if (!flagFile.delete()) {
                    log.warn("unable to delete flagfile (" + flagFile.getAbsolutePath() + ")");
                }
            }
            throw ex;
        }
    }
    
    /**
     * Creates the flag file using all of the valid ingest files.
     * 
     * @param flagging
     *            ingest files
     * @param fc
     *            data type for ingest
     * @param baseName
     *            base name for flag file
     * @return handle for flag file
     * @throws IOException
     *             error creating flag file
     */
    protected File write(Collection<InputFile> flagging, FlagDataTypeConfig fc, String baseName) throws IOException {
        // create the flag.generating file
        log.debug("Creating flag file" + baseName + ".flag" + " for data type " + fc.getDataName() + " containing " + flagging.size() + " files");
        File f = new File(baseName + ".flag.generating");
        if (!f.createNewFile()) {
            throw new IOException("Unable to create flag file " + f);
        }
        
        try (FileOutputStream flagOS = new FileOutputStream(f)) {
            StringBuilder sb = new StringBuilder(fmc.getDatawaveHome() + File.separator + fc.getScript());
            if (fc.getFileListMarker() == null) {
                String sep = " ";
                for (InputFile inFile : flagging) {
                    if (fc.isCollectMetrics())
                        ctx.getCounter(InputFile.class.getSimpleName(), inFile.getFileName()).setValue(inFile.getTimestamp());
                    sb.append(sep).append(inFile.getFlagged().toUri());
                    sep = ",";
                }
            } else {
                // put a variable in here instead which will resolve to the flag file inprogress when that time comes. The baseName could have changed by then.
                sb.append(" ${JOB_FILE}");
            }
            sb.append(" ").append(fc.getReducers()).append(" -inputFormat ").append(fc.getInputFormat().getName()).append(" ");
            if (fc.getFileListMarker() != null) {
                sb.append("-inputFileLists -inputFileListMarker ").append(fc.getFileListMarker()).append(" ");
            }
            if (fc.getExtraIngestArgs() != null) {
                sb.append(fc.getExtraIngestArgs());
            }
            sb.append("\n");
            if (fc.getFileListMarker() != null) {
                sb.append(fc.getFileListMarker()).append('\n');
                for (InputFile inFile : flagging) {
                    if (fc.isCollectMetrics())
                        ctx.getCounter(InputFile.class.getSimpleName(), inFile.getFileName()).setValue(inFile.getTimestamp());
                    sb.append(inFile.getFlagged().toUri()).append('\n');
                }
            }
            
            flagOS.write(sb.toString().getBytes());
        }
        
        return f;
    }
    
    private boolean processResults(List<Future<InputFile>> futures, Collection<InputFile> entries) throws IOException {
        IOException ioex = null;
        for (Future<InputFile> future : futures) {
            try {
                InputFile fe = future.get();
                if (fe != null && fe.isMoved()) {
                    entries.add(fe);
                }
            } catch (InterruptedException | ExecutionException ex) {
                ioex = new IOException("Failure during move", ex.getCause());
            }
        }
        futures.clear();
        if (ioex != null) {
            throw ioex;
        }
        return !entries.isEmpty();
    }
    
    private void moveFilesBack(Collection<InputFile> files, List<Future<InputFile>> futures, FileSystem fs) throws IOException {
        if (files.isEmpty()) {
            return;
        }
        for (InputFile flagEntry : files) {
            final SimpleMover mover = new SimpleMover(directoryCache, flagEntry, InputFile.TrackedDir.PATH_DIR, fs);
            final Future<InputFile> exec = executor.submit(mover);
            futures.add(exec);
        }
        HashSet<InputFile> moved = new HashSet<>();
        processResults(futures, moved);
        if (moved.size() != files.size()) {
            StringBuilder sb = new StringBuilder();
            for (InputFile entry : files) {
                if (!entry.getPath().equals(entry.getCurrentDir())) {
                    sb.append("\n").append(entry.getCurrentDir().toString());
                }
            }
            log.error("An error occurred while attempting to move files. The following files were orphaned:" + sb.toString());
        }
    }
    
    FileSystem getHadoopFS() throws IOException {
        Configuration hadoopConfiguration = new Configuration();
        hadoopConfiguration.set("fs.defaultFS", fmc.getHdfs());
        try {
            return FileSystem.get(hadoopConfiguration);
        } catch (IOException ex) {
            log.error("Unable to connect to HDFS. Exiting");
            throw ex;
        }
    }
    
    @Override
    public void update(Observable o, Object arg) {
        if (flagSocket != o || arg == null) {
            return;
        }
        String s = arg.toString();
        if ("shutdown".equals(s)) {
            running = false;
        }
        if (s.startsWith("kick")) {
            String dtype = s.substring(4).trim();
            for (FlagDataTypeConfig cfg : fmc.getFlagConfigs()) {
                if (cfg.getDataName().equals(dtype)) {
                    log.info("Forcing {} to generate flag file", dtype);
                    cfg.setLast(System.currentTimeMillis() - cfg.getTimeoutMilliSecs());
                    break;
                }
            }
        }
        
    }
    
    private void startSocket() {
        try {
            flagSocket = new FlagSocket(fmc.getSocketPort());
            flagSocket.addObserver(this);
            Thread socketThread = new Thread(flagSocket, "Flag_Socket_Thread");
            socketThread.setDaemon(true);
            socketThread.start();
        } catch (IOException ex) {
            log.error("Error occurred while starting socket. Exiting.", ex);
            running = false;
        }
    }
    
    /**
     * Get the length of the flag file that would be created using this set of files.
     * 
     * @param fc
     * @param inFiles
     * @return The size in characters of the flag file
     */
    private long getFlagFileSize(FlagDataTypeConfig fc, Collection<InputFile> inFiles) {
        long length = fmc.getDatawaveHome().length() + 1 + fc.getScript().length();
        int first = -1;
        for (InputFile inFile : inFiles) {
            length += 1 + inFile.getTrackedDirLength(InputFile.TrackedDir.FLAGGED_DIR);
        }
        
        length += 1 + Integer.toString(fc.getReducers()).length() + " -inputFormat ".length() + fc.getInputFormat().getName().length() + 1
                        + (fc.getExtraIngestArgs() == null ? 0 : fc.getExtraIngestArgs().length());
        length += 1; // new line
        if (fc.getFileListMarker() != null) {
            length += fmc.getFlagFileDirectory().length() + df.format(System.currentTimeMillis() / 1000).length() + fc.getIngestPool().length()
                            + fc.getDataName().length() + first + Integer.toString(inFiles.size()).length();
            length += 41;
            length += fc.getFileListMarker().length();
        }
        
        return length;
    }
    
    @Override
    public boolean isValidSize(FlagDataTypeConfig fc, Collection<InputFile> files) {
        int maxCounters = Integer.MAX_VALUE;
        
        // Check Hadoop 2 variable, if null check Hadoop 1 variable
        if (this.config.get(COUNTER_LIMIT_HADOOP_2) != null) {
            maxCounters = Integer.parseInt(this.config.get(COUNTER_LIMIT_HADOOP_2));
        } else if (this.config.get(COUNTER_LIMIT_HADOOP_1) != null) {
            maxCounters = Integer.parseInt(this.config.get(COUNTER_LIMIT_HADOOP_1));
        }
        if (calculateCounters(files.size()) > maxCounters) {
            log.warn("Check hadoop configuration. Counter limit ({}) exceeded for {}. Restricting to {} files per flag file.", maxCounters, fc.getDataName(),
                            filesPerPartition(maxCounters));
            return false;
        }
        
        // now check the flag file size
        if (getFlagFileSize(fc, files) > fmc.getMaxFileLength()) {
            log.warn("Flag file size for {} exceeding {}.  Reducing number of files to compensate", fc.getDataName(), fmc.getMaxFileLength());
            return false;
        }
        
        return true;
    }
    
    private int calculateCounters(int numFiles) {
        return (numFiles * COUNTERS_PER_INPUT_FILE) + 2;
    }
    
    private int filesPerPartition(int maxCounters) {
        return ((maxCounters - 2) / 2);
    }
}
