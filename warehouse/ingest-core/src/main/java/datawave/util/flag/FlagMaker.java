package datawave.util.flag;

import java.io.*;
import java.net.Socket;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import datawave.util.StringUtils;
import datawave.metrics.util.flag.FlagFile;
import datawave.ingest.mapreduce.StandaloneStatusReporter;
import datawave.ingest.mapreduce.StandaloneTaskAttemptContext;
import datawave.util.flag.config.ConfigUtil;
import datawave.util.flag.config.FlagDataTypeConfig;
import datawave.util.flag.config.FlagMakerConfig;
import datawave.util.flag.processor.DateFlagDistributor;
import datawave.util.flag.processor.DateFolderFlagDistributor;
import datawave.util.flag.processor.DateUtils;
import datawave.util.flag.processor.FlagDistributor;
import datawave.util.flag.processor.SimpleFlagDistributor;
import datawave.util.flag.processor.SizeValidator;
import datawave.util.flag.processor.UnusableFileException;

import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Counters;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;

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
    /**
     * Directory cache will serve as a place holder the directories in HDFS that were created. This will cut down on the number of RPC calls tot he NameNode
     */
    private final Cache<Path,Path> directoryCache;
    // Executor will be used for directory lookups
    protected ExecutorService executor;
    private final FlagMakerConfig fmc;
    private FlagDistributor fd;
    private volatile boolean running = true;
    private FlagSocket flagSocket;
    private final DecimalFormat df = new DecimalFormat("#0.00");
    private final static byte ONE = '1';
    private final ReentrantLock lock = new ReentrantLock();
    private DateUtils util = new DateUtils();
    private StandaloneTaskAttemptContext<?,?,?,?> ctx;
    private static final SimpleDateFormat metricsFormat = new SimpleDateFormat("_yyyyMMdd_HHmmssSS");
    private static final String COUNTER_LIMIT_HADOOP_2 = "mapreduce.job.counters.max";
    private static final String COUNTER_LIMIT_HADOOP_1 = "mapreduce.job.counters.limit";
    private static final int COUNTERS_PER_INPUT_FILE = 2;
    protected JobConf config;
    
    public FlagMaker(FlagMakerConfig fmconfig) {
        this.fmc = fmconfig;
        this.config = new JobConf(new Configuration());
        
        setup();
        
        // build the cache per the default configuration.
        directoryCache = CacheBuilder.newBuilder().maximumSize(fmc.getDirectoryCacheSize())
                        .expireAfterWrite(fmc.getDirectoryCacheTimeout(), TimeUnit.MILLISECONDS).concurrencyLevel(fmc.getMaxHdfsThreads()).build();
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
            FlagMaker m = new FlagMaker(flagMakerConfig);
            m.run();
        } catch (IllegalArgumentException ex) {
            System.err.println("" + ex.getMessage());
            printUsage();
            System.exit(1);
        }
        
    }
    
    static FlagMakerConfig getFlagMakerConfig(String[] args) throws JAXBException, IOException {
        String flagConfig = null;
        String baseHDFSDirOverride = null;
        String extraIngestArgsOverride = null;
        String flagFileDirectoryOverride = null;
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
        log.debug(xmlObject.toString());
        return xmlObject;
        
    }
    
    private static void shutdown(int port) throws IOException {
        Socket s = new Socket("localhost", port);
        PrintWriter pw = new PrintWriter(s.getOutputStream(), true);
        pw.write("shutdown");
        pw.flush();
        pw.close();
        s.close();
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
        log.debug("FlagMaker run() starting");
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
        log.info("FlagMaker Exiting.");
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
            
            for (String folder : fc.getFolder()) {
                String folderPattern = folder + "/" + fmc.getFilePattern();
                log.trace("searching for {} files in {}", dataName, folderPattern);
                FileStatus[] files = fs.globStatus(new Path(folderPattern));
                if (files == null || files.length == 0) {
                    log.trace("files: {}", (files == null ? "null" : files.length));
                    continue;
                }
                
                // pull the base directory off of the folder
                if (folder.startsWith(fmc.getBaseHDFSDir())) {
                    log.trace("Removing base directory off folder {}", folder);
                    folder = folder.substring(fmc.getBaseHDFSDir().length());
                    log.trace("Adjusted folder: {}", folder);
                    
                    if (folder.startsWith(File.separator)) {
                        folder = folder.substring(File.separator.length());
                        log.trace("Removed separator: {}", folder);
                    }
                }
                
                // add the files
                for (FileStatus status : files) {
                    if (status.isDir()) {
                        log.warn("Skipping subdirectory {}", status.getPath());
                    } else {
                        try {
                            fd.addInputFile(new InputFile(folder, status.getPath(), status.getBlockSize(), status.getLen(), getTimestamp(status.getPath(),
                                            status.getModificationTime())));
                        } catch (UnusableFileException e) {
                            log.warn("Skipping unusable file " + status.getPath(), e);
                        }
                    }
                }
            }
            
            while (fd.hasNext(shouldOnlyCreateFullFlags(fc)) && running) {
                initStats(startTime);
                writeFlagFile(fc, fd.next(this));
            }
        }
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
    
    public long getTimestamp(Path path, long fileTimestamp) {
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
    int countFlagFileBacklog(final FlagDataTypeConfig fc) {
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
    
    /**
     * Write the flag file. This is done in several steps to ensure we can easily recover if we are killed somewhere in-between.
     * 
     * 1) move the files to the flagging directory for those that do not already exist in flagging, flagged, or loaded 2) create the flag.generating file for
     * those files we were able to move, and do not exist elsewhere 3) set the timestamp of the flag file to that of the most recent file 4) move the flagging
     * files to the flagged directory 5) move the generating file to the final flag file form 6) create the cleanup file
     * 
     * Using these steps, a cleanup of an abnormal termination is as follows: 1) move all flagging files to the base directory 2) for all flag.generating files,
     * move the flagged files to the base directory 3) remove the flag.generating files.
     * 
     * @param fc
     * @param inFiles
     * @throws IOException
     */
    void writeFlagFile(final FlagDataTypeConfig fc, Collection<InputFile> inFiles) throws IOException {
        
        long estSize = getFlagFileSize(fc, inFiles);
        if (inFiles == null || inFiles.isEmpty())
            throw new IllegalArgumentException("inFiles for Flag file");
        final ConcurrentHashMap<InputFile,Path> moved = new ConcurrentHashMap<>();
        File flagFile = null;
        final FileSystem fs = getHadoopFS();
        try {
            // first lets create the dest directories, and move the files into the flagging directory
            final AtomicLong latestTime = new AtomicLong(-1);
            List<Callable<Path>> callables = Lists.newArrayList();
            for (final InputFile inFile : inFiles) {
                // Create the flagging directory
                Callable<Path> mover = () -> {
                    Path dstFlagging = getDestPath(inFile, "flagging", fc);
                    if (directoryCache.getIfPresent(dstFlagging) == null && !fs.exists(dstFlagging.getParent())) {
                        fs.mkdirs(dstFlagging.getParent());
                        directoryCache.put(dstFlagging, dstFlagging);
                    }
                    // Create the flagged directory
                    Path dstFlagged = getDestPath(inFile, "flagged", fc);
                    if (directoryCache.getIfPresent(dstFlagged) == null && !fs.exists(dstFlagged.getParent())) {
                        fs.mkdirs(dstFlagged.getParent());
                        directoryCache.put(dstFlagged, dstFlagged);
                    }
                    // Check for existence of the file already in the flagging, flagged, or loaded directories
                    Path dstLoaded = getDestPath(inFile, "loaded", fc);
                    if (fs.exists(dstFlagged) || fs.exists(dstFlagging) || fs.exists(dstLoaded)) {
                        log.warn("Unable to move file {}/{} as it already exists in the flagging, flagged, or loaded directory", inFile.getDirectory(),
                                        inFile.getFileName());
                    } else {
                        // now move the file into the flagging directory
                        if (fs.rename(inFile.getPath(), dstFlagging)) {
                            moved.put(inFile, dstFlagging);
                            latestTime.set(Math.max(inFile.getTimestamp(), latestTime.get()));
                        } else {
                            log.error("Unable to move file {}/{}, skipping", inFile.getDirectory(), inFile.getFileName());
                        }
                    }
                    return dstLoaded;
                };
                // can look at I files that have a logical ts greater than the logical ts of the last compaction. that would indicate what has been imported
                // since that compaction started. which is basically the pile up we've seen.
                // we can look at the size of that compacted file and use that to determine how to size the next one
                
                // two prior compactions: 3139106321 3139418030, (311k) apart. and 4,274,086,297 4.2GB
                // 3139017948 - 3138938475 = 79473 (22 minutes) 3822549588 bytes = 3.8GB 3138796228 to 3139468770
                // can determine how many I files came in since the most recent C file
                // can find 5 most recent logical changes
                
                callables.add(mover);
            }
            try {
                List<Future<Path>> execResults = executor.invokeAll(callables);
                
                for (Future<Path> future : execResults) {
                    if (future.get() == null)
                        throw new IOException("Error while attempting to create path");
                }
                
            } catch (InterruptedException e) {
                throw new IOException(e.getCause());
            } catch (ExecutionException e) {
                throw new IOException(e.getCause());
            }
            
            // if no files moved, then abort
            if (moved.isEmpty()) {
                log.warn("No pending files were able to be moved to the flagging directory. Please investigate.");
                return;
            }
            
            // create the flag.generating file
            Path first = moved.entrySet().iterator().next().getValue();
            long now = System.currentTimeMillis();
            String baseName = fmc.getFlagFileDirectory() + File.separator + df.format(now / 1000) + "_" + fc.getIngestPool() + "_" + fc.getDataName() + "_"
                            + first.getName() + "+" + moved.size();
            log.info("Creating flag file {}.flag for data type {} containing {} files", baseName, fc.getDataName(), moved.size());
            File f = new File(baseName + ".flag.generating");
            if (f.createNewFile()) {
                flagFile = f;
            } else {
                throw new IOException("Unable to create flag file " + f);
            }
            FileOutputStream flagOS = new FileOutputStream(f);
            StringBuilder sb = new StringBuilder(fmc.getDatawaveHome() + File.separator + fc.getScript());
            if (fc.getFileListMarker() == null) {
                String sep = " ";
                for (InputFile inFile : moved.keySet()) {
                    if (fc.isCollectMetrics())
                        ctx.getCounter(InputFile.class.getSimpleName(), inFile.getFileName()).setValue(inFile.getTimestamp());
                    Path dstFlagged = getDestPath(inFile, "flagged", fc);
                    sb.append(sep).append(dstFlagged.toUri());
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
                for (InputFile inFile : moved.keySet()) {
                    if (fc.isCollectMetrics())
                        ctx.getCounter(InputFile.class.getSimpleName(), inFile.getFileName()).setValue(inFile.getTimestamp());
                    Path dstFlagged = getDestPath(inFile, "flagged", fc);
                    sb.append(dstFlagged.toUri()).append('\n');
                }
            }
            if (estSize != sb.length()) {
                log.error("Estimated size is not accurate: {} vs {}", estSize, sb.length());
            }
            
            flagOS.write(sb.toString().getBytes());
            flagOS.close();
            
            // now set the modification time of the flag file
            if (fmc.isSetFlagFileTimestamp()) {
                f.setLastModified(latestTime.get());
            }
            
            // Now we have files moved into the flagging directory, and a flag.generating file created
            
            // move the files to the flagged directory
            callables = Lists.newArrayList();
            
            for (Map.Entry<InputFile,Path> entry : moved.entrySet()) {
                final InputFile inFile = entry.getKey();
                final Path dstFlagging = entry.getValue();
                final Path dstFlagged = getDestPath(inFile, "flagged", fc);
                Callable<Path> mover = () -> {
                    if (fs.rename(dstFlagging, dstFlagged)) {
                        moved.put(inFile, dstFlagged);
                        if (fc.isCollectMetrics())
                            ctx.getCounter(FlagFile.class.getSimpleName(), dstFlagged.getName()).setValue(System.currentTimeMillis());
                    } else {
                        throw new IOException("Unable to move file " + inFile.getDirectory() + "/" + inFile.getFileName());
                    }
                    
                    return dstFlagged;
                };
                callables.add(mover);
            }
            
            try {
                List<Future<Path>> execResults = executor.invokeAll(callables);
                
                for (Future<Path> future : execResults) {
                    if (future.get() == null)
                        throw new IOException("Error while attempting to create path");
                }
                
            } catch (InterruptedException e) {
                throw new IOException(e.getCause());
            } catch (ExecutionException e) {
                throw new IOException(e.getCause());
            }
            
            File f2 = new File(baseName + ".flag");
            if (f.renameTo(f2)) {
                flagFile = f2;
            } else {
                throw new IOException("Failed to rename" + f.toString() + " to " + f2);
            }
            
            try {
                lock.lock();
                // after we write a file, set the timeout to the forceInterval
                fc.setLast(now + fc.getTimeoutMilliSecs());
            } finally {
                lock.unlock();
            }
            
            if (fc.isCollectMetrics())
                updateStats();
            
            if (fc.isCollectMetrics()) {
                writeMetrics(ctx.getReporter(), this.fmc.getFlagMetricsDirectory(), new Path(baseName).getName(), ct, cc);
            }
        } catch (IOException ex) {
            log.error("Unable to complete flag file ", ex);
            moveFilesBack(moved);
            if (flagFile != null) {
                flagFile.delete();
            }
            throw ex;
        }
    }
    
    private void moveFilesBack(ConcurrentHashMap<InputFile,Path> moved) throws IOException {
        if (moved.isEmpty())
            return;
        try {
            FileSystem fs = getHadoopFS();
            Iterator<Map.Entry<InputFile,Path>> it = moved.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<InputFile,Path> kv = it.next();
                if (fs.rename(kv.getValue(), kv.getKey().getPath())) {
                    it.remove();
                }
            }
        } catch (IOException ex) {
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<InputFile,Path> orphan : moved.entrySet()) {
                sb.append("\n").append(orphan.getValue().toString());
            }
            log.error("An error occurred while attempting to move files. The following files were orphaned: {}", sb.toString());
        }
    }
    
    /**
     * if it matches the date pattern, use that, otherwise use a pattern of now
     * 
     * @param inFile
     * @return
     */
    private Path getDestPath(InputFile inFile, String subdir, FlagDataTypeConfig fc) {
        Matcher m = pattern.matcher(inFile.getDirectory());
        String dst = fmc.getBaseHDFSDir() + subdir + File.separator + inFile.getFolder() + File.separator;
        if (m.find()) {
            String grp = m.group(1);
            dst += grp;
        } else {
            SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT_STRING);
            dst += format.format(new Date());
        }
        return new Path(dst + File.separator + inFile.getFileName());
    }
    
    /**
     * Get the length of the URI produced by getDestPath(inFile,subdir,fc)
     * 
     * @param inFile
     * @return the length of the URI
     */
    private int getDestPathLength(InputFile inFile, String subdir, FlagDataTypeConfig fc) {
        Matcher m = pattern.matcher(inFile.getDirectory());
        int len = fmc.getBaseHDFSDir().length() + subdir.length() + 1 + inFile.getFolder().length() + 1;
        if (m.find()) {
            String grp = m.group(1);
            len += grp.length();
        } else {
            SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT_STRING);
            len += format.format(new Date()).length();
        }
        return len + 1 + inFile.getFileName().length();
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
                    try {
                        lock.lock();
                        log.info("Forcing {} to generate flag file", dtype);
                        cfg.setLast(System.currentTimeMillis() - cfg.getTimeoutMilliSecs());
                        break;
                    } finally {
                        lock.unlock();
                    }
                    
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
     * Validate config and set up folders for each data type. Here we have a few rules: if you provide no folders, then we will assume that the folder is the
     * the data type which will be appended to the base directory (e.g. /data/ShardIngest). Users can provide absolute file paths by leading with a slash ("/")
     */
    private void setup() {
        String prefix = "FlagDataTypeConfig Error: ";
        // validate the config
        if (fmc.getDefaultCfg().getScript() == null) {
            throw new IllegalArgumentException(prefix + "default script is required");
        }
        String hdfsbasedir = fmc.getBaseHDFSDir();
        if (hdfsbasedir == null) {
            throw new IllegalArgumentException(prefix + "baseHDFSDir is required");
        }
        
        if (!hdfsbasedir.endsWith("/")) {
            fmc.setBaseHDFSDir(hdfsbasedir + "/");
        }
        
        int socketPort = fmc.getSocketPort();
        if (socketPort < 1025 || socketPort > 65534) {
            throw new IllegalArgumentException(prefix + "socketPort is required and must be greater than 1024 and less than 65535");
        }
        
        if (fmc.getFlagFileDirectory() == null) {
            throw new IllegalArgumentException(prefix + "flagFileDirectory is required");
        }
        
        if (fmc.getDefaultCfg().getMaxFlags() < 1) {
            throw new IllegalArgumentException(prefix + "Default Max Flags must be set.");
        }
        
        String dtype = fmc.getDistributorType();
        if (dtype == null || !dtype.matches("(simple|date|folderdate)")) {
            throw new IllegalArgumentException("Invalid Distributor type provided: " + dtype + ". Must be one of the following: simple|date|folderdate");
        }
        
        if ("simple".equals(dtype)) {
            fd = new SimpleFlagDistributor();
        } else if ("date".equals(dtype)) {
            fd = new DateFlagDistributor();
        } else if ("folderdate".equals(dtype)) {
            fd = new DateFolderFlagDistributor();
        }
        for (FlagDataTypeConfig cfg : fmc.getFlagConfigs()) {
            if (cfg.getInputFormat() == null)
                throw new IllegalArgumentException("Input Format Class must be specified for data type: " + cfg.getDataName());
            if (cfg.getIngestPool() == null)
                throw new IllegalArgumentException("Ingest Pool must be specified for data type: " + cfg.getDataName());
            if (cfg.getFlagCountThreshold() == FlagMakerConfig.UNSET) {
                cfg.setFlagCountThreshold(fmc.getFlagCountThreshold());
            }
            if (cfg.getTimeoutMilliSecs() == FlagMakerConfig.UNSET) {
                cfg.setTimeoutMilliSecs(fmc.getTimeoutMilliSecs());
            }
            cfg.setLast(System.currentTimeMillis() + cfg.getTimeoutMilliSecs());
            if (cfg.getMaxFlags() < 1) {
                cfg.setMaxFlags(fmc.getDefaultCfg().getMaxFlags());
            }
            if (cfg.getReducers() < 1) {
                cfg.setReducers(fmc.getDefaultCfg().getReducers());
            }
            if (cfg.getScript() == null || "".equals(cfg.getScript())) {
                cfg.setScript(fmc.getDefaultCfg().getScript());
            }
            if (cfg.getFileListMarker() == null || "".equals(cfg.getFileListMarker())) {
                cfg.setFileListMarker(fmc.getDefaultCfg().getFileListMarker());
            }
            if (cfg.getFileListMarker() != null) {
                if (cfg.getFileListMarker().indexOf(' ') >= 0) {
                    throw new IllegalArgumentException(prefix + "fileListMarker cannot contain spaces");
                }
            }
            if (cfg.getCollectMetrics() == null || "".equals(cfg.getCollectMetrics())) {
                cfg.setCollectMetrics(fmc.getDefaultCfg().getCollectMetrics());
            }
            List<String> folders = cfg.getFolder();
            if (folders == null || folders.isEmpty()) {
                folders = new ArrayList<>();
                cfg.setFolder(folders);
                // add the default path. we'll bomb later if it's not there.
                folders.add(cfg.getDataName());
            }
            List<String> fixedFolders = new ArrayList<>();
            for (int i = 0; i < folders.size(); i++) {
                for (String folder : StringUtils.split(folders.get(i), ',')) {
                    folder = folder.trim();
                    // let someone specify an absolute path.
                    if (!folder.startsWith("/")) {
                        fixedFolders.add(fmc.getBaseHDFSDir() + folder);
                    } else {
                        fixedFolders.add(folder);
                    }
                }
            }
            cfg.setFolder(fixedFolders);
        }
        
        // configure the executor per the FlagMakerConfig input
        executor = Executors.newFixedThreadPool(fmc.getMaxHdfsThreads());
    }
    
    private void initStats(long startTime) {
        this.ctx = new StandaloneTaskAttemptContext<Object,Object,Object,Object>(new Configuration(), new StandaloneStatusReporter());
        ctx.putIfAbsent(datawave.metrics.util.flag.InputFile.FLAGMAKER_START_TIME, startTime);
    }
    
    private void updateStats() {
        ctx.getCounter(datawave.metrics.util.flag.InputFile.FLAGMAKER_END_TIME).setValue(System.currentTimeMillis());
    }
    
    protected void writeMetrics(final StandaloneStatusReporter reporter, final String metricsDirectory, final String baseName, final CompressionType ct,
                    final CompressionCodec cc) throws IOException {
        
        FileSystem fs = getHadoopFS();
        
        if (reporter == null)
            return;
        
        final Counters c = reporter.getCounters();
        if (c == null || c.countCounters() <= 0)
            return;
        
        final String baseFileName = metricsDirectory + File.separator + baseName + ".metrics";
        
        String fileName = baseFileName;
        Path finishedMetricsFile = new Path(fileName);
        Path src = new Path(fileName + ".working");
        
        if (!fs.exists(finishedMetricsFile.getParent())) {
            fs.mkdirs(finishedMetricsFile.getParent());
        }
        
        if (!fs.exists(src.getParent())) {
            fs.mkdirs(src.getParent());
        }
        
        int count = 0;
        
        while (true) {
            while (fs.exists(finishedMetricsFile) || !fs.createNewFile(src)) {
                count++;
                
                fileName = baseFileName + '.' + count;
                finishedMetricsFile = new Path(fileName);
                src = new Path(fileName + ".working");
            }
            
            if (!fs.exists(finishedMetricsFile))
                break;
            fs.delete(src, false);
        }
        
        final Writer writer = SequenceFile.createWriter(fs, new Configuration(), src, Text.class, Counters.class, ct, cc);
        writer.append(new Text(baseName), c);
        writer.close();
        
        if (!fs.rename(src, finishedMetricsFile))
            log.error("Could not rename metrics file to completed name. Failed file will persist until manually removed.");
    }
    
    /**
     * Get the length of the flag file that would be created using this set of files.
     * 
     * @param fc
     * @param inFiles
     * @return The size in characters of the flag file
     */
    long getFlagFileSize(FlagDataTypeConfig fc, Collection<InputFile> inFiles) {
        long length = 0;
        length += fmc.getDatawaveHome().length() + 1 + fc.getScript().length();
        int first = -1;
        for (InputFile inFile : inFiles) {
            if (first == -1) {
                first = getDestPathLength(inFile, "flagged", fc);
                length += 1 + first;
            } else {
                length += 1 + getDestPathLength(inFile, "flagged", fc);
            }
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
