package datawave.ingest.mapreduce.job;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.mapreduce.StandaloneStatusReporter;
import datawave.util.cli.PasswordConverter;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.Credentials;
import org.apache.accumulo.core.client.impl.MasterClient;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.master.thrift.MasterClientService.Iface;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * A processor whose job is to watch for completed Bulk Ingest jobs and bring the map files produced by them online in accumulo. This class attempts to bring
 * multiple map files online at once if many jobs have completed, and also attempts to throttle itself to prevent queuing up too many major compactions on the
 * various tablet servers.
 */
public final class BulkIngestMapFileLoader implements Runnable {
    private static Logger log = Logger.getLogger(BulkIngestMapFileLoader.class);
    private static int SLEEP_TIME = 30000;
    private static int FAILURE_SLEEP_TIME = 10 * 60 * 1000; // 10 minutes
    private static int MAX_DIRECTORIES = 1;
    private static int MAJC_CHECK_INTERVAL = 1;
    private static int MAJC_THRESHOLD = 3000;
    private static int MAJC_WAIT_TIMEOUT = 0;// 2 * 60 * 1000;
    private static int SHUTDOWN_PORT = 24111;
    private static boolean FIFO = true;
    private static boolean INGEST_METRICS = true;
    
    public static final String COMPLETE_FILE_MARKER = "job.complete";
    public static final String LOADING_FILE_MARKER = "job.loading";
    public static final String FAILED_FILE_MARKER = "job.failed";
    public static final String ATTEMPT_FILE_MARKER = "job.load.attempt.failed.do.not.delete";
    public static final String INPUT_FILES_MARKER = "job.paths";
    private static String cleanUpScript;
    
    private Path workDir;
    private String jobDirPattern;
    private String instanceName;
    private String zooKeepers;
    private Credentials credentials;
    private Map<String,Integer> tablePriorities;
    private Configuration conf;
    private URI seqFileHdfs;
    private URI srcHdfs;
    private URI destHdfs;
    private String jobtracker;
    private StandaloneStatusReporter reporter = new StandaloneStatusReporter();
    private volatile boolean running;
    private ExecutorService executor;
    
    public static void main(String[] args) throws AccumuloSecurityException, IOException {
        
        URI seqFileHdfs = null;
        URI srcHdfs = null;
        URI destHdfs = null;
        String jobtracker = null;
        Configuration conf = new Configuration();
        ArrayList<String[]> properties = new ArrayList<>();
        
        if (args.length < 6) {
            log.error("usage: BulkIngestMapFileLoader hdfsWorkDir jobDirPattern instanceName zooKeepers username password "
                            + "[-sleepTime sleepTime] [-majcThreshold threshold] [-majcCheckInterval count] [-majcDelay majcDelay] "
                            + " [-seqFileHdfs seqFileSystemUri] [-srcHdfs srcFileSystemURI] [-destHdfs destFileSystemURI] [-jt jobTracker] "
                            + "[-ingestMetricsDisabled] [-shutdownPort portNum] confFile [{confFile}]");
            System.exit(-1);
        }
        
        int numBulkThreads = 8;
        int numBulkAssignThreads = 4;
        // default the number of HDFS threads to 1
        int numHdfsThreads = 1;
        if (args.length > 6) {
            for (int i = 6; i < args.length; ++i) {
                if ("-sleepTime".equalsIgnoreCase(args[i])) {
                    if (i + 2 > args.length) {
                        log.error("-sleepTime must be followed by the number of ms to sleep between checks for map files.");
                        System.exit(-2);
                    }
                    try {
                        SLEEP_TIME = Integer.parseInt(args[++i]);
                    } catch (NumberFormatException e) {
                        log.error("-sleepTime must be followed by the number of ms to sleep between checks for map files.", e);
                        System.exit(-2);
                    }
                } else if ("-majcThreshold".equalsIgnoreCase(args[i])) {
                    if (i + 2 > args.length) {
                        log.error("-majcThreshold must be followed by the maximum number of major compactions allowed before waiting");
                        System.exit(-2);
                    }
                    try {
                        MAJC_THRESHOLD = Integer.parseInt(args[++i]);
                    } catch (NumberFormatException e) {
                        log.error("-majcThreshold must be followed by the maximum number of major compactions allowed before waiting", e);
                        System.exit(-2);
                    }
                } else if ("-majcDelay".equalsIgnoreCase(args[i])) {
                    if (i + 2 > args.length) {
                        log.error("-majcDelay must be followed by the minimum number of ms to elapse between bringing map files online");
                        System.exit(-2);
                    }
                    try {
                        MAJC_WAIT_TIMEOUT = Integer.parseInt(args[++i]);
                    } catch (NumberFormatException e) {
                        log.error("-majcDelay must be followed by the minimum number of ms to elapse between bringing map files online", e);
                        System.exit(-2);
                    }
                } else if ("-majcCheckInterval".equalsIgnoreCase(args[i])) {
                    if (i + 2 > args.length) {
                        log.error("-majcCheckInterval must be followed by the number of bulk loads to process before rechecking the majcThreshold and majcDelay");
                        System.exit(-2);
                    }
                    try {
                        MAJC_CHECK_INTERVAL = Integer.parseInt(args[++i]);
                    } catch (NumberFormatException e) {
                        log.error("-majcCheckInterval must be followed by the number of bulk loads to process before rechecking the majcThreshold and majcDelay",
                                        e);
                        System.exit(-2);
                    }
                } else if ("-maxDirectories".equalsIgnoreCase(args[i])) {
                    if (i + 2 > args.length) {
                        log.error("-maxDirectories must be followed a number of directories");
                        System.exit(-2);
                    }
                    try {
                        MAX_DIRECTORIES = Integer.parseInt(args[++i]);
                    } catch (NumberFormatException e) {
                        log.error("-maxDirectories must be followed a number of directories", e);
                        System.exit(-2);
                    }
                } else if ("-numThreads".equalsIgnoreCase(args[i])) {
                    if (i + 2 > args.length) {
                        log.error("-numThreads must be followed by the number of bulk import threads");
                        System.exit(-2);
                    }
                    try {
                        numBulkThreads = Integer.parseInt(args[++i]);
                    } catch (NumberFormatException e) {
                        log.error("-numThreads must be followed by the number of bulk import threads", e);
                        System.exit(-2);
                    }
                } else if ("-numHdfsThreads".equalsIgnoreCase(args[i])) {
                    if (i + 2 > args.length) {
                        log.error("-numHdfsThreads must be followed by the number of threads to use for concurrent HDFS operations");
                        System.exit(-2);
                    }
                    try {
                        numHdfsThreads = Integer.parseInt(args[++i]);
                    } catch (NumberFormatException e) {
                        log.error("-numHdfsThreads must be followed by the number of threads to use for concurrent HDFS operations", e);
                        System.exit(-2);
                    }
                } else if ("-numAssignThreads".equalsIgnoreCase(args[i])) {
                    if (i + 2 > args.length) {
                        log.error("-numAssignThreads must be followed by the number of bulk import assignment threads");
                        System.exit(-2);
                    }
                    try {
                        numBulkAssignThreads = Integer.parseInt(args[++i]);
                    } catch (NumberFormatException e) {
                        log.error("-numAssignThreads must be followed by the number of bulk import assignment threads", e);
                        System.exit(-2);
                    }
                } else if ("-seqFileHdfs".equalsIgnoreCase(args[i])) {
                    if (i + 2 > args.length) {
                        log.error("-seqFileHdfs must be followed a file system URI (e.g. hdfs://hostname:54310).");
                        System.exit(-2);
                    }
                    try {
                        seqFileHdfs = new URI(args[++i]);
                    } catch (URISyntaxException e) {
                        log.error("-seqFileHdfs must be followed a file system URI (e.g. hdfs://hostname:54310).", e);
                        System.exit(-2);
                    }
                } else if ("-srcHdfs".equalsIgnoreCase(args[i])) {
                    if (i + 2 > args.length) {
                        log.error("-srcHdfs must be followed a file system URI (e.g. hdfs://hostname:54310).");
                        System.exit(-2);
                    }
                    try {
                        srcHdfs = new URI(args[++i]);
                    } catch (URISyntaxException e) {
                        log.error("-srcHdfs must be followed a file system URI (e.g. hdfs://hostname:54310).", e);
                        System.exit(-2);
                    }
                } else if ("-destHdfs".equalsIgnoreCase(args[i])) {
                    if (i + 2 > args.length) {
                        log.error("-destHdfs must be followed a file system URI (e.g. hdfs://hostname:54310).");
                        System.exit(-2);
                    }
                    try {
                        destHdfs = new URI(args[++i]);
                    } catch (URISyntaxException e) {
                        log.error("-destHdfs must be followed a file system URI (e.g. hdfs://hostname:54310).", e);
                        System.exit(-2);
                    }
                } else if ("-jobCleanupScript".equalsIgnoreCase(args[i])) {
                    if (i + 2 > args.length) {
                        log.error("-jobCleanupScript must be followed by an absolute file path.");
                        System.exit(-2);
                    }
                    cleanUpScript = args[++i];
                    log.info("Using " + cleanUpScript + " to post process map loading");
                } else if ("-jt".equalsIgnoreCase(args[i])) {
                    if (i + 2 > args.length) {
                        log.error("-jt must be followed a jobtracker (e.g. hostname:54311).");
                        System.exit(-2);
                    }
                    jobtracker = args[++i];
                } else if ("-shutdownPort".equalsIgnoreCase(args[i])) {
                    if (i + 2 > args.length) {
                        log.error("-shutdownPort must be followed a port number");
                        System.exit(-2);
                    }
                    try {
                        SHUTDOWN_PORT = Integer.parseInt(args[++i]);
                    } catch (NumberFormatException e) {
                        log.error("-shutdownPort must be followed a port number", e);
                        System.exit(-2);
                    }
                } else if ("-ingestMetricsDisabled".equalsIgnoreCase(args[i])) {
                    INGEST_METRICS = false;
                    log.info("Ingest metrics disabled");
                } else if ("-lifo".equalsIgnoreCase(args[i])) {
                    FIFO = false;
                    log.info("Changing processing order to LIFO");
                } else if ("-fifo".equalsIgnoreCase(args[i])) {
                    FIFO = true;
                    log.info("Changing processing order to FIFO");
                } else if (args[i].startsWith("-")) {
                    int index = args[i].indexOf('=', 1);
                    if (index < 0) {
                        log.error("WARN: skipping bad property configuration " + args[i]);
                    } else {
                        String[] strArr = new String[] {args[i].substring(1, index), args[i].substring(index + 1)};
                        log.info("Setting " + strArr[0] + " = \"" + strArr[1] + '"');
                        properties.add(strArr);
                    }
                } else {
                    log.info("Adding resource " + args[i]);
                    conf.addResource(args[i]);
                }
            }
        }
        
        log.info("Set sleep time to " + SLEEP_TIME + "ms");
        log.info("Will wait to bring map files online if there are more than " + MAJC_THRESHOLD + " running or queued major compactions.");
        log.info("Will not bring map files online unless at least " + MAJC_WAIT_TIMEOUT + "ms have passed since last time.");
        log.info("Will check the majcThreshold and majcDelay every " + MAJC_CHECK_INTERVAL + " bulk loads.");
        log.info("Processing a max of " + MAX_DIRECTORIES + " directories");
        log.info("Using " + numBulkThreads + " bulk load threads");
        log.info("Using " + numHdfsThreads + " HDFS operation threads");
        log.info("Using " + numBulkAssignThreads + " bulk assign threads");
        log.info("Using " + seqFileHdfs + " as the file system containing the original sequence files");
        log.info("Using " + srcHdfs + " as the source file system");
        log.info("Using " + destHdfs + " as the destination file system");
        log.info("Using " + jobtracker + " as the jobtracker");
        log.info("Using " + SHUTDOWN_PORT + " as the shutdown port");
        log.info("Using " + (FIFO ? "FIFO" : "LIFO") + " processing order");
        
        for (String[] s : properties) {
            conf.set(s[0], s[1]);
        }
        
        TypeRegistry.getInstance(conf);
        if (TypeRegistry.getTypes().isEmpty()) {
            log.error("Configured data types is empty");
            System.exit(-2);
        }
        
        // get the table priorities
        Map<String,Integer> tablePriorities = TableConfigurationUtil.getTablePriorities(conf);
        if (tablePriorities.isEmpty()) {
            log.error("Configured tables for configured data types is empty");
            System.exit(-2);
        }
        log.info("Found table priorities: " + tablePriorities);
        
        String workDir = args[0];
        String jobDirPattern = args[1].replaceAll("'", "");
        String instanceName = args[2];
        String zooKeepers = args[3];
        String passwordStr = PasswordConverter.parseArg(args[5]);
        
        Credentials credentials = new Credentials(args[4], new PasswordToken(passwordStr));
        BulkIngestMapFileLoader processor = new BulkIngestMapFileLoader(workDir, jobDirPattern, instanceName, zooKeepers, credentials, seqFileHdfs, srcHdfs,
                        destHdfs, jobtracker, tablePriorities, conf, SHUTDOWN_PORT, numHdfsThreads);
        Thread t = new Thread(processor, "map-file-watcher");
        t.start();
    }
    
    public BulkIngestMapFileLoader(String workDir, String jobDirPattern, String instanceName, String zooKeepers, Credentials credentials, URI seqFileHdfs,
                    URI srcHdfs, URI destHdfs, String jobtracker, Map<String,Integer> tablePriorities, Configuration conf) {
        this(workDir, jobDirPattern, instanceName, zooKeepers, credentials, seqFileHdfs, srcHdfs, destHdfs, jobtracker, tablePriorities, conf, SHUTDOWN_PORT, 1);
    }
    
    public BulkIngestMapFileLoader(String workDir, String jobDirPattern, String instanceName, String zooKeepers, Credentials credentials, URI seqFileHdfs,
                    URI srcHdfs, URI destHdfs, String jobtracker, Map<String,Integer> tablePriorities, Configuration conf, int shutdownPort) {
        this(workDir, jobDirPattern, instanceName, zooKeepers, credentials, seqFileHdfs, srcHdfs, destHdfs, jobtracker, tablePriorities, conf, shutdownPort, 1);
    }
    
    public BulkIngestMapFileLoader(String workDir, String jobDirPattern, String instanceName, String zooKeepers, Credentials credentials, URI seqFileHdfs,
                    URI srcHdfs, URI destHdfs, String jobtracker, Map<String,Integer> tablePriorities, Configuration conf, int shutdownPort, int numHdfsThreads) {
        this.conf = conf;
        this.tablePriorities = tablePriorities;
        this.workDir = new Path(workDir);
        this.jobDirPattern = jobDirPattern;
        this.instanceName = instanceName;
        this.zooKeepers = zooKeepers;
        this.credentials = credentials;
        this.seqFileHdfs = seqFileHdfs;
        this.srcHdfs = srcHdfs;
        this.destHdfs = destHdfs;
        this.jobtracker = jobtracker;
        this.running = true;
        this.executor = Executors.newFixedThreadPool(numHdfsThreads > 0 ? numHdfsThreads : 1);
        try {
            if (shutdownPort > 0) {
                final ServerSocket serverSocket = new ServerSocket(shutdownPort);
                Runnable shutdownListener = () -> listenForShutdownCommand(serverSocket);
                Thread t = new Thread(shutdownListener, "shutdown-listener");
                t.setDaemon(true);
                t.start();
            }
        } catch (IOException e) {
            log.error("Unable to create shutdown listener socket. Exiting.", e);
            System.exit(-3);
        }
    }
    
    @Override
    public void run() {
        log.info("Starting process to monitor map files.");
        long lastOnlineTime = 0;
        long lastLoadMessageTime = 0;
        int fsAccessFailures = 0;
        Path[] jobDirectories = new Path[0];
        int nextJobIndex = 0;
        try {
            while (true) {
                try {
                    if (!running)
                        break;
                    sleep();
                    if (!running)
                        break;
                    long loadMessageDelta = System.currentTimeMillis() - lastLoadMessageTime;
                    boolean logMessages = (loadMessageDelta > (5 * 60 * 1000));
                    if (logMessages) {
                        lastLoadMessageTime = System.currentTimeMillis();
                    }
                    if (!canBringMapFilesOnline(lastOnlineTime, logMessages)) {
                        if (logMessages) {
                            log.info("Waiting for load to decrease before bringing more map files online.");
                        }
                        continue;
                    }
                    List<Path> processedDirectories = new ArrayList<>();
                    if (nextJobIndex >= jobDirectories.length) {
                        jobDirectories = getJobDirectories();
                        nextJobIndex = 0;
                    }
                    if (jobDirectories.length > 0) {
                        while (processedDirectories.size() < MAJC_CHECK_INTERVAL && jobDirectories.length > 0) {
                            Path srcJobDirectory = jobDirectories[nextJobIndex++];
                            if (!running)
                                break;
                            // take ownership of the job directory if we can
                            if (takeOwnershipJobDirectory(srcJobDirectory)) {
                                processedDirectories.add(srcJobDirectory);
                                Path mapFilesDir = new Path(srcJobDirectory, "mapFiles");
                                reporter.getCounter("MapFileLoader.StartTimes", srcJobDirectory.getName()).increment(System.currentTimeMillis());
                                Path dstJobDirectory = srcJobDirectory;
                                URI workingHdfs = srcHdfs;
                                
                                try {
                                    log.info("Started processing " + mapFilesDir);
                                    long start = System.currentTimeMillis();
                                    
                                    // copy the data if needed
                                    dstJobDirectory = distCpDirectory(srcJobDirectory);
                                    workingHdfs = destHdfs;
                                    
                                    // recreate the map files directory reference in case it moved filesystems
                                    mapFilesDir = new Path(dstJobDirectory, "mapFiles");
                                    
                                    // now if we have a destination work directory, then move then move the files
                                    bringMapFilesOnline(mapFilesDir);
                                    
                                    // ensure everything got loaded
                                    verifyNothingLeftBehind(mapFilesDir);
                                    
                                    cleanUpJobDirectory(mapFilesDir);
                                    long end = System.currentTimeMillis();
                                    log.info("Finished processing " + mapFilesDir + ", duration (sec): " + ((end - start) / 1000));
                                    
                                    // now that we actually processed something, reset the last load message time to force a message on the next round
                                    lastLoadMessageTime = 0;
                                } catch (Exception e) {
                                    log.error("Failed to process " + mapFilesDir, e);
                                    boolean marked = markJobDirectoryFailed(workingHdfs, dstJobDirectory);
                                    if (!marked) {
                                        ++fsAccessFailures;
                                        if (fsAccessFailures >= 3) {
                                            log.error("Too many failures updating marker files.  Exiting...");
                                            shutdown();
                                        } else {
                                            log.warn("Failed to mark " + dstJobDirectory + " as failed. Sleeping in case this was a transient failure.");
                                            try {
                                                Thread.sleep(FAILURE_SLEEP_TIME);
                                            } catch (InterruptedException ie) {
                                                log.warn("Interrupted while sleeping.", ie);
                                            }
                                        }
                                    }
                                }
                            }
                            if (nextJobIndex >= jobDirectories.length) {
                                jobDirectories = getJobDirectories();
                                nextJobIndex = 0;
                            }
                            
                        }
                        if (!processedDirectories.isEmpty()) {
                            writeStats(processedDirectories.toArray(new Path[processedDirectories.size()]));
                            lastOnlineTime = System.currentTimeMillis();
                        }
                    }
                } catch (Exception e) {
                    log.error("Error: " + e.getMessage(), e);
                }
            }
        } finally {
            log.info("Shutting down executor service");
            executor.shutdown();
        }
        log.info("Bulk map file loader shutting down.");
    }
    
    protected void shutdown() {
        running = false;
    }
    
    /**
     * Listens for connections on {@code serverSocket}. Upon receipt of a connection, listens for a shutdown command which must be sent within 30 seconds. If
     * the shutdown command is received, then the map file loader will shut down.
     */
    protected void listenForShutdownCommand(ServerSocket serverSocket) {
        log.info("Listening for shutdown commands on port " + serverSocket.getLocalPort());
        while (true) {
            try {
                Socket s = serverSocket.accept();
                SocketAddress remoteAddress = s.getRemoteSocketAddress();
                try {
                    log.info(remoteAddress + " connected to the shutdown port");
                    s.setSoTimeout(30000);
                    InputStream is = s.getInputStream();
                    BufferedReader rdr = new BufferedReader(new InputStreamReader(is));
                    String line = rdr.readLine();
                    is.close();
                    s.close();
                    if ("quit".equalsIgnoreCase(line) || "exit".equalsIgnoreCase(line) || "shutdown".equalsIgnoreCase(line)) {
                        log.info("Shutdown command received.");
                        shutdown();
                        serverSocket.close();
                        break;
                    } else {
                        log.info("Unkown command [" + line + "] received from " + remoteAddress + ".  Ignoring.");
                    }
                } catch (SocketTimeoutException e) {
                    log.info("Timed out waiting for input from " + remoteAddress);
                }
            } catch (IOException e) {
                log.error("Error waiting for shutdown connection: " + e.getMessage(), e);
            }
        }
    }
    
    private FileSystem getFileSystem(URI uri) throws IOException {
        return (uri == null ? FileSystem.get(conf) : FileSystem.get(uri, conf));
    }
    
    private Path distCpDirectory(Path jobDirectory) throws Exception {
        // if the src filesystem is not the same as our local file system, then move the files using distcp
        FileSystem src = getFileSystem(srcHdfs);
        FileSystem dest = getFileSystem(destHdfs);
        if (!src.equals(dest)) {
            Path srcPath = src.makeQualified(new Path(jobDirectory.toUri().getPath()));
            Path destPath = dest.makeQualified(new Path(jobDirectory.toUri().getPath()));
            Path logPath = new Path(destPath, "logs");
            
            log.info("Copying (using distcp) " + srcPath + " to " + destPath);
            
            // Make sure the destination path doesn't already exist, so that distcp won't
            // complain. We could add -i to the distcp command, but we don't want to hide
            // any other failures that we might care about (such as map files failing to
            // copy). We know the distcp target shouldn't exist, so if it does, it could
            // only be from a previous failed attempt.
            dest.delete(destPath, true);
            
            // NOTE: be careful with the preserve option. We only want to preserve user, group, and permissions but
            // not carry block size or replication across. This is especially important because by default the
            // MapReduce jobs produce output with the replication set to 1 and we definitely don't want to preserve
            // that when copying across clusters.
            //@formatter:off
            DistCpOptions options = new DistCpOptions.Builder(srcPath, destPath)
                .withLogPath(logPath)
                .withSyncFolder(true)
                .preserve(DistCpOptions.FileAttribute.USER)
                .preserve(DistCpOptions.FileAttribute.GROUP)
                .preserve(DistCpOptions.FileAttribute.PERMISSION)
                .build();
            //@formatter:on
            String[] args = (jobtracker == null) ? new String[0] : new String[] {"-jt", jobtracker};
            int res = ToolRunner.run(conf, new DistCp(conf, options), args);
            if (res != 0) {
                log.error("The toolrunner failed to execute.  Returned with exit code of " + res);
                throw new RuntimeException("Failed to DistCp: " + res);
            } else {
                // verify the data was copied
                Map<String,FileStatus> destFiles = new HashMap<>();
                for (FileStatus destFile : dest.listStatus(destPath)) {
                    destFiles.put(destFile.getPath().getName(), destFile);
                }
                
                for (FileStatus srcFile : src.listStatus(srcPath)) {
                    FileStatus destFile = destFiles.get(srcFile.getPath().getName());
                    if (destFile == null || destFile.getLen() != srcFile.getLen()) {
                        log.error("The DistCp failed to copy " + srcFile.getPath());
                        throw new RuntimeException("Failed to DistCp " + srcFile.getPath());
                    }
                }
            }
            
            // now we can clean up the src job directory
            src.delete(jobDirectory, true);
            
            return destPath;
        }
        return jobDirectory;
    }
    
    /**
     * Determines whether or not it is safe to bring map files online. This asks Accumulo for its stats for major compaction (running and queued), and will
     * return false if either "too many" compactions are running/queued.
     */
    public boolean canBringMapFilesOnline(long lastOnlineTime, boolean logInfo) {
        Level level = (logInfo ? Level.INFO : Level.DEBUG);
        int majC = getMajorCompactionCount();
        log.log(level, "There are " + majC + " compactions currently running or queued.");
        
        long delta = System.currentTimeMillis() - lastOnlineTime;
        log.log(level, "Time since map files last brought online: " + (delta / 1000) + "s");
        
        return (delta > MAJC_WAIT_TIMEOUT) && (majC < MAJC_THRESHOLD);
    }
    
    private int getMajorCompactionCount() {
        int majC = 0;
        
        ZooKeeperInstance instance = new ZooKeeperInstance(ClientConfiguration.loadDefault().withInstance(instanceName).withZkHosts(zooKeepers));
        
        Iface client = null;
        try {
            client = MasterClient.getConnection(new ClientContext(instance, credentials, AccumuloConfiguration.getDefaultConfiguration()));
            MasterMonitorInfo mmi = client.getMasterStats(null, credentials.toThrift(instance));
            Map<String,TableInfo> tableStats = mmi.getTableMap();
            
            for (java.util.Map.Entry<String,TableInfo> e : tableStats.entrySet()) {
                majC += e.getValue().getMajors().getQueued();
                majC += e.getValue().getMajors().getRunning();
            }
        } catch (Exception e) {
            // Accumulo API changed, catch exception for now until we redeploy
            // accumulo on lightning.
            log.error("Unable to retrieve major compaction stats: " + e.getMessage());
        } finally {
            if (client != null) {
                MasterClient.close(client);
            }
        }
        
        return majC;
    }
    
    /**
     * Gets a list of job directories that are marked as completed. That is, these are job directories for which the MapReduce jobs have completed and there are
     * map files ready to be loaded.
     */
    private Path[] getJobDirectories() throws IOException {
        log.debug("Checking for completed job directories.");
        FileSystem fs = getFileSystem(srcHdfs);
        FileStatus[] files = fs.globStatus(new Path(workDir, jobDirPattern + '/' + COMPLETE_FILE_MARKER));
        Path[] jobDirectories;
        if (files != null && files.length > 0) {
            final int order = (FIFO ? 1 : -1);
            Arrays.sort(files, (o1, o2) -> {
                long m1 = o1.getModificationTime();
                long m2 = o2.getModificationTime();
                return order * ((m1 < m2) ? -1 : ((m1 > m2) ? 1 : 0));
            });
            jobDirectories = new Path[Math.min(MAX_DIRECTORIES, files.length)];
            for (int i = 0; i < jobDirectories.length; i++) {
                jobDirectories[i] = files[i].getPath().getParent();
            }
        } else {
            jobDirectories = new Path[0];
        }
        log.debug("Completed job directories: " + Arrays.toString(jobDirectories));
        return jobDirectories;
    }
    
    /**
     * Brings all map files in {@code mapFilesDir} online in accumulo. Note that {@code mapFilesDir} is assumed to have subdirectories that are the names of the
     * tables for which map files are to be loaded. Under those directories should be "part-XXXXX" directories which in turn contain the map/index files.
     */
    public void bringMapFilesOnline(Path mapFilesDir) throws IOException, AccumuloException, AccumuloSecurityException, TableNotFoundException {
        log.info("Bringing all mapFiles under " + mapFilesDir + " online.");
        
        // By now the map files should be on the local filesystem
        FileSystem fs = getFileSystem(destHdfs);
        
        Instance instance = new ZooKeeperInstance(ClientConfiguration.loadDefault().withInstance(instanceName).withZkHosts(zooKeepers));
        TableOperations tops = instance.getConnector(credentials.getPrincipal(), credentials.getToken()).tableOperations();
        Map<String,String> tableIds = tops.tableIdMap();
        FileStatus[] tableDirs = fs.globStatus(new Path(mapFilesDir, "*"));
        
        // sort the table dirs in priority order based on the configuration
        Arrays.sort(tableDirs, (o1, o2) -> {
            Integer p1 = tablePriorities.get(o1.getPath().getName());
            Integer p2 = tablePriorities.get(o2.getPath().getName());
            if (p1 == null) {
                if (p2 == null) {
                    return o1.getPath().getName().compareTo(o2.getPath().getName());
                } else {
                    return 1;
                }
            } else {
                if (p2 == null) {
                    return -1;
                } else {
                    return p1.compareTo(p2);
                }
            }
        });
        
        // now load the tables in the prioritized order, concurrently loading those with the same priority
        Integer priority = null;
        Stack<ImportRunnable> imports = new Stack<>();
        Map<String,Path> tableNames = new HashMap<>();
        for (FileStatus stat : tableDirs) {
            Path tableDir = stat.getPath();
            String tableName = tableDir.getName();
            
            if (!tableIds.containsKey(tableName)) {
                log.debug("Skipping " + tableDir + " since it is not a accumulo table directory.");
                continue;
            }
            
            if (tableNames.containsKey(tableName)) {
                if (tableNames.get(tableName).equals(tableDir)) {
                    log.warn("Skipping " + tableDir + " since we already processed " + tableName + " under " + tableNames.get(tableName));
                    continue;
                } else {
                    log.error("We got two different paths for " + tableName + ": " + tableNames.get(tableName) + " and " + tableDir);
                    throw new IOException("We got two different paths for " + tableName + ": " + tableNames.get(tableName) + " and " + tableDir);
                }
            }
            tableNames.put(tableName, tableDir);
            
            Integer newPriority = tablePriorities.get(stat.getPath().getName());
            if (!Objects.equal(priority, newPriority)) {
                Exception e = null;
                while (!imports.isEmpty()) {
                    ImportRunnable importTask = imports.pop();
                    try {
                        importTask.waitForCompletion();
                        if (e == null)
                            e = importTask.getException();
                    } catch (InterruptedException interrupted) {
                        // this task was interrupted, wait for the others and then terminate
                        if (e == null)
                            e = interrupted;
                    }
                }
                // if an exception occurred during processing, terminate
                if (e != null)
                    throw new IOException(e);
                
                priority = tablePriorities.get(stat.getPath().getName());
            }
            imports.push(startImport(mapFilesDir, tableName, tableDir, tops));
        }
        
        Exception e = null;
        while (!imports.isEmpty()) {
            ImportRunnable importTask = imports.pop();
            try {
                importTask.waitForCompletion();
                if (e == null)
                    e = importTask.getException();
            } catch (InterruptedException interrupted) {
                // this task was interrupted, wait for the others and then terminate
                if (e == null)
                    e = interrupted;
            }
        }
        // if an exception occurred during processing, terminate
        if (e != null)
            throw new IOException(e);
    }
    
    public ImportRunnable startImport(Path mapFilesDir, String tableName, Path tableDir, TableOperations tops) {
        ImportRunnable runnable = new ImportRunnable(mapFilesDir, tableName, tableDir, tops);
        Thread thread = new Thread(runnable);
        runnable.setThread(thread);
        thread.start();
        return runnable;
    }
    
    public class ImportRunnable implements Runnable {
        private boolean complete = false;
        private String tableName;
        private Path tableDir;
        private TableOperations tops;
        private Path mapFilesDir;
        private Exception exception = null;
        private Thread thread = null;
        
        private ImportRunnable(Path mapFilesDir, String tableName, Path tableDir, TableOperations tops) {
            this.tableName = tableName;
            this.tableDir = tableDir;
            this.tops = tops;
            this.mapFilesDir = mapFilesDir;
        }
        
        private void setThread(Thread thread) {
            this.thread = thread;
        }
        
        public Exception getException() {
            return exception;
        }
        
        public boolean isComplete() {
            return complete;
        }
        
        public void waitForCompletion() throws InterruptedException {
            synchronized (this) {
                while (!complete) {
                    if (!thread.isAlive()) {
                        throw new InterruptedException("This thread is no longer alive but yet the task is incomplete");
                    }
                    if (thread.isInterrupted()) {
                        throw new InterruptedException("This thread has been interrupted");
                    }
                    this.wait(10000);
                }
            }
        }
        
        public void run() {
            try {
                // Ensure all of the files put just under tableDir....
                collapseDirectory();
                
                // create the failures directory
                String failuresDir = mapFilesDir + "/failures/" + tableName;
                Path failuresPath = new Path(failuresDir);
                FileSystem fileSystem = FileSystem.get(srcHdfs, new Configuration());
                if (fileSystem.exists(failuresPath)) {
                    log.fatal("Cannot bring map files online because a failures directory already exists: " + failuresDir);
                    throw new IOException("Cannot bring map files online because a failures directory already exists: " + failuresDir);
                }
                fileSystem.mkdirs(failuresPath);
                
                // import the directory
                log.info("Bringing Map Files online for " + tableName);
                tops.importDirectory(tableName, tableDir.toString(), failuresDir, false);
                log.info("Completed bringing map files online for " + tableName);
                validateComplete();
            } catch (Exception e) {
                log.error("Error importing files into table " + tableName + " from directory " + mapFilesDir, e);
                this.exception = e;
            } finally {
                this.complete = true;
                synchronized (this) {
                    this.notifyAll();
                }
            }
        }
        
        private void collapseDirectory() throws IOException {
            collapseDirectory(tableDir);
        }
        
        private void collapseDirectory(Path dir) throws IOException {
            // collapse any subdirectories, and then collapse those to the top level
            FileSystem fileSystem = FileSystem.get(srcHdfs, new Configuration());
            for (FileStatus file : fileSystem.listStatus(dir)) {
                if (file.isDirectory()) {
                    Path filePath = file.getPath();
                    log.warn("Found an unexpected subdirectory " + filePath + ".  Collapsing into " + tableDir + ".");
                    collapseDirectory(filePath);
                    for (FileStatus subFile : fileSystem.listStatus(filePath)) {
                        Path subFilePath = subFile.getPath();
                        Path destFilePath = new Path(tableDir, subFilePath.getName());
                        // if the dest file already exists, then check if it is the same file
                        if (fileSystem.exists(destFilePath)) {
                            FileChecksum subFileCheckSum = fileSystem.getFileChecksum(subFilePath);
                            FileChecksum destFileCheckSum = fileSystem.getFileChecksum(destFilePath);
                            if (subFileCheckSum.equals(destFileCheckSum)) {
                                log.info(subFilePath + " and " + destFilePath + " are identical, removing the former");
                                fileSystem.delete(subFilePath, false);
                            } else {
                                // Attempt to rename the file instead of failing
                                destFilePath = new Path(tableDir, getNextName(subFilePath.getName()));
                                while (fileSystem.exists(destFilePath)) {
                                    destFilePath = new Path(tableDir, getNextName(destFilePath.getName()));
                                }
                                log.info("Renaming " + subFilePath + " to " + destFilePath);
                                fileSystem.rename(subFilePath, destFilePath);
                            }
                        } else {
                            log.info("Renaming " + subFilePath + " to " + destFilePath);
                            fileSystem.rename(subFilePath, destFilePath);
                        }
                    }
                    // verify the directory is empty
                    if (fileSystem.listStatus(filePath).length > 0) {
                        log.fatal("Failed to collapse subdirectory " + filePath);
                        throw new IOException("Failed to collapse subdirectory " + filePath);
                    }
                    fileSystem.delete(filePath, false);
                }
            }
        }
        
        /**
         * Return a rfile with .1 appended before the extension. {@code foo.ext -> foo.1.ext foo -> foo.1}
         *
         * @param rfile
         * @return a rfile with .1 appended before the extension
         */
        private String getNextName(String rfile) {
            int index = rfile.lastIndexOf('.');
            if (index < 0) {
                return rfile + ".1";
            } else {
                return rfile.substring(0, index) + ".1" + rfile.substring(index);
            }
        }
        
        private void validateComplete() throws IOException {
            FileSystem fileSystem = FileSystem.get(srcHdfs, new Configuration());
            if (fileSystem.listStatus(tableDir).length > 0) {
                log.fatal("Failed to completely import " + tableDir);
                throw new IOException("Failed to completely import " + tableDir);
            }
        }
    }
    
    /**
     * Verify there are no RFiles left behind. If there are, then we need to throw an exception to ensure we fail this bulk load and the directory is not
     * removed.
     */
    public void verifyNothingLeftBehind(Path mapFilesDir) throws IOException {
        verifyNothingLeftBehind(getFileSystem(destHdfs), mapFilesDir.getParent());
    }
    
    protected void verifyNothingLeftBehind(FileSystem fs, Path dir) throws IOException {
        for (FileStatus file : fs.listStatus(dir)) {
            if (file.isDirectory()) {
                verifyNothingLeftBehind(fs, file.getPath());
            } else if (file.getPath().getName().endsWith(".rf")) {
                throw new IOException("Found an RFile left behind, failing bulk load: " + file.getPath());
            }
        }
    }
    
    /**
     * Cleans up a job directory. If the process to bring map files online was successful, then the job directory and map files directory are removed.
     * Otherwise, they are just marked as failed.
     */
    public void cleanUpJobDirectory(Path mapFilesDir) throws IOException {
        Path jobDirectory = mapFilesDir.getParent();
        
        FileSystem destFs = getFileSystem(destHdfs);
        
        FileStatus[] failedDirs = destFs.globStatus(new Path(mapFilesDir, "failures/*/*"));
        boolean jobSucceeded = failedDirs == null || failedDirs.length == 0;
        if (jobSucceeded) {
            markSourceFilesLoaded(jobDirectory);
            // delete the successfully loaded map files directory and its parent directory
            destFs.delete(jobDirectory, true);
        } else {
            log.error("There were failures bringing map files online.  See: failed." + mapFilesDir.getName() + "failures/* for details");
            
            // rename the map files directory
            boolean success = destFs.rename(mapFilesDir, new Path(mapFilesDir.getParent(), "failed." + mapFilesDir.getName()));
            if (!success)
                log.error("Unable to rename map files directory " + destFs.getUri() + " " + mapFilesDir + " to failed." + mapFilesDir.getName());
            
            // create the job.failed file (renamed from job.loading if possible)
            success = destFs.rename(new Path(jobDirectory, LOADING_FILE_MARKER), new Path(jobDirectory, FAILED_FILE_MARKER));
            if (!success) {
                success = destFs.createNewFile(new Path(jobDirectory, FAILED_FILE_MARKER));
                if (!success)
                    log.error("Unable to create " + FAILED_FILE_MARKER + " file in " + jobDirectory);
            }
        }
        
        if (cleanUpScript != null) {
            Process proc = Runtime.getRuntime().exec(new String[] {cleanUpScript, jobDirectory.toString(), "" + jobSucceeded});
            int code;
            try {
                code = proc.waitFor();
            } catch (InterruptedException ex) {
                log.error("Cleanup script interrupted. ");
                code = -1;
            }
            if (code != 0) {
                log.error("Error occurred running external cleanup script: " + cleanUpScript);
            }
        }
        
    }
    
    /**
     * Marks {@code jobDirectory} as failed (in the source filesystem) so that the loader won't try again to load the map files in this job directory.
     */
    public boolean takeOwnershipJobDirectory(Path jobDirectory) {
        boolean success = false;
        try {
            FileSystem fs = getFileSystem(srcHdfs);
            
            try {
                success = fs.rename(new Path(jobDirectory, COMPLETE_FILE_MARKER), new Path(jobDirectory, LOADING_FILE_MARKER));
                log.info("Renamed " + jobDirectory + '/' + COMPLETE_FILE_MARKER + " to " + LOADING_FILE_MARKER);
            } catch (IOException e2) {
                log.error("Exception while marking " + jobDirectory + " for loading: " + e2.getMessage(), e2);
            }
            
            // if not successful, see if we can provide a reason
            if (!success) {
                if (fs.exists(new Path(jobDirectory, LOADING_FILE_MARKER))) {
                    log.info("Another process already took ownership of " + jobDirectory + " for loading");
                } else {
                    log.error("Unable to take ownership of " + jobDirectory + " for loading");
                }
            } else {
                if (!fs.exists(new Path(jobDirectory, LOADING_FILE_MARKER))) {
                    // if the loading file marker does not exist, then we did not really succeed....hadoop strangeness?
                    log.error("Rename returned success but yet we did not take ownership of " + jobDirectory + " (" + LOADING_FILE_MARKER + " does not exist)");
                    success = false;
                } else if (fs.exists(new Path(jobDirectory, COMPLETE_FILE_MARKER))) {
                    // if the complete file still exists, then perhaps the IngestJob received a create failure and subsequently reattempted.
                    log.error("Rename returned success but yet we did not fully take ownership of " + jobDirectory + " (" + COMPLETE_FILE_MARKER + " moved to "
                                    + LOADING_FILE_MARKER + " but " + COMPLETE_FILE_MARKER + " still exists)");
                    success = false;
                    // move the job.loading out of the way. I don't want to delete any files just in case hadoop is getting confused
                    // and a delete might result in both files deleted and then we might think this is simply a failed distcp finally
                    // resulting in lost data.
                    int count = 0;
                    boolean done = false;
                    while (!done && fs.exists(new Path(jobDirectory, COMPLETE_FILE_MARKER)) && count < 10) {
                        count++;
                        if (fs.rename(new Path(jobDirectory, LOADING_FILE_MARKER), new Path(jobDirectory, ATTEMPT_FILE_MARKER + '.' + count))) {
                            log.error("Moved " + LOADING_FILE_MARKER + " to " + ATTEMPT_FILE_MARKER + '.' + count);
                            done = true;
                        }
                    }
                }
            }
        } catch (IOException e) {
            log.error("Exception while marking " + jobDirectory + " for loading: " + e.getMessage(), e);
        }
        return success;
    }
    
    /**
     * Marks {@code jobDirectory} as failed (in the source filesystem) so that the loader won't try again to load the map files in this job directory. If we
     * were successfully distCped over, then this will fail but that is OK because it no longer in the source filesystem.
     */
    public boolean markJobDirectoryFailed(URI workingHdfs, Path jobDirectory) {
        boolean success = false;
        try {
            FileSystem fs = getFileSystem(workingHdfs);
            success = fs.rename(new Path(jobDirectory, LOADING_FILE_MARKER), new Path(jobDirectory, FAILED_FILE_MARKER));
            if (!success) {
                success = fs.createNewFile(new Path(jobDirectory, FAILED_FILE_MARKER));
                if (!success)
                    log.error("Unable to create " + FAILED_FILE_MARKER + " file in " + jobDirectory);
            }
        } catch (IOException e) {
            log.error("Exception while marking " + jobDirectory + " as failed: " + e.getMessage(), e);
        }
        return success;
    }
    
    public void markSourceFilesLoaded(Path jobDirectory) throws IOException {
        ArrayList<String> files = new ArrayList<>();
        
        final FileSystem destFs = getFileSystem(destHdfs);
        try (BufferedReader rdr = new BufferedReader(new InputStreamReader(destFs.open(new Path(jobDirectory, INPUT_FILES_MARKER))))) {
            String line;
            while ((line = rdr.readLine()) != null) {
                files.add(line);
            }
            
        }
        
        final FileSystem sourceFs = getFileSystem(seqFileHdfs);
        List<Callable<Boolean>> renameCallables = Lists.newArrayList();
        
        for (final String file : files) {
            
            renameCallables.add(() -> {
                if (file.contains("/flagged/")) {
                    Path dst = new Path(file.replaceFirst("/flagged/", "/loaded/"));
                    boolean mkdirs = sourceFs.mkdirs(dst.getParent());
                    if (mkdirs) {
                        boolean renamed = false;
                        try {
                            renamed = sourceFs.rename(new Path(file), dst);
                        } catch (Exception e) {
                            log.warn("Exception renaming " + file + " to " + dst, e);
                            renamed = false;
                        }
                        if (!renamed) {
                            // if the file is already in loaded and not in flagged,
                            // then we do not need to fail here
                            boolean flaggedExists = sourceFs.exists(new Path(file));
                            boolean loadedExists = sourceFs.exists(dst);
                            if (flaggedExists || !loadedExists) {
                                throw new IOException("Unable to rename " + file + " (exists=" + flaggedExists + ") to " + dst + " (exists=" + loadedExists
                                                + ")");
                            } else {
                                log.warn("File was already moved to loaded: " + dst);
                                renamed = true;
                            }
                        }
                        return Boolean.valueOf(renamed);
                    } else {
                        throw new IOException("Unable to create parent dir " + dst.getParent());
                    }
                    
                }
                return Boolean.valueOf(false);
            });
        }
        try {
            log.info("Marking " + renameCallables.size() + " sequence files from flagged to loaded");
            
            if (!renameCallables.isEmpty()) {
                List<Future<Boolean>> execResults = executor.invokeAll(renameCallables);
                
                for (Future<Boolean> future : execResults) {
                    if (future.get() == null)
                        throw new IOException("Error while attempting to mark job as loaded");
                }
            }
            
        } catch (InterruptedException e) {
            if (null != e.getCause())
                throw new IOException(e.getCause().getMessage());
            else
                throw new IOException(e);
        } catch (ExecutionException e) {
            if (null != e.getCause())
                throw new IOException(e.getCause().getMessage());
            else
                throw new IOException(e);
        }
    }
    
    private void writeStats(Path[] jobDirectories) throws IOException {
        if (!INGEST_METRICS) {
            log.info("ingest metrics disabled");
        } else {
            long now = System.currentTimeMillis();
            for (Path p : jobDirectories)
                reporter.getCounter("MapFileLoader.EndTimes", p.getName()).increment(now);
            // Write out the metrics.
            // We are going to serialize the counters into a file in HDFS.
            // The context was set in the processKeyValues method below, and should not be null. We'll guard against NPE anyway
            FileSystem fs = getFileSystem(seqFileHdfs);
            RawLocalFileSystem rawFS = new RawLocalFileSystem();
            rawFS.setConf(conf);
            CompressionCodec cc = new GzipCodec();
            CompressionType ct = CompressionType.BLOCK;
            
            Counters c = reporter.getCounters();
            if (null != c && c.countCounters() > 0) {
                // Serialize the counters to a file in HDFS.
                Path src = new Path(File.createTempFile("MapFileLoader", ".metrics").getAbsolutePath());
                Writer writer = SequenceFile.createWriter(conf, Writer.file(rawFS.makeQualified(src)), Writer.keyClass(NullWritable.class),
                                Writer.valueClass(Counters.class), Writer.compression(ct, cc));
                writer.append(NullWritable.get(), c);
                writer.close();
                
                // Now we will try to move the file to HDFS.
                // Copy the file to the temp dir
                try {
                    Path mDir = new Path(workDir, "MapFileLoaderMetrics");
                    if (!fs.exists(mDir))
                        fs.mkdirs(mDir);
                    Path dst = new Path(mDir, src.getName());
                    log.info("Copying file " + src + " to " + dst);
                    fs.copyFromLocalFile(false, true, src, dst);
                    // If this worked, then remove the local file
                    rawFS.delete(src, false);
                    // also remove the residual crc file
                    rawFS.delete(getCrcFile(src), false);
                } catch (IOException e) {
                    // If an error occurs in the copy, then we will leave in the local metrics directory.
                    log.error("Error copying metrics file into HDFS, will remain in metrics directory.");
                }
                
                // reset reporter so that old metrics don't persist over time
                this.reporter = new StandaloneStatusReporter();
            }
        }
    }
    
    private Path getCrcFile(Path path) {
        return new Path(path.getParent(), "." + path.getName() + ".crc");
    }
    
    private void sleep() {
        try {
            System.gc();
            Thread.sleep(SLEEP_TIME);
        } catch (InterruptedException e) {
            log.warn("Interrupted while sleeping.", e);
        }
    }
    
}
