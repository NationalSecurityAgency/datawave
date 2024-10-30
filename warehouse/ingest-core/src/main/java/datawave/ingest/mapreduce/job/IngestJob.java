package datawave.ingest.mapreduce.job;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Observer;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.io.comparator.LastModifiedFileComparator;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.springframework.util.StopWatch;

import datawave.ingest.config.TableConfigCache;
import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.ConfigurationHelper;
import datawave.ingest.data.config.DataTypeHelper;
import datawave.ingest.data.config.ingest.AccumuloHelper;
import datawave.ingest.input.reader.event.EventSequenceFileInputFormat;
import datawave.ingest.mapreduce.EventMapper;
import datawave.ingest.mapreduce.handler.JobSetupHandler;
import datawave.ingest.mapreduce.handler.shard.NumShards;
import datawave.ingest.mapreduce.job.reduce.BulkIngestKeyAggregatingReducer;
import datawave.ingest.mapreduce.job.reduce.BulkIngestKeyDedupeCombiner;
import datawave.ingest.mapreduce.job.statsd.CounterStatsDClient;
import datawave.ingest.mapreduce.job.statsd.CounterToStatsDConfiguration;
import datawave.ingest.mapreduce.job.writer.AbstractContextWriter;
import datawave.ingest.mapreduce.job.writer.AggregatingContextWriter;
import datawave.ingest.mapreduce.job.writer.BulkContextWriter;
import datawave.ingest.mapreduce.job.writer.ChainedContextWriter;
import datawave.ingest.mapreduce.job.writer.ContextWriter;
import datawave.ingest.mapreduce.job.writer.DedupeContextWriter;
import datawave.ingest.mapreduce.job.writer.LiveContextWriter;
import datawave.ingest.mapreduce.job.writer.TableCachingContextWriter;
import datawave.ingest.mapreduce.partition.MultiTableRangePartitioner;
import datawave.ingest.metric.IngestInput;
import datawave.ingest.metric.IngestOutput;
import datawave.ingest.metric.IngestProcess;
import datawave.marking.MarkingFunctions;
import datawave.util.StringUtils;
import datawave.util.cli.PasswordConverter;

/**
 * Class that starts a MapReduce job to create Accumulo Map files that to be bulk imported into Accumulo If outputMutations is specified, then Mutations are
 * created instead which will modify accumulo directly instead of using Accumulo Map files (e.g. use for live ingest). If mapOnly is specified (only valid for
 * live ingest), then the combiner and reducers will be run as part of the map process. Beware that potentially more data may be cached in memory when doing
 * mapOnly processing. This will only be an issue if something like the EdgeDataTypeHandler produces an unreasonable number of edges for one event. The general
 * sequence of events is as follows:
 * <p>
 * EventSequenceFileInputFormat produces an EventSequenceFileReader to read files of Event objects EventMapper used in map phase which calls processBulk on
 * DataTypeHelper implementations to produce BulkIngestKey,Value pairs BulkIngestDedupeCombiner is invoked from the DedupeContextWriter to primarily dedupe
 * BulkIngestKey,Value pairs if not running a mapOnly job, then the Delegating Partitioner will run, using the Partitioners that are configured for each table
 * or the default Partitioner if none is specified for a table. BulkIngestAggregatingReducer is used as the reducer (or invoked from the
 * AggregatingContextWriter in mapOnly mode) to produce dedupped BulkIngestKey,Value pairs The BulkContextWriter or the LiveContextWriter are at all stages to
 * write data to the context in the appropriate format For bulk ingest the MultiRFileOutputFormatter is then used to format the output which is placed in the
 * {@code <workDir>/mapFiles} directory For live ingest the AccumuloOutputFormat is then used to apply the mutations directly to accumulo
 */
public class IngestJob implements Tool {

    public static final String DAEMON_PROCESSES_PROPERTY = "accumulo.ingest.daemons";
    public static final String REDUCE_TASKS_ARG_PREFIX = "-mapreduce.job.reduces=";

    protected boolean eventProcessingError = false;
    protected Logger log = Logger.getLogger("datawave.ingest");
    private ConsoleAppender ca = new ConsoleAppender();

    protected ArrayList<String[]> confOverrides = new ArrayList<>();
    protected int reduceTasks = 0;
    protected String inputPaths = null;
    // inputFileLists denotes whether the files in inputPaths are the files to process, or lists of files to process (one per line).
    protected boolean inputFileLists = false;
    // inputFileListMarker if set is a marker line to look for before treating the lines as files. Useful for our flag files
    // where the initial part of the file is our script line and the remainder contains the actual file list
    protected String inputFileListMarker = null;
    protected String idFilterFsts = null;

    protected String workDir = null;
    protected Set<String> tableNames = null;
    protected String flagFile = null;
    protected String flagFileDir = null;
    protected String flagFilePattern = null;
    protected String cacheBaseDir = "/data/BulkIngest/jobCache";
    protected float markerFileReducePercentage = 0.33f;
    protected boolean markerFileFIFO = true;
    protected boolean generateMarkerFile = true;
    protected String pipelineId = null;
    protected boolean outputMutations = false;
    protected boolean useMapOnly = false;
    protected boolean useCombiner = false;
    protected boolean useInlineCombiner = false;
    protected boolean verboseCounters = false;
    protected boolean tableCounters = false;
    protected boolean fileNameCounters = true;
    protected boolean contextWriterCounters = false;
    protected boolean disableSpeculativeExecution = false;
    protected boolean enableBloomFilters = false;
    protected boolean collectDistributionStats = false;
    protected boolean createTables = false;
    protected boolean metricsOutputEnabled = true;
    private String metricsLabelOverride = null;
    protected boolean generateMapFileRowKeys = false;
    protected String compressionType = null;
    protected final Set<String> compressionTableDisallowList = new HashSet<>();
    protected int maxRFileEntries = 0;
    protected long maxRFileSize = 0;
    @SuppressWarnings("rawtypes")
    protected Class<? extends InputFormat> inputFormat = EventSequenceFileInputFormat.class;
    @SuppressWarnings("rawtypes")
    protected Class<? extends Mapper> mapper = EventMapper.class;

    protected String instanceName = null;
    protected String zooKeepers = null;
    protected String userName = null;
    protected byte[] password = null;

    protected URI srcHdfs = null;
    protected URI destHdfs = null;
    protected String distCpConfDir = null;
    protected int distCpBandwidth = 512;
    protected int distCpMaxMaps = 200;
    protected String distCpStrategy = "dynamic";
    protected boolean deleteAfterDistCp = true;

    protected ArrayList<Path> jobDependencies = new ArrayList<>();

    protected boolean writeDirectlyToDest = false;

    private Configuration hadoopConfiguration;
    private List<Observer> jobObservers = new ArrayList<>();
    private JobObservable jobObservable;

    public static void main(String[] args) throws Exception {
        System.out.println("Running main");
        System.exit(ToolRunner.run(null, new IngestJob(), args));
    }

    protected void printUsage() {
        System.out.println("Usage: " + getClass().getSimpleName() + " inputpath configfile configfile");
        System.out.println("                     -user username -pass password -instance instanceName");
        System.out.println("                     -zookeepers host[,host,host] -workDir directoryName -flagFileDir directoryName");
        System.out.println("                     [-inputFileLists] [-inputFileListMarker marker]");
        System.out.println("                     [-srcHdfs srcFileSystemURI] [-destHdfs destFileSystemURI]");
        System.out.println("                     [-distCpConfDir distCpHadoopConfDir] [-distCpBandwidth bandwidth]");
        System.out.println("                     [-distCpMaxMaps maps] [-distCpStrategy strategy]");
        System.out.println("                     [-writeDirectlyToDest]");
        System.out.println("                     [-createTables]");
        System.out.println("                     [-doNotDeleteAfterDistCp]");
        System.out.println("                     [-idFilterFsts comma-separated-list-of-files]");
        System.out.println("                     [-inputFormat inputFormatClass]");
        System.out.println("                     [-mapper mapperClass]");
        System.out.println("                     [-splitsCacheTimeoutMs timeout]");
        System.out.println("                     [-disableRefreshSplits]");
        System.out.println("                     [-splitsCacheDir /path/to/directory]");
        System.out.println("                     [-accumuloConfigCachePath /path/to/file]");
        System.out.println("                     [-cacheBaseDir baseDir] [-cacheJars jar,jar,...]");
        System.out.println("                     [-multipleNumShardsCacheDir /path/to/directory]");
        System.out.println("                     [-skipMarkerFileGeneration] [-markerFileLIFO]");
        System.out.println("                     [-markerFileReducePercentage float_in_0_to_1]");
        System.out.println("                     [-pipelineId id]");
        System.out.println("                     [-flagFile flagFile]");
        System.out.println("                     [-flagFilePattern flagFilePattern]");
        System.out.println("                     [-outputMutations]");
        System.out.println("                     [-mapreduce.job.reduces=numReducers]");
        System.out.println("                     [-disableSpeculativeExecution] [-mapOnly] [-useCombiner] [-useInlineCombiner]");
        System.out.println("                     [-verboseCounters]");
        System.out.println("                     [-tableCounters] [-contextWriterCounters] [-noFileNameCounters]");
        System.out.println("                     [-generateMapFileRowKeys]");
        System.out.println("                     [-enableBloomFilters]");
        System.out.println("                     [-collectDistributionStats]");
        System.out.println("                     [-ingestMetricsDisabled]");
        System.out.println("                     [-ingestMetricsLabel label]");
        System.out.println("                     [-compressionType lzo|gz|zstd]");
        System.out.println("                     [-compressionTableDisallowList table,table,...");
        System.out.println("                     [-maxRFileUndeduppedEntries maxEntries]");
        System.out.println("                     [-maxRFileUncompressedSize maxSize]");
        System.out.println("                     [-jobObservers jobObserverClasses]");
        System.out.println("                     [-shardedMapFiles table1=/hdfs/path/table1splits.seq[,table2=/hdfs/path/table2splits.seq] ]");
    }

    @Override
    public int run(String[] args) throws Exception {

        long start = System.currentTimeMillis();

        StopWatch sw = new StopWatch("Ingest Job");
        sw.start("local init");

        Logger.getLogger(TypeRegistry.class).setLevel(Level.ALL);

        ca.setLayout(new PatternLayout("%p [%c{1}] %m%n"));
        ca.setThreshold(Level.INFO);
        log.addAppender(ca);
        log.setLevel(Level.INFO);

        // Initialize the markings file helper so we get the right markings file
        MarkingFunctions.Factory.createMarkingFunctions();
        TypeRegistry.reset();

        // Parse the job arguments
        Configuration conf = parseArguments(args, this.getConf());

        if (conf == null) {
            printUsage();
            return -1;
        }

        updateConfWithOverrides(conf);

        jobObservable = new JobObservable(srcHdfs != null ? getFileSystem(conf, srcHdfs) : null);
        for (Observer observer : jobObservers) {
            this.jobObservable.addObserver(observer);
            if (observer instanceof Configurable) {
                log.info("Applying configuration to observer");
                ((Configurable) observer).setConf(conf);
            }
        }

        AccumuloHelper cbHelper = new AccumuloHelper();
        cbHelper.setup(conf);

        TypeRegistry.getInstance(conf);

        log.info(conf.toString());
        log.info(String.format("getStrings('%s') = %s", TypeRegistry.INGEST_DATA_TYPES, conf.get(TypeRegistry.INGEST_DATA_TYPES)));
        log.info(String.format("getStrings('data.name') = %s", conf.get("data.name")));
        int index = 0;
        for (String name : TypeRegistry.getTypeNames()) {
            log.info(String.format("name[%d] = '%s'", index++, name));
        }

        if (TypeRegistry.getTypes().isEmpty()) {
            log.error("No data types were configured");
            return -1;
        }

        this.tableNames = setupAndCacheTables(conf, createTables);
        if (createTables) {
            log.info("Created tables: " + tableNames + " successfully!");
        }

        // get the source and output hadoop file systems
        FileSystem inputFs = getFileSystem(conf, srcHdfs);
        FileSystem outputFs = (writeDirectlyToDest ? getFileSystem(conf, destHdfs) : inputFs);
        conf.set("output.fs.uri", outputFs.getUri().toString());

        // get the qualified work directory path
        Path unqualifiedWorkPath = Path.getPathWithoutSchemeAndAuthority(new Path(workDir));
        conf.set("ingest.work.dir.unqualified", unqualifiedWorkPath.toString());
        Path workDirPath = new Path(new Path(writeDirectlyToDest ? destHdfs : srcHdfs), unqualifiedWorkPath);
        conf.set("ingest.work.dir.qualified", workDirPath.toString());

        // Create the Job
        Job job = Job.getInstance(conf);
        // Job copies the configuration, so any changes made after this point don't get captured in the job.
        // Use the job's configuration from this point.
        conf = job.getConfiguration();

        setupHandlers(conf);
        if (!useMapOnly || !outputMutations) {
            // Calculate the sampled splits, splits file, and set up the partitioner, but not if only doing only a map phase and outputting mutations
            // if not outputting mutations and only doing a map phase, we still need to go through this logic as the MultiRFileOutputFormatter
            // depends on this.
            try {
                configureBulkPartitionerAndOutputFormatter(job, cbHelper, conf, outputFs);
            } catch (Exception e) {
                log.error(e);
                log.info("Deleting orphaned directory: " + workDirPath);
                try {
                    outputFs.delete(workDirPath, true);
                } catch (Exception er) {
                    log.error("Unable to remove directory: " + workDirPath, er);
                }
                return -1;
            }
        }

        job.setJarByClass(this.getClass());
        for (Path inputPath : getFilesToProcess(inputFs, inputFileLists, inputFileListMarker, inputPaths)) {
            FileInputFormat.addInputPath(job, inputPath);
        }
        for (Path dependency : jobDependencies)
            job.addFileToClassPath(dependency);

        configureInputFormat(job, cbHelper, conf);

        configureJob(job, conf, workDirPath, outputFs);

        // Log configuration
        log.info("Types: " + TypeRegistry.getTypeNames());
        log.info("Tables: " + tableNames);
        log.info("InputFormat: " + job.getInputFormatClass().getName());
        log.info("Mapper: " + job.getMapperClass().getName());
        log.info("Reduce tasks: " + (useMapOnly ? 0 : reduceTasks));
        log.info("Split File: " + conf.get(TableSplitsCache.SPLITS_CACHE_DIR) + "/"
                        + conf.get(TableSplitsCache.SPLITS_CACHE_FILE, TableSplitsCache.DEFAULT_SPLITS_CACHE_FILE));

        // Note that if we run any other jobs in the same vm (such as a sampler), then we may
        // need to catch and throw away an exception here
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory(conf));

        startDaemonProcesses(conf);
        sw.stop();
        log.info(formatTaskInfo(sw.getLastTaskInfo()));
        sw.start("AM Init");

        job.submit();
        JobID jobID = job.getJobID();
        log.info("JOB ID: " + jobID);

        createFileWithRetries(outputFs, new Path(workDirPath, jobID.toString()));

        // Wait for reduce progress to pass the 30% mark and then
        // kick off the next job of this type.
        boolean done = false;
        while (generateMarkerFile && !done && !job.isComplete()) {
            if (job.reduceProgress() > markerFileReducePercentage) {
                File flagDir = new File(flagFileDir);
                if (flagDir.isDirectory()) {
                    // Find flag files that start with this datatype
                    RegexFileFilter filter;
                    if (flagFilePattern != null) {
                        filter = new RegexFileFilter(flagFilePattern);
                    } else {
                        filter = new RegexFileFilter(outputMutations ? ".*_(live|fivemin)_.*\\.flag" : ".*_(bulk|onehr)_.*\\.flag");
                    }
                    File[] flagFiles = flagDir.listFiles((FilenameFilter) filter);
                    if (flagFiles.length > 0) {
                        // Reverse sort by time to get the earliest file
                        Comparator<File> comparator = LastModifiedFileComparator.LASTMODIFIED_COMPARATOR;
                        if (!markerFileFIFO) {
                            comparator = LastModifiedFileComparator.LASTMODIFIED_REVERSE;
                        }
                        Arrays.sort(flagFiles, comparator);
                        // Just grab the first one and rename it to .marker
                        File flag = flagFiles[0];
                        File targetFile = new File(flag.getAbsolutePath() + (pipelineId == null ? "" : '.' + pipelineId) + ".marker");
                        if (!flag.renameTo(targetFile)) {
                            log.error("Unable to rename flag file: " + flag.getAbsolutePath());
                            continue;
                        }
                        log.info("Renamed flag file " + flag + " to " + targetFile);
                    } else {
                        log.info("No more flag files to process");
                        // + datatype);
                    }
                } else {
                    log.error("Flag file directory does not exist: " + flagFileDir);
                }
                done = true;
            } else {
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException ie) {
                    // do nothing
                }
            }
        }
        sw.stop();
        log.info(formatTaskInfo(sw.getLastTaskInfo()));

        sw.start("MR Job");
        job.waitForCompletion(true);
        sw.stop();

        long stop = System.currentTimeMillis();

        log.info(formatTaskInfo(sw.getLastTaskInfo()));
        log.info(sw.prettyPrint());

        // output the counters to the log
        Counters counters = job.getCounters();
        try (JobClient jobClient = new JobClient((org.apache.hadoop.mapred.JobConf) job.getConfiguration())) {
            RunningJob runningJob = jobClient.getJob(new org.apache.hadoop.mapred.JobID(jobID.getJtIdentifier(), jobID.getId()));

            // If the job failed, then don't bring the map files online.
            if (!job.isSuccessful()) {
                return jobFailed(job, runningJob, outputFs, workDirPath);
            }

            // determine if we had processing errors
            if (counters.findCounter(IngestProcess.RUNTIME_EXCEPTION).getValue() > 0) {
                eventProcessingError = true;
                log.error("Found Runtime Exceptions in the counters");
                long numExceptions = 0;
                long numRecords = 0;
                CounterGroup exceptionCounterGroup = counters.getGroup(IngestProcess.RUNTIME_EXCEPTION.name());
                for (Counter exceptionC : exceptionCounterGroup) {
                    numExceptions += exceptionC.getValue();
                }
                CounterGroup recordCounterGroup = counters.getGroup(IngestOutput.EVENTS_PROCESSED.name());
                for (Counter recordC : recordCounterGroup) {
                    numRecords += recordC.getValue();
                }
                // records that throw runtime exceptions are still counted as processed
                float percentError = (numRecords == 0) ? 0.0f : 100 * ((float) numExceptions / numRecords);
                log.info(String.format("Percent Error: %.2f", percentError));
                if (conf.getInt("job.percent.error.threshold", 101) <= percentError) {
                    return jobFailed(job, runningJob, outputFs, workDirPath);
                }
            }
        }
        if (counters.findCounter(IngestInput.EVENT_FATAL_ERROR).getValue() > 0) {
            eventProcessingError = true;
            log.error("Found Fatal Errors in the counters");
        }

        // If we're doing "live" ingest (sending mutations to accumulo rather than
        // bringing map files online), then simply delete the workDir since it
        // doesn't contain anything we need. If we are doing bulk ingest, then
        // write out a marker file to indicate that the job is complete and a
        // separate process will bulk import the map files.
        if (outputMutations) {
            markFilesLoaded(inputFs, FileInputFormat.getInputPaths(job), job.getJobID());
            boolean deleted = outputFs.delete(workDirPath, true);
            if (!deleted) {
                log.error("Unable to remove job working directory: " + workDirPath);
            }
        } else {
            // now move the job directory over to the warehouse if needed
            FileSystem destFs = getFileSystem(conf, destHdfs);

            if (!inputFs.equals(destFs) && !writeDirectlyToDest) {
                Configuration distCpConf = conf;
                // Use the configuration dir specified on the command-line for DistCP if necessary.
                // Basically this means pulling in all of the *-site.xml config files from the specified
                // directory. By adding these resources last, their properties will override those in the
                // current config.
                if (distCpConfDir != null) {
                    distCpConf = new Configuration(false);
                    FilenameFilter ff = (dir, name) -> name.toLowerCase().endsWith("-site.xml");
                    for (String file : new File(distCpConfDir).list(ff)) {
                        Path path = new Path(distCpConfDir, file);
                        distCpConf.addResource(file.replace("-site", "-default"));
                        distCpConf.addResource(path);
                    }
                }

                log.info("Moving (using distcp) " + unqualifiedWorkPath + " from " + inputFs.getUri() + " to " + destFs.getUri());
                try {
                    distCpDirectory(unqualifiedWorkPath, inputFs, destFs, distCpConf, deleteAfterDistCp);
                } catch (Exception e) {
                    log.error("Failed to move job directory over to the warehouse.", e);
                    return -3;
                }
            }

            Path destWorkDirPath = FileSystem.get(destHdfs, conf).makeQualified(unqualifiedWorkPath);
            boolean marked = markJobComplete(destFs, destWorkDirPath);
            if (!marked) {
                log.error("Failed to create marker file indicating job completion.");
                return -3;
            }
        }

        // if we had a failure writing the metrics, or we have event processing errors, then return -5
        // this should result in administrators getting an email, but the job will be considered successful

        if (metricsOutputEnabled) {
            log.info("Writing Stats");
            Path statsDir = new Path(unqualifiedWorkPath.getParent(), "IngestMetrics");
            if (!writeStats(log, job, jobID, counters, start, stop, outputMutations, inputFs, statsDir, this.metricsLabelOverride)) {
                log.warn("Failed to output statistics for the job");
                return -5;
            }
        } else {
            log.info("Ingest stats output disabled via 'ingestMetricsDisabled' flag");
        }

        if (eventProcessingError) {
            log.warn("Job had processing errors.  See counters for more information");
            return -5;
        }

        return 0;
    }

    private String formatTaskInfo(StopWatch.TaskInfo taskInfo) {
        return "Timing - " + taskInfo.getTaskName() + ": " + taskInfo.getTimeSeconds() + " s";
    }

    private void setupHandlers(Configuration conf) {
        // default to all types
        Collection<Type> types = TypeRegistry.getTypes();

        // when an override is specified only load handlers associated with that data type
        String override = conf.get(DataTypeHelper.Properties.DATA_NAME_OVERRIDE);
        if (override != null) {
            types = Collections.singleton(TypeRegistry.getType(override));
        }

        for (Type t : types) {
            String[] handlers = t.getDefaultDataTypeHandlers();
            if (handlers != null) {
                for (String handler : handlers) {
                    try {
                        Object o = Class.forName(handler).getDeclaredConstructor().newInstance();
                        if (o instanceof JobSetupHandler) {
                            JobSetupHandler setupHandler = (JobSetupHandler) o;
                            conf.set(DataTypeHelper.Properties.DATA_NAME, t.typeName());
                            setupHandler.setup(conf);
                        }
                    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
                        log.warn("Could not setup handler: " + handler, e);
                    }
                }
            }
        }
    }

    protected Configuration interpolateEnvironment(Configuration conf) {
        // We have set up the Configuration, now replace all instances of ${DATAWAVE_INGEST_HOME} with
        // the value that is set in the environment.
        String ingestHomeValue = System.getenv("DATAWAVE_INGEST_HOME");
        if (null == ingestHomeValue)
            throw new IllegalArgumentException("DATAWAVE_INGEST_HOME must be set in the environment.");

        log.info("Replacing ${DATAWAVE_INGEST_HOME} with " + ingestHomeValue);

        return ConfigurationHelper.interpolate(conf, "\\$\\{DATAWAVE_INGEST_HOME\\}", ingestHomeValue);

    }

    /**
     * Parse the arguments and update the configuration as needed
     *
     * @param args
     *            the args
     * @param conf
     *            the config
     * @return the configuration
     * @throws ClassNotFoundException
     *             if class is not found
     * @throws URISyntaxException
     *             if there are issues with URI syntax
     */
    protected Configuration parseArguments(String[] args, Configuration conf) throws ClassNotFoundException, URISyntaxException, IllegalArgumentException {
        List<String> activeResources = new ArrayList<>();

        inputPaths = args[0];
        log.info("InputPaths is " + inputPaths);
        for (int i = 1; i < args.length; i++) {
            if (args[i].equals("-inputFileLists")) {
                inputFileLists = true;
            } else if (args[i].equals("-inputFileListMarker")) {
                inputFileListMarker = args[++i];
            } else if (args[i].equals("-instance")) {
                instanceName = args[++i];
                AccumuloHelper.setInstanceName(conf, instanceName);
            } else if (args[i].equals("-zookeepers")) {
                zooKeepers = args[++i];
                AccumuloHelper.setZooKeepers(conf, zooKeepers);
            } else if (args[i].equals("-workDir")) {
                workDir = args[++i];
                if (!workDir.endsWith(Path.SEPARATOR)) {
                    workDir = workDir + Path.SEPARATOR;
                }
            } else if (args[i].equals("-user")) {
                userName = args[++i];
                AccumuloHelper.setUsername(conf, userName);
            } else if (args[i].equals("-pass")) {
                password = PasswordConverter.parseArg(args[++i]).getBytes();
                AccumuloHelper.setPassword(conf, password);
            } else if (args[i].equals("-flagFile")) {
                flagFile = args[++i];
            } else if (args[i].equals("-flagFileDir")) {
                flagFileDir = args[++i];
            } else if (args[i].equals("-flagFilePattern")) {
                flagFilePattern = args[++i];
            } else if ("-srcHdfs".equalsIgnoreCase(args[i])) {
                srcHdfs = new URI(args[++i]);
            } else if ("-destHdfs".equalsIgnoreCase(args[i])) {
                destHdfs = new URI(args[++i]);
            } else if ("-distCpConfDir".equalsIgnoreCase(args[i])) {
                distCpConfDir = args[++i];
            } else if ("-distCpBandwidth".equalsIgnoreCase(args[i])) {
                distCpBandwidth = Integer.parseInt(args[++i]);
            } else if ("-distCpMaxMaps".equalsIgnoreCase(args[i])) {
                distCpMaxMaps = Integer.parseInt(args[++i]);
            } else if ("-distCpStrategy".equalsIgnoreCase(args[i])) {
                distCpStrategy = args[++i];
            } else if ("-doNotDeleteAfterDistCp".equalsIgnoreCase(args[i])) {
                deleteAfterDistCp = false;
            } else if ("-writeDirectlyToDest".equalsIgnoreCase(args[i])) {
                writeDirectlyToDest = true;
            } else if ("-filterFsts".equalsIgnoreCase(args[i])) {
                idFilterFsts = args[++i];
            } else if (args[i].equals("-inputFormat")) {
                inputFormat = Class.forName(args[++i]).asSubclass(InputFormat.class);
            } else if (args[i].equals("-mapper")) {
                mapper = Class.forName(args[++i]).asSubclass(Mapper.class);
            } else if (args[i].equals("-splitsCacheTimeoutMs")) {
                conf.set(TableSplitsCacheStatus.SPLITS_CACHE_TIMEOUT_MS, args[++i]);
            } else if (args[i].equals("-disableRefreshSplits")) {
                conf.setBoolean(TableSplitsCache.REFRESH_SPLITS, false);
            } else if (args[i].equals("-splitsCacheDir")) {
                conf.set(TableSplitsCache.SPLITS_CACHE_DIR, args[++i]);
            } else if (args[i].equals("-multipleNumShardsCacheDir")) {
                conf.set(NumShards.MULTIPLE_NUMSHARDS_CACHE_PATH, args[++i]);
            } else if (args[i].equals("-enableAccumuloConfigCache")) {
                conf.setBoolean(TableConfigCache.ACCUMULO_CONFIG_FILE_CACHE_ENABLE_PROPERTY, true);
            } else if (args[i].equalsIgnoreCase("-accumuloConfigCachePath")) {
                conf.set(TableConfigCache.ACCUMULO_CONFIG_CACHE_PATH_PROPERTY, args[++i]);
                conf.setBoolean(TableConfigCache.ACCUMULO_CONFIG_FILE_CACHE_ENABLE_PROPERTY, true);
            } else if (args[i].equals("-disableSpeculativeExecution")) {
                disableSpeculativeExecution = true;
            } else if (args[i].equals("-skipMarkerFileGeneration")) {
                generateMarkerFile = false;
            } else if (args[i].equals("-outputMutations")) {
                outputMutations = true;
            } else if (args[i].equals("-mapOnly")) {
                useMapOnly = true;
                generateMarkerFile = false;
            } else if (args[i].equals("-useCombiner")) {
                useCombiner = true;
            } else if (args[i].equals("-useInlineCombiner")) {
                useInlineCombiner = true;
            } else if (args[i].equals("-pipelineId")) {
                pipelineId = args[++i];
            } else if (args[i].equals("-markerFileReducePercentage")) {
                try {
                    markerFileReducePercentage = Float.parseFloat(args[++i]);
                } catch (NumberFormatException e) {
                    log.error("ERROR: marker file reduce percentage must be a float in [0.0,1.0]");
                    return null;
                }
            } else if (args[i].equals("-markerFileLIFO")) {
                markerFileFIFO = false;
            } else if (args[i].equals("-cacheBaseDir")) {
                cacheBaseDir = args[++i];
            } else if (args[i].equals("-cacheJars")) {
                String[] jars = StringUtils.trimAndRemoveEmptyStrings(args[++i].replaceAll("\\s+", "").split(","));
                for (String jarString : jars) {
                    File jar = new File(jarString);
                    Path file = new Path(cacheBaseDir, jar.getName());
                    log.info("Adding " + file + " to job class path via distributed cache.");
                    jobDependencies.add(file);
                }
            } else if (args[i].equals("-verboseCounters")) {
                verboseCounters = true;
            } else if (args[i].equals("-tableCounters")) {
                tableCounters = true;
            } else if (args[i].equals("-noFileNameCounters")) {
                fileNameCounters = false;
            } else if (args[i].equals("-contextWriterCounters")) {
                contextWriterCounters = true;
            } else if (args[i].equals("-enableBloomFilters")) {
                enableBloomFilters = true;
            } else if (args[i].equals("-collectDistributionStats")) {
                conf.setBoolean(MultiTableRangePartitioner.PARTITION_STATS, true);
            } else if (args[i].equals("-ingestMetricsLabel")) {
                this.metricsLabelOverride = args[++i];
            } else if (args[i].equals("-ingestMetricsDisabled")) {
                this.metricsOutputEnabled = false;
            } else if (args[i].equals("-generateMapFileRowKeys")) {
                generateMapFileRowKeys = true;
            } else if (args[i].equals("-compressionType")) {
                compressionType = args[++i];
            } else if (args[i].equals("-compressionTableDisallowList")) {
                String[] tables = StringUtils.split(args[++i], ',');
                compressionTableDisallowList.addAll(Arrays.asList(tables));
            } else if (args[i].equals("-maxRFileUndeduppedEntries")) {
                maxRFileEntries = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-maxRFileUncompressedSize")) {
                maxRFileSize = Long.parseLong(args[++i]);
            } else if (args[i].equals("-createTables")) {
                createTables = true;
            } else if (args[i].startsWith(REDUCE_TASKS_ARG_PREFIX)) {
                try {
                    reduceTasks = Integer.parseInt(args[i].substring(REDUCE_TASKS_ARG_PREFIX.length(), args[i].length()));
                } catch (NumberFormatException e) {
                    log.error("ERROR: mapred.reduce.tasks must be set to an integer (" + REDUCE_TASKS_ARG_PREFIX + "#)");
                    return null;
                }
            } else if (args[i].equals("-jobObservers")) {
                if (i + 2 > args.length) {
                    log.error("-jobObservers must be followed by a class name");
                    System.exit(-2);
                }
                String jobObserverClasses = args[++i];
                try {
                    String[] classes = jobObserverClasses.split(",");
                    for (String jobObserverClass : classes) {
                        log.info("Adding job observer: " + jobObserverClass);
                        Class clazz = Class.forName(jobObserverClass);
                        Observer o = (Observer) clazz.newInstance();
                        jobObservers.add(o);
                    }
                } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
                    log.error("cannot instantiate job observer class '" + jobObserverClasses + "'", e);
                    System.exit(-2);
                } catch (ClassCastException e) {
                    log.error("cannot cast '" + jobObserverClasses + "' to Observer", e);
                    System.exit(-2);
                }
            } else if (args[i].startsWith("-")) {
                // Configuration key/value entries can be overridden via the command line
                // (taking precedence over entries in *conf.xml files)
                addConfOverride(args[i].substring(1));
            } else {
                log.info("Adding resource " + args[i]);
                conf.addResource(args[i]);
                activeResources.add(args[i]);
            }
        }

        conf = interpolateEnvironment(conf);

        for (String resource : activeResources) {
            conf.addResource(resource);
        }

        // To enable passing the MONITOR_SERVER_HOME environment variable through to the monitor,
        // pull it into the configuration
        String monitorHostValue = System.getenv("MONITOR_SERVER_HOST");
        log.info("Setting MONITOR_SERVER_HOST to " + monitorHostValue);
        if (null != monitorHostValue) {
            conf.set("MONITOR_SERVER_HOST", monitorHostValue);
        }

        if (workDir == null) {
            log.error("ERROR: Must provide a working directory name");
            return null;
        }

        if ((!useMapOnly) && (reduceTasks == 0)) {
            log.error("ERROR: -mapred.reduce.tasks must be set");
            return null;
        }

        if (flagFileDir == null && generateMarkerFile) {
            log.error("ERROR: -flagFileDir must be set");
            return null;
        }

        if (useMapOnly && !outputMutations) {
            log.error("ERROR: Cannot do bulk ingest mapOnly (i.e. without the reduce phase).  Bulk ingest required sorted keys.");
            return null;
        }

        if (!outputMutations && destHdfs == null) {
            log.error("ERROR: -destHdfs must be specified for bulk ingest");
            return null;
        }

        return conf;
    }

    /**
     * Configure the partitioner and the output formatter.
     *
     * @param job
     *            the job
     * @param cbHelper
     *            the accumulo helper
     * @param conf
     *            the config
     * @param outputFs
     *            the file system
     * @throws AccumuloSecurityException
     *             for issues with accumulo security
     * @throws AccumuloException
     *             for general accumulo issues
     * @throws IOException
     *             for issues with read or write
     * @throws URISyntaxException
     *             for issues with URI syntax
     * @throws TableExistsException
     *             if the table already exists
     * @throws TableNotFoundException
     *             if the table is not found
     */
    protected void configureBulkPartitionerAndOutputFormatter(Job job, AccumuloHelper cbHelper, Configuration conf, FileSystem outputFs)
                    throws AccumuloSecurityException, AccumuloException, IOException, URISyntaxException, TableExistsException, TableNotFoundException {
        if (null == conf.get(SplitsFile.SPLIT_WORK_DIR)) {
            conf.set(SplitsFile.SPLIT_WORK_DIR, conf.get("ingest.work.dir.qualified"));
        }
        conf.setInt("splits.num.reduce", this.reduceTasks);
        // used by the output formatter and the sharded partitioner
        long before = System.currentTimeMillis();
        SplitsFile.setupFile(job, conf);
        long after = System.currentTimeMillis();

        log.info("Sharded splits files setup time: " + (after - before) + "ms");

        conf.setInt(MultiRFileOutputFormatter.EVENT_PARTITION_COUNT, this.reduceTasks * 2);
        configureMultiRFileOutputFormatter(conf, compressionType, compressionTableDisallowList, maxRFileEntries, maxRFileSize, generateMapFileRowKeys);
        String[] tables = tableNames.toArray(new String[tableNames.size()]);
        DelegatingPartitioner.configurePartitioner(job, conf, tables); // sets the partitioner
    }

    protected void configureInputFormat(Job job, AccumuloHelper cbHelper, Configuration conf) throws Exception {
        // see if we need to do any accumulo setup on the input format class (initial for EventProcessingErrorTableFileInputFormat)
        if (inputFormat != null) {
            try {
                log.info("Looking for " + inputFormat.getName() + ".setup(" + JobContext.class.getName() + ", " + AccumuloHelper.class.getName() + ")");
                Method setup = inputFormat.getMethod("setup", JobContext.class, AccumuloHelper.class);
                log.info("Calling " + inputFormat.getName() + ".setup(" + JobContext.class.getName() + ", " + AccumuloHelper.class.getName() + ")");
                setup.invoke(null, job, cbHelper);
            } catch (NoSuchMethodException nsme) {
                // no problem, nothing to call
            }
            job.setInputFormatClass(inputFormat);
        }
    }

    protected void configureJob(Job job, Configuration conf, Path workDirPath, FileSystem outputFs) throws Exception {
        // create a job name
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss.SSS");
        job.setJobName(IngestJob.class.getSimpleName() + "_" + format.format(new Date()));

        // if doing this as a bulk job, create the job.paths file and the flag file if supplied
        if (!outputMutations) {
            writeInputPathsFile(outputFs, workDirPath, FileInputFormat.getInputPaths(job));
            if (flagFile != null) {
                writeFlagFile(outputFs, workDirPath, flagFile);
            }
        }

        // Setup the Mapper
        job.setMapperClass(mapper);
        job.setSortComparatorClass(BulkIngestKey.Comparator.class);

        if (idFilterFsts != null) {
            job.getConfiguration().set(EventMapper.ID_FILTER_FSTS, idFilterFsts);
        }

        // Setup the context writer counters boolean
        job.getConfiguration().setBoolean(AbstractContextWriter.CONTEXT_WRITER_COUNTERS, contextWriterCounters);
        job.getConfiguration().setBoolean(EventMapper.FILE_NAME_COUNTERS, fileNameCounters);

        // if we are using the combiner, then ensure the flag is set
        if (useCombiner || useInlineCombiner) {
            job.getConfiguration().setBoolean(BulkIngestKeyDedupeCombiner.USING_COMBINER, true);
            if (useCombiner && useInlineCombiner) {
                log.warn("Using both an inline combiner AND a map-reduce combiner...perhaps only one is needed");
            } else if (useCombiner) {
                log.info("Using a combiner.  Consider using 'useInlineCombiner' instead");
            } else {
                log.info("Using an inline combiner");
            }
        }

        // Setup the job output and reducer classes
        if (outputMutations) {
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Mutation.class);

            if (!useMapOnly) {
                job.setMapOutputKeyClass(BulkIngestKey.class);
                job.setMapOutputValueClass(Value.class);

                if (useCombiner) {
                    // Dedupe combiner will remove dupes and reset timestamps
                    // Note: to guarantee the combiner runs we need the min combine splits to be 1
                    job.getConfiguration().setInt("min.num.spills.for.combine", 1);
                    job.getConfiguration().setClass(BulkIngestKeyDedupeCombiner.CONTEXT_WRITER_CLASS, BulkContextWriter.class, ContextWriter.class);
                    job.setCombinerClass(BulkIngestKeyDedupeCombiner.class);
                }

                if (useInlineCombiner) {
                    // The dedupe context writer invokes the BulkIngestKeyDedupeCombiner.
                    // We are running the DedupeContextWriter in the context writer stream instead of using a combiner for performance reasons
                    job.getConfiguration().setClass(EventMapper.CONTEXT_WRITER_CLASS, DedupeContextWriter.class, ChainedContextWriter.class);
                    job.getConfiguration().setClass(DedupeContextWriter.CONTEXT_WRITER_CLASS, TableCachingContextWriter.class, ContextWriter.class);
                } else {
                    job.getConfiguration().setClass(EventMapper.CONTEXT_WRITER_CLASS, TableCachingContextWriter.class, ChainedContextWriter.class);
                }
                job.getConfiguration().setClass(TableCachingContextWriter.CONTEXT_WRITER_CLASS, BulkContextWriter.class, ContextWriter.class);

                // Aggregating reducer will remove dupes for each reduce task and reset the reset timestamps
                // The reducer will take care of translating from BulkIngestKeys to Mutations by using the LiveContextWriter
                job.getConfiguration().setClass(BulkIngestKeyAggregatingReducer.CONTEXT_WRITER_CLASS, LiveContextWriter.class, ContextWriter.class);
                job.getConfiguration().setBoolean(BulkIngestKeyAggregatingReducer.CONTEXT_WRITER_OUTPUT_TABLE_COUNTERS, tableCounters);
                job.setReducerClass(BulkIngestKeyAggregatingReducer.class);
            } else {
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Mutation.class);

                // The dedupe context writer invokes the BulkIngestKeyDedupeCombiner, and the aggregating context writer
                // invokes the BulkIngestKeyAggregatingReducer. The LiveContextWriter will take care of translating from BulkIngestKeys to Mutations
                job.getConfiguration().setBoolean(EventMapper.CONTEXT_WRITER_OUTPUT_TABLE_COUNTERS, tableCounters);

                if (useCombiner || useInlineCombiner) {
                    job.getConfiguration().setClass(EventMapper.CONTEXT_WRITER_CLASS, DedupeContextWriter.class, ChainedContextWriter.class);
                    job.getConfiguration().setClass(DedupeContextWriter.CONTEXT_WRITER_CLASS, TableCachingContextWriter.class, ContextWriter.class);
                } else {
                    job.getConfiguration().setClass(EventMapper.CONTEXT_WRITER_CLASS, TableCachingContextWriter.class, ChainedContextWriter.class);
                }

                job.getConfiguration().setClass(TableCachingContextWriter.CONTEXT_WRITER_CLASS, AggregatingContextWriter.class, ContextWriter.class);
                job.getConfiguration().setClass(AggregatingContextWriter.CONTEXT_WRITER_CLASS, LiveContextWriter.class, ContextWriter.class);
            }

        } else {
            job.setMapOutputKeyClass(BulkIngestKey.class);
            job.setMapOutputValueClass(Value.class);
            job.setOutputKeyClass(BulkIngestKey.class);
            job.setOutputValueClass(Value.class);

            if (!useMapOnly) {
                if (useCombiner) {
                    // Dedupe combiner will remove dupes and reset timestamps
                    // Note: to guarantee the combiner runs we need the min combine splits to be 1
                    job.getConfiguration().setInt("min.num.spills.for.combine", 1);
                    job.getConfiguration().setClass(BulkIngestKeyDedupeCombiner.CONTEXT_WRITER_CLASS, BulkContextWriter.class, ContextWriter.class);
                    job.setCombinerClass(BulkIngestKeyDedupeCombiner.class);
                }

                if (useInlineCombiner) {
                    // The dedupe context writer invokes the BulkIngestKeyDedupeCombiner.
                    // We are running the DedupeContextWriter in the context writer stream instead of using a combiner for performance reasons
                    job.getConfiguration().setClass(EventMapper.CONTEXT_WRITER_CLASS, DedupeContextWriter.class, ChainedContextWriter.class);
                    job.getConfiguration().setClass(DedupeContextWriter.CONTEXT_WRITER_CLASS, TableCachingContextWriter.class, ContextWriter.class);
                } else {
                    job.getConfiguration().setClass(EventMapper.CONTEXT_WRITER_CLASS, TableCachingContextWriter.class, ChainedContextWriter.class);
                }
                job.getConfiguration().setClass(TableCachingContextWriter.CONTEXT_WRITER_CLASS, BulkContextWriter.class, ContextWriter.class);

                // Aggregating reducer will remove dupes for each reduce task and reset the reset timestamps
                job.getConfiguration().setClass(BulkIngestKeyAggregatingReducer.CONTEXT_WRITER_CLASS, BulkContextWriter.class, ContextWriter.class);
                job.getConfiguration().setBoolean(BulkIngestKeyAggregatingReducer.CONTEXT_WRITER_OUTPUT_TABLE_COUNTERS, tableCounters);
                job.setReducerClass(BulkIngestKeyAggregatingReducer.class);
            } else {
                // The dedupe context writer invokes the BulkIngestKeyDedupeCombiner, and the aggregating context writer
                // invokes the BulkIngestKeyAggregatingReducer. The LiveContextWriter will take care of translating from BulkIngestKeys to Mutations
                job.getConfiguration().setBoolean(EventMapper.CONTEXT_WRITER_OUTPUT_TABLE_COUNTERS, tableCounters);

                if (useCombiner || useInlineCombiner) {
                    job.getConfiguration().setClass(EventMapper.CONTEXT_WRITER_CLASS, DedupeContextWriter.class, ChainedContextWriter.class);
                    job.getConfiguration().setClass(DedupeContextWriter.CONTEXT_WRITER_CLASS, TableCachingContextWriter.class, ContextWriter.class);
                } else {
                    job.getConfiguration().setClass(EventMapper.CONTEXT_WRITER_CLASS, TableCachingContextWriter.class, ChainedContextWriter.class);
                }

                job.getConfiguration().setClass(TableCachingContextWriter.CONTEXT_WRITER_CLASS, AggregatingContextWriter.class, ContextWriter.class);
                job.getConfiguration().setClass(AggregatingContextWriter.CONTEXT_WRITER_CLASS, BulkContextWriter.class, ContextWriter.class);
            }
        }

        // If only doing a map phase, then no reduce tasks to run
        if (useMapOnly) {
            job.setNumReduceTasks(0);
            setReduceSpeculativeExecution(job.getConfiguration(), false);
        } else {
            job.setNumReduceTasks(reduceTasks);
        }

        // Turn off speculative execution if we're using live ingest.
        // We don't want to send the same mutations to accumulo multiple times.
        // Normally for bulk we use speculative execution since there are no
        // direct accumulo writes. However, the user can turn off speculative
        // execution if they want. This may, for example, increase overall
        // throughput if the system is fully loaded.
        if (disableSpeculativeExecution || outputMutations) {
            setMapSpeculativeExecution(job.getConfiguration(), false);
            setReduceSpeculativeExecution(job.getConfiguration(), false);
        }

        // Setup the Output
        job.setWorkingDirectory(workDirPath);
        if (outputMutations) {
            CBMutationOutputFormatter.configure(job.getConfiguration()).createTables(true).store(job);
            job.setOutputFormatClass(CBMutationOutputFormatter.class);
        } else {
            FileOutputFormat.setOutputPath(job, new Path(workDirPath, "mapFiles"));
            job.setOutputFormatClass(MultiRFileOutputFormatter.class);
        }

        // Setup the location for the history output (old and new property names)
        job.getConfiguration().setIfUnset("hadoop.job.history.user.location", workDirPath + "/mapFiles");
        job.getConfiguration().setIfUnset("mapreduce.job.userhistorylocation", workDirPath + "/mapFiles");

        // verbose counters will add counters showing the output in the EventMapper, and the input to the reducers
        if (verboseCounters) {
            job.getConfiguration().setBoolean("verboseCounters", true);
        }

        // we always want the job to use our jars instead of the ones in $HADOOP_HOME/lib
        job.getConfiguration().setBoolean("mapreduce.job.user.classpath.first", true);

        // fetch the multiple numshards cache, if necessary
        if (job.getConfiguration().getBoolean(NumShards.ENABLE_MULTIPLE_NUMSHARDS, false)) {
            NumShards numShards = new NumShards(job.getConfiguration());
            String multipleNumShardsConfig = numShards.readMultipleNumShardsConfig();

            // this could return empty string, if the feature is enabled, but no entries in the metadata table
            // if it didn't throw RuntimeException, it found a valid cache file
            job.getConfiguration().set(NumShards.PREFETCHED_MULTIPLE_NUMSHARDS_CONFIGURATION, multipleNumShardsConfig == null ? "" : multipleNumShardsConfig);
        }
    }

    /**
     * @param keyValue
     *            of format 'key=value'
     */
    protected void addConfOverride(String keyValue) {
        String[] strArr = keyValue.split("=", 2);
        if (strArr.length != 2) {
            log.error("WARN: skipping bad property configuration " + keyValue);
        } else {
            log.info("Setting " + strArr[0] + " = \"" + strArr[1] + '"');
            confOverrides.add(strArr);
        }
    }

    private void updateConfWithOverrides(Configuration conf) {
        for (String[] conOverride : confOverrides) {
            conf.set(conOverride[0], conOverride[1]);
        }
    }

    protected int jobFailed(Job job, RunningJob runningJob, FileSystem fs, Path workDir) throws IOException {
        log.error("Map Reduce job " + job.getJobName() + " was unsuccessful. Check the logs.");
        log.error("Since job was not successful, deleting work directory: " + workDir);
        boolean deleted = fs.delete(workDir, true);
        if (!deleted) {
            log.error("Unable to remove job working directory: " + workDir);
        }
        if (runningJob.getJobState() == JobStatus.KILLED) {
            log.warn("Job was killed");
            return -2;
        } else {
            log.error("Job failed with a jobstate of " + runningJob.getJobState());
            return -3;
        }
    }

    protected boolean createFileWithRetries(FileSystem fs, Path file) throws IOException, InterruptedException {
        return createFileWithRetries(fs, file, file);
    }

    protected boolean createFileWithRetries(FileSystem fs, Path file, Path verification) throws IOException, InterruptedException {
        Exception exception = null;
        // we will attempt this 10 times at most....
        for (int i = 0; i < 10; i++) {
            try {
                exception = null;
                // create the file....ignoring the return value as we will be checking ourselves anyway....
                log.info("Creating " + file);
                fs.createNewFile(file);
            } catch (Exception e) {
                exception = e;
            }
            // check to see if the file exists in which case we are good to go
            try {
                log.info("Verifying " + file + " with " + verification);
                FileStatus[] files = fs.globStatus(verification);
                if (files == null || files.length == 0) {
                    throw new FileNotFoundException("Failed to get status for " + file);
                }
                // we found the file!
                log.info("Created " + file);
                return true;
            } catch (Exception e) {
                log.warn("Trying again to create " + file + " in one second");
                // now this is getting frustrating....
                // wait a sec and try again
                Thread.sleep(1000);
            }
        }
        // log the exception if any
        if (exception != null) {
            log.error("Failed to create " + file, exception);
        }
        return false;

    }

    protected boolean markJobComplete(FileSystem fs, Path workDir) throws IOException, InterruptedException {
        return createFileWithRetries(fs, new Path(workDir, "job.complete"), new Path(workDir, "job.[^p]*"));
    }

    /**
     * Get the files to process
     *
     * @param fs
     *            used by extending classes such as MapFileMergeJob
     * @param inputFileLists
     *            the input file lists
     * @param inputFileListMarker
     *            the input file list marker
     * @param inputPaths
     *            the input paths
     * @return a list of paths
     * @throws IOException
     *             if there is an issue with read or write
     */
    protected Path[] getFilesToProcess(FileSystem fs, boolean inputFileLists, String inputFileListMarker, String inputPaths) throws IOException {
        String[] paths = StringUtils.trimAndRemoveEmptyStrings(StringUtils.split(inputPaths, ','));
        List<Path> inputPathList = new ArrayList<>(inputFileLists ? paths.length * 100 : paths.length);
        for (String inputPath : paths) {
            // if we are to treat the input paths as file lists, then expand here
            if (inputFileLists) {
                FileInputStream in = new FileInputStream(inputPath);
                try (BufferedReader r = new BufferedReader(new InputStreamReader(in))) {
                    String line = r.readLine();
                    boolean useit = (inputFileListMarker == null);
                    while (line != null) {
                        if (useit) {
                            inputPathList.add(new Path(line));
                        } else {
                            if (line.equals(inputFileListMarker)) {
                                useit = true;
                            }
                        }
                        line = r.readLine();
                    }
                } finally {
                    in.close();
                }
            } else {
                inputPathList.add(new Path(inputPath));
            }
        }
        // log the input path list if we had to expand file lists
        if (inputFileLists) {
            log.info("inputPathList is " + inputPathList);
        }
        return inputPathList.toArray(new Path[inputPathList.size()]);
    }

    protected FileSystem getFileSystem(Configuration conf, URI uri) throws IOException {
        return (uri == null ? FileSystem.get(conf) : FileSystem.get(uri, conf));
    }

    /**
     * Writes the input paths for this job into the work directory in a file named "job.paths"
     *
     * @param fs
     *            the filesystem
     * @param workDir
     *            the work directory
     * @param inputPaths
     *            the input paths
     * @throws IOException
     *             for issues with read or write
     */
    protected void writeInputPathsFile(FileSystem fs, Path workDir, Path[] inputPaths) throws IOException {
        FSDataOutputStream os = fs.create(new Path(workDir, "job.paths"));
        PrintStream ps = new PrintStream(new BufferedOutputStream(os));
        for (Path p : inputPaths) {
            ps.println(new Path(p.toUri().getPath()));
        }
        ps.close();
    }

    /**
     * Writes the flag file for this job into the work directory in a file with the same name
     *
     * @param fs
     *            the file system
     * @param workDir
     *            the work directory
     * @param flagFileName
     *            the flag file name
     * @throws IOException
     *             for read or write related issues
     */

    protected void writeFlagFile(FileSystem fs, Path workDir, String flagFileName) throws IOException {
        File flagFile = new File(flagFileName);
        if (!flagFile.exists() || !flagFile.isFile() || !flagFile.canRead()) {
            throw new IOException("Unable to access " + flagFile + " for copying into hdfs " + workDir + " directory");
        }

        try (InputStream is = new BufferedInputStream(new FileInputStream(flagFile))) {
            String flagFileBase = getBaseFlagFileName(flagFile.getName());
            Path destFile = new Path(workDir, flagFileBase);
            log.info("Copying flag file into " + destFile);
            try (OutputStream os = new BufferedOutputStream(fs.create(destFile))) {

                byte[] buffer = new byte[2048];
                int bytesRead = is.read(buffer);
                while (bytesRead >= 0) {
                    if (bytesRead > 0) {
                        os.write(buffer, 0, bytesRead);
                    }
                    bytesRead = is.read(buffer);
                }
            }
        }
    }

    /*
     * Get the base flag filename. The name supplied may have something line '.inprogress' tagged on the end. Return everything up to and including .flag.
     */
    protected String getBaseFlagFileName(String filename) {
        if (filename.endsWith(".flag")) {
            return filename;
        } else {
            int index = filename.lastIndexOf(".flag");
            if (index < 0) {
                return filename;
            } else {
                return filename.substring(0, index + ".flag".length());
            }
        }
    }

    /**
     * Marks the input files given to this job as loaded by moving them from the "flagged" directory to the "loaded" directory.
     *
     * @param fs
     *            the filesystem
     * @param inputPaths
     *            the input paths
     * @param jobID
     *            the job id
     * @throws IOException
     *             if there is an issue with read or write
     */
    protected void markFilesLoaded(FileSystem fs, Path[] inputPaths, JobID jobID) throws IOException {
        for (Path src : inputPaths) {
            String ssrc = src.toString();
            if (ssrc.contains("/flagged/")) {
                Path dst = new Path(ssrc.replaceFirst("/flagged/", "/loaded/"));
                boolean mkdirs = fs.mkdirs(dst.getParent());
                if (mkdirs) {
                    boolean renamed = fs.rename(src, dst);
                    if (!renamed) {
                        throw new IOException("Unable to rename " + src + " to " + dst);
                    }
                } else {
                    throw new IOException("Unable to create parent dir: " + dst.getParent());
                }
            }
        }

        // notify observers
        jobObservable.setJobId(jobID.toString());
    }

    /**
     * Some properties cannot be set using the new API. However, we know internally that the configuration Hadoop uses is really just the old JobConf which
     * exposes the methods we want. In particular, we have to turn off speculative execution since we are loading data and don't want Hadoop to spawn many
     * speculative tasks that will load duplicate data.
     *
     * @param conf
     *            the configuration
     * @param value
     *            a boolean value
     */
    protected void setMapSpeculativeExecution(Configuration conf, boolean value) {
        if (conf instanceof org.apache.hadoop.mapred.JobConf) {
            org.apache.hadoop.mapred.JobConf jobConf = (org.apache.hadoop.mapred.JobConf) conf;
            jobConf.setMapSpeculativeExecution(value);
        }
    }

    /**
     * Some properties cannot be set using the new API. However, we know internally that the configuration Hadoop uses is really just the old JobConf which
     * exposes the methods we want. In particular, we have to turn off speculative execution since we are loading data and don't want Hadoop to spawn many
     * speculative tasks that will load duplicate data.
     *
     * @param conf
     *            the configuration
     * @param value
     *            a boolean value
     */
    protected void setReduceSpeculativeExecution(Configuration conf, boolean value) {
        if (conf instanceof org.apache.hadoop.mapred.JobConf) {
            org.apache.hadoop.mapred.JobConf jobConf = (org.apache.hadoop.mapred.JobConf) conf;
            jobConf.setReduceSpeculativeExecution(value);
        }
    }

    /**
     * Configures the output formatter with the correct accumulo instance information, splits file, and shard table.
     *
     * @param config
     *            hadoop configuration
     * @param compressionType
     *            type of compression to use for the output format
     * @param compressionTableDisallowList
     *            a set of table names for which we will not compress the rfile output
     * @param maxEntries
     *            the max entries
     * @param maxSize
     *            the max size
     */
    public static void configureMultiRFileOutputFormatter(Configuration config, String compressionType, Set<String> compressionTableDisallowList,
                    int maxEntries, long maxSize) {
        IngestJob.configureMultiRFileOutputFormatter(config, compressionType, compressionTableDisallowList, maxEntries, maxSize, false);
    }

    public static void configureMultiRFileOutputFormatter(Configuration config, String compressionType, Set<String> compressionTableDisallowList,
                    int maxEntries, long maxSize, boolean generateMapFileRowKeys) {
        MultiRFileOutputFormatter.setAccumuloConfiguration(config);
        if (compressionType != null) {
            MultiRFileOutputFormatter.setCompressionType(config, compressionType);
        }
        if (compressionTableDisallowList != null) {
            MultiRFileOutputFormatter.setCompressionTableDisallowList(config, compressionTableDisallowList);
        }
        MultiRFileOutputFormatter.setRFileLimits(config, maxEntries, maxSize);
        MultiRFileOutputFormatter.setGenerateMapFileRowKeys(config, generateMapFileRowKeys);
    }

    public static Set<String> setupAndCacheTables(Configuration conf, boolean createTables)
                    throws IOException, AccumuloException, TableNotFoundException, AccumuloSecurityException {
        TableConfigurationUtil tableConfigUtil = new TableConfigurationUtil(conf);
        tableConfigUtil.registerTableNamesFromConfigFiles(conf);
        Set<String> tableNames = tableConfigUtil.getJobOutputTableNames(conf);

        if (createTables) {
            boolean wasConfigureTablesSuccessful = tableConfigUtil.configureTables(conf);
            if (!wasConfigureTablesSuccessful) {
                throw new RuntimeException("Could not create tables");
            }
        }

        tableConfigUtil.serializeTableConfgurationIntoConf(conf);

        return tableNames;
    }

    protected void startDaemonProcesses(Configuration configuration) {
        String daemonClassNames = configuration.get(DAEMON_PROCESSES_PROPERTY);
        if (daemonClassNames == null) {
            return;
        }
        for (String className : StringUtils.split(daemonClassNames, ',')) {
            try {
                @SuppressWarnings("unchecked")
                Class<? extends Runnable> daemonClass = (Class<? extends Runnable>) Class.forName(className.trim());
                Runnable daemon = daemonClass.getDeclaredConstructor().newInstance();
                if (daemon instanceof Configurable) {
                    Configurable configurable = (Configurable) daemon;
                    configurable.setConf(configuration);
                }
                Thread daemonThread = new Thread(daemon);
                daemonThread.setDaemon(true);
                daemonThread.start();
            } catch (ClassNotFoundException | IllegalAccessException | InstantiationException | NoSuchMethodException | InvocationTargetException e) {
                throw new IllegalArgumentException(e);
            }
        }
    }

    protected void distCpDirectory(Path workDir, FileSystem src, FileSystem dest, Configuration distcpConfig, boolean deleteAfterDistCp) throws Exception {
        Path srcPath = src.makeQualified(workDir);
        Path destPath = dest.makeQualified(workDir);
        Path logPath = new Path(destPath, "logs");

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
        DistCpOptions options = new DistCpOptions.Builder(Collections.singletonList(srcPath), destPath)
            .withLogPath(logPath)
            .withMapBandwidth(distCpBandwidth)
            .maxMaps(distCpMaxMaps)
            .withCopyStrategy(distCpStrategy)
            .withSyncFolder(true)
            .preserve(DistCpOptions.FileAttribute.USER)
            .preserve(DistCpOptions.FileAttribute.GROUP)
            .preserve(DistCpOptions.FileAttribute.PERMISSION)
            .preserve(DistCpOptions.FileAttribute.BLOCKSIZE)
            .preserve(DistCpOptions.FileAttribute.CHECKSUMTYPE)
            .withBlocking(true)
            .build();
        //@formatter:on

        DistCp cp = new DistCp(distcpConfig, options);
        log.info("Starting distcp from " + srcPath + " to " + destPath + " with configuration: " + options);
        try {
            cp.execute();
        } catch (Exception e) {
            throw new RuntimeException("Distcp failed.", e);
        }
        // verify the data was copied
        Map<String,FileStatus> destFiles = new HashMap<>();
        for (FileStatus destFile : dest.listStatus(destPath)) {
            destFiles.put(destFile.getPath().getName(), destFile);
        }

        for (FileStatus srcFile : src.listStatus(srcPath)) {
            FileStatus destFile = destFiles.get(srcFile.getPath().getName());
            if (destFile == null || destFile.getLen() != srcFile.getLen()) {
                throw new RuntimeException("DistCp failed to copy " + srcFile.getPath());
            }
        }

        // now we can clean up the src job directory
        if (deleteAfterDistCp) {
            src.delete(srcPath, true);
        }
    }

    protected boolean writeStats(Logger log, Job job, JobID jobId, Counters counters, long start, long stop, boolean outputMutations, FileSystem fs,
                    Path statsDir, String metricsLabelOverride) throws IOException, InterruptedException {

        Configuration conf = job.getConfiguration();

        // We are going to serialize the counters into a file in HDFS.
        // The context was set in the processKeyValues method below, and should not be null. We'll guard against NPE anyway
        try (RawLocalFileSystem rawFS = new RawLocalFileSystem()) {
            rawFS.setConf(conf);
            CompressionCodec cc = new GzipCodec();
            CompressionType ct = CompressionType.BLOCK;

            // Add additional counters
            if (!outputMutations) {
                counters.findCounter(IngestProcess.OUTPUT_DIRECTORY.name(), job.getWorkingDirectory().getName()).increment(1);
            } else {
                counters.findCounter(IngestProcess.LIVE_INGEST).increment(1);
            }
            counters.findCounter(IngestProcess.START_TIME).increment(start);
            counters.findCounter(IngestProcess.END_TIME).increment(stop);

            if (metricsLabelOverride != null) {
                counters.getGroup(IngestProcess.METRICS_LABEL_OVERRIDE.name()).findCounter(metricsLabelOverride).increment(1);
            }

            // Serialize the counters to a file in HDFS.
            Path src = new Path("/tmp" + File.separator + job.getJobName() + ".metrics");
            src = rawFS.makeQualified(src);
            createFileWithRetries(rawFS, src);
            try (Writer writer = SequenceFile.createWriter(conf, Writer.file(src), Writer.keyClass(Text.class), Writer.valueClass(Counters.class),
                            Writer.compression(ct, cc))) {
                writer.append(new Text(jobId.toString()), counters);
            }

            // Now we will try to move the file to HDFS.
            // Copy the file to the temp dir
            try {
                if (!fs.exists(statsDir))
                    fs.mkdirs(statsDir);
                Path dst = new Path(statsDir, src.getName());
                log.info("Copying file " + src + " to " + dst);
                fs.copyFromLocalFile(false, true, src, dst);
                // If this worked, then remove the local file
                rawFS.delete(src, false);
                // also remove the residual crc file
                rawFS.delete(getCrcFile(src), false);
            } catch (IOException e) {
                // If an error occurs in the copy, then we will leave in the local metrics directory.
                log.error("Error copying metrics file into HDFS, will remain in metrics directory.", e);
                return false;
            }
        }
        // now if configured, lets write the stats out to statsd
        CounterToStatsDConfiguration statsDConfig = new CounterToStatsDConfiguration(conf);
        if (statsDConfig.isConfigured()) {
            log.info("Sending final counters via statsd: " + statsDConfig);
            CounterStatsDClient statsd = statsDConfig.getClient();
            try {
                statsd.sendFinalStats(counters);
            } finally {
                statsd.close();
            }
        }

        return true;
    }

    private Path getCrcFile(Path path) {
        return new Path(path.getParent(), "." + path.getName() + ".crc");
    }

    /**
     * Output some verbose counters
     *
     * @param context
     *            hadoop task context for writing counter values
     * @param tableName
     *            the table name to write in the counter
     * @param mutation
     *            a Mutation containing the key-value pairs to log to counters
     * @param location
     *            the location
     */
    @SuppressWarnings("rawtypes")
    public static void verboseCounters(TaskInputOutputContext context, String location, Text tableName, Mutation mutation) {
        for (KeyValue keyValue : getKeyValues(mutation)) {
            verboseCounter(context, location, tableName, keyValue.getKey().getRow().getBytes(), keyValue.getKey().getColumnFamily().getBytes(),
                            keyValue.getKey().getColumnQualifier().getBytes(), keyValue.getKey().getColumnVisibility(), keyValue.getValue().get());
        }
    }

    /**
     * Output some verbose counters. Since the input is an iterable, this will cache the values in a list and return the new iterable.
     *
     * @param context
     *            the task context
     * @param key
     *            the ingest key
     * @param location
     *            the location
     * @param values
     *            the value
     * @return the iterable for the value list
     */
    @SuppressWarnings("rawtypes")
    public static Iterable<Value> verboseCounters(TaskInputOutputContext context, String location, BulkIngestKey key, Iterable<Value> values) {
        List<Value> valueList = new ArrayList<>();
        for (Value value : values) {
            valueList.add(value);
            verboseCounters(context, location, key, value);
        }
        return valueList;
    }

    /**
     * Output some verbose counters
     *
     * @param context
     *            hadoop task context for writing counter values
     * @param location
     *            the location
     * @param key
     *            hadoop key to log all key-value pairs to counters
     * @param value
     *            the value that goes with {@code key}
     */
    @SuppressWarnings("rawtypes")
    public static void verboseCounters(TaskInputOutputContext context, String location, BulkIngestKey key, Value value) {
        verboseCounter(context, location, key.getTableName(), key.getKey().getRow().getBytes(), key.getKey().getColumnFamily().getBytes(),
                        key.getKey().getColumnQualifier().getBytes(), key.getKey().getColumnVisibility(), value.get());
    }

    public final static int MAX_DATA_TO_PRINT = 64;

    /**
     * Output a verbose counter
     *
     * @param context
     *            hadoop task context for writing counter values
     * @param location
     *            the location string
     * @param tableName
     *            the table name to write in the counter
     * @param row
     *            the row of the key to writer in the counter
     * @param colFamily
     *            the column family of the key to write in the counter
     * @param colQualifier
     *            the column qualifier of the key to write in the counter
     * @param colVis
     *            the column visibility of the key to write in the counter
     * @param val
     *            the value that goes with the supplied key
     */
    @SuppressWarnings("rawtypes")
    public static void verboseCounter(TaskInputOutputContext context, String location, Text tableName, byte[] row, byte[] colFamily, byte[] colQualifier,
                    Text colVis, byte[] val) {
        String labelString = new ColumnVisibility(colVis).toString();
        String s = Key.toPrintableString(row, 0, row.length, MAX_DATA_TO_PRINT) + " " + Key.toPrintableString(colFamily, 0, colFamily.length, MAX_DATA_TO_PRINT)
                        + ":" + Key.toPrintableString(colQualifier, 0, colQualifier.length, MAX_DATA_TO_PRINT) + " " + labelString + " "
                        + (val == null ? "null" : String.valueOf(val.length) + " value bytes");

        s = s.replace('\n', ' ');

        context.getCounter("TABLE.KEY.VALUElen", tableName.toString() + ' ' + location + ' ' + s).increment(1);
    }

    /**
     * Turn a mutation's column update into a key
     *
     * @param m
     *            the Mutation from which KeyValue pairs should be extracted
     * @return a List of KeyValue pairs representing the contents of {@code m}
     */
    public static List<KeyValue> getKeyValues(Mutation m) {
        List<KeyValue> values = new ArrayList<>();
        for (ColumnUpdate update : m.getUpdates()) {
            values.add(new KeyValue(new Key(m.getRow(), update.getColumnFamily(), update.getColumnQualifier(), update.getColumnVisibility(),
                            (update.hasTimestamp() ? update.getTimestamp() : -1), update.isDeleted()), update.getValue()));
        }
        return values;
    }

    @Override
    public Configuration getConf() {
        return hadoopConfiguration;
    }

    @Override
    public void setConf(Configuration conf) {
        this.hadoopConfiguration = conf;
    }
}
