package datawave.ingest.mapreduce.job;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Observer;
import java.util.Set;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.io.comparator.LastModifiedFileComparator;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import datawave.ingest.config.TableConfigCache;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.ingest.AccumuloHelper;
import datawave.ingest.mapreduce.handler.shard.NumShards;
import datawave.ingest.mapreduce.partition.MultiTableRangePartitioner;
import datawave.ingest.metric.IngestInput;
import datawave.ingest.metric.IngestOutput;
import datawave.ingest.metric.IngestProcess;
import datawave.marking.MarkingFunctions;
import datawave.util.StringUtils;
import datawave.util.cli.PasswordConverter;

public class IdentityMapIngestJob extends IngestJob {
    private ConsoleAppender ca = new ConsoleAppender(new PatternLayout("%p [%c{1}] %m%n"));
    private String manifestFiles;
    private Set<Path> errorSeqSet = new HashSet<>();
    private Map<String,Path> manifests = new HashMap<>();
    
    public IdentityMapIngestJob() {
        super();
    }
    
    @Override
    public int run(String[] args) throws Exception {
        
        Logger.getLogger(TypeRegistry.class).setLevel(Level.ALL);
        
        ca.setThreshold(Level.INFO);
        log.addAppender(ca);
        log.setLevel(Level.INFO);
        
        MarkingFunctions.Factory.createMarkingFunctions();
        TypeRegistry.reset();
        
        Configuration conf = parseArguments(args, getConf());
        
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
        TableConfigurationUtil tableConfigUtil = new TableConfigurationUtil(conf);
        tableNames = tableConfigUtil.getTableNames();
        
        if (createTables) {
            boolean wasConfigureTablesSuccessful = tableConfigUtil.configureTables(conf);
            if (!wasConfigureTablesSuccessful) {
                return -1;
            } else
                log.info("Created tables: " + tableNames + " successfully!");
        }
        
        try {
            tableConfigUtil.serializeAggregatorConfiguration(cbHelper, conf, log);
        } catch (TableNotFoundException tnf) {
            log.error("One or more configured DataWave tables are missing in Accumulo. If this is a new system or if new tables have recently been introduced, run a job using the '-createTables' flag before attempting to ingest more data",
                            tnf);
            return -1;
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
        
        // getFilesToProcess will also get manifest files
        // if a seq file does not have a matching manifest file, we will log that and throw it into a quarantine set
        job.setJarByClass(this.getClass());
        for (Path inputPath : getFilesToProcess(inputFs, inputFileLists, inputFileListMarker, inputPaths)) {
            FileInputFormat.addInputPath(job, inputPath);
        }
        for (Path dependency : jobDependencies) {
            job.addFileToClassPath(dependency);
        }
        
        configureInputFormat(job, cbHelper, conf);
        
        configureJob(job, conf, workDirPath, outputFs);
        
        // Log configuration
        log.info("Types: " + TypeRegistry.getTypeNames());
        log.info("Tables: " + Arrays.toString(tableNames));
        log.info("InputFormat: " + job.getInputFormatClass().getName());
        log.info("Mapper: " + job.getMapperClass().getName());
        log.info("Reduce tasks: " + (useMapOnly ? 0 : reduceTasks));
        log.info("Split File: " + workDirPath + "/splits.txt");
        
        // Note that if we run any other jobs in the same vm (such as a sampler), then we may
        // need to catch and throw away an exception here
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory(conf));
        
        startDaemonProcesses(conf);
        long start = System.currentTimeMillis();
        job.submit();
        JobID jobID = job.getJobID();
        log.info("JOB ID: " + jobID);
        
        createFileWithRetries(outputFs, new Path(workDirPath, jobID.toString()));
        
        // Wait for map progress to pass the 70% mark and then
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
                        filter = new RegexFileFilter(".*_(bulkingestkey)_.*\\.flag");
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
        
        job.waitForCompletion(true);
        long stop = System.currentTimeMillis();
        
        // output the counters to the log
        Counters counters = job.getCounters();
        log.info(counters);
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
                float percentError = 100 * ((float) numExceptions / numRecords);
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
        
        // Since we are doing bulk ingest, we will
        // write out a marker file to indicate that the job is complete and a
        // separate process will bulk import the map files.
        // For this class, we highly likely aren't going to be outputing mutations
        if (!outputMutations) {
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
        if (errorSeqSet.size() > 0) {
            log.warn("There were files without matching manifest file. Check failed flags for specifics");
            File flagDir = new File(flagFileDir);
            if (flagDir.isDirectory() && flagDir.canWrite()) {
                String baseFlagName = getBaseFlagFileName(flagFile);
                File errorFlagFile = new File(flagDir, baseFlagName + "_" + "nomanifest.flag.allfailed");
                FileWriter fw = new FileWriter(errorFlagFile);
                for (Path erroredSeqFile : errorSeqSet) {
                    fw.append(erroredSeqFile.toString());
                }
                fw.close();
            }
            return -5;
        }
        
        return 0;
    }
    
    /**
     * Parse the arguments and update the configuration as needed
     *
     * @param args
     * @param conf
     * @throws ClassNotFoundException
     * @throws URISyntaxException
     */
    @Override
    protected Configuration parseArguments(String[] args, Configuration conf) throws ClassNotFoundException, URISyntaxException, IllegalArgumentException {
        List<String> activeResources = new ArrayList<>();
        
        inputPaths = args[0];
        log.info("InputPaths is " + inputPaths);
        for (int i = 1; i < args.length; i++) {
            if (args[i].equals("-manifestFiles")) {
                manifestFiles = args[++i];
            } else if (args[i].equals("-inputFileLists")) {
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
                conf.setBoolean(TableConfigCache.ACCUMULO_CONFIG_CACHE_ENABLE_PROPERTY, true);
            } else if (args[i].equalsIgnoreCase("-accumuloConfigCachePath")) {
                conf.set(TableConfigCache.ACCUMULO_CONFIG_CACHE_PATH_PROPERTY, args[++i]);
                conf.setBoolean(TableConfigCache.ACCUMULO_CONFIG_CACHE_ENABLE_PROPERTY, true);
            } else if (args[i].equals("-disableSpeculativeExecution")) {
                disableSpeculativeExecution = true;
            } else if (args[i].equals("-skipMarkerFileGeneration")) {
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
                String[] jars = StringUtils.trimAndRemoveEmptyStrings(args[++i].split("\\s*,\\s*"));
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
            } else if (args[i].equals("-compressionTableBlackList")) {
                String[] tables = StringUtils.split(args[++i], ',');
                compressionTableBlackList.addAll(Arrays.asList(tables));
            } else if (args[i].equals("-maxRFileUndeduppedEntries")) {
                maxRFileEntries = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-maxRFileUncompressedSize")) {
                maxRFileSize = Long.parseLong(args[++i]);
            } else if (args[i].equals("-shardedMapFiles")) {
                conf.set(ShardedTableMapFile.SHARDED_MAP_FILE_PATHS_RAW, args[++i]);
                ShardedTableMapFile.extractShardedTableMapFilePaths(conf);
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
     * Get the files to process
     *
     * @param fs
     *            used by extending classes such as MapFileMergeJob
     * @param inputFileLists
     * @param inputFileListMarker
     * @param inputPaths
     * @return
     * @throws IOException
     */
    @Override
    protected Path[] getFilesToProcess(FileSystem fs, boolean inputFileLists, String inputFileListMarker, String inputPaths) throws IOException {
        String[] paths = StringUtils.trimAndRemoveEmptyStrings(StringUtils.split(inputPaths, ','));
        String[] manifestPaths = StringUtils.trimAndRemoveEmptyStrings(StringUtils.split(manifestFiles, ','));
        List<Path> inputPathList = new ArrayList<>(inputFileLists ? paths.length * 100 : paths.length);
        
        for (String path : manifestPaths) {
            FileInputStream in = new FileInputStream(path);
            try (BufferedReader r = new BufferedReader(new InputStreamReader(in))) {
                String line = r.readLine();
                // There may be multiple files in each manifest
                if (line != null) {
                    // Format: <Manifest file name, OG ingest file name>
                    manifests.put(path, new Path(line));
                }
            }
        }
        for (String inputPath : paths) {
            if (manifests.containsKey(inputPath)) {
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
            } else {
                // Put mismatched sequenced/manifest pairs in an error set
                errorSeqSet.add(new Path(inputPath));
                log.warn("No manifest file to go with sequence file of " + inputPaths);
            }
        }
        // log the input path list if we had to expand file lists
        if (inputFileLists) {
            log.info("inputPathList is " + inputPathList);
        }
        return inputPathList.toArray(new Path[inputPathList.size()]);
    }
    
    protected boolean createFileWithRetries(FileSystem fs, Path file, Path verification, boolean useManifest) throws IOException, InterruptedException {
        Exception exception = null;
        // we will attempt this 10 times at most....
        for (int i = 0; i < 10; i++) {
            try {
                exception = null;
                // create the file....ignoring the return value as we will be checking ourselves anyway....
                log.info("Creating " + file);
                if (useManifest == false) {
                    fs.createNewFile(file);
                } else {
                    FSDataOutputStream os = fs.create(file);
                    PrintStream ps = new PrintStream(new BufferedOutputStream(os));
                    for (Path p : manifests.values()) {
                        ps.println(p);
                    }
                    ps.close();
                    os.close();
                }
                
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
    
    @Override
    protected boolean markJobComplete(FileSystem fs, Path workDir) throws IOException, InterruptedException {
        boolean complete = createFileWithRetries(fs, new Path(workDir, "job.complete"), new Path(workDir, "job.[^p]*"), false);
        boolean manifestCreated = createFileWithRetries(fs, new Path(workDir, "job.manifests"), new Path(workDir, "job.[^pc]*"), true);
        return manifestCreated && complete;
    }
    
    public static void main(String[] args) throws Exception {
        System.out.println("Running main");
        System.exit(ToolRunner.run(null, new IdentityMapIngestJob(), args));
    }
}
