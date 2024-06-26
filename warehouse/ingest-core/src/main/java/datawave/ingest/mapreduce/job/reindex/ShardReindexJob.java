package datawave.ingest.mapreduce.job.reindex;

import static datawave.ingest.mapreduce.job.ShardedTableMapFile.SPLIT_WORK_DIR;
import static datawave.ingest.mapreduce.job.TableConfigurationUtil.JOB_OUTPUT_TABLE_NAMES;
import static datawave.ingest.mapreduce.job.TableConfigurationUtil.TABLES_CONFIGS_TO_CACHE;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.hadoop.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.hadoop.mapreduce.InputFormatBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import datawave.ingest.data.config.ingest.AccumuloHelper;
import datawave.ingest.mapreduce.EventMapper;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.DelegatingPartitioner;
import datawave.ingest.mapreduce.job.IngestJob;
import datawave.ingest.mapreduce.job.MultiRFileOutputFormatter;
import datawave.ingest.mapreduce.job.RFileInputFormat;
import datawave.ingest.mapreduce.job.ShardedTableMapFile;
import datawave.ingest.mapreduce.job.reduce.BulkIngestKeyAggregatingReducer;
import datawave.ingest.mapreduce.job.reduce.BulkIngestKeyDedupeCombiner;
import datawave.ingest.mapreduce.job.util.AccumuloUtil;
import datawave.ingest.mapreduce.job.util.RFileUtil;
import datawave.ingest.mapreduce.job.writer.BulkContextWriter;
import datawave.ingest.mapreduce.job.writer.ChainedContextWriter;
import datawave.ingest.mapreduce.job.writer.ContextWriter;
import datawave.ingest.mapreduce.job.writer.DedupeContextWriter;
import datawave.ingest.mapreduce.job.writer.SpillingSortedContextWriter;
import datawave.ingest.mapreduce.job.writer.TableCachingContextWriter;
import datawave.util.StringUtils;

/**
 * Job will read from the FI of a shard table generating index and reverse index entries based on a pluggable framework
 *
 * May use a variety of input formats RFileInputFormat - RFileInputFormat doesn't know how to seek, so will pass over all key values
 *
 */
public class ShardReindexJob implements Tool {
    private static final Logger log = Logger.getLogger(ShardReindexJob.class);
    public static final Text FI_START = new Text("fi" + '\u0000');
    public static final Text FI_END = new Text("fi" + '\u0000' + '\uffff');

    private Configuration configuration;
    private JobConfig jobConfig = new JobConfig();

    private AccumuloClient accumuloClient;

    @Override
    public int run(String[] args) throws Exception {
        // parse command line options
        JCommander cmd = JCommander.newBuilder().addObject(jobConfig).build();
        cmd.parse(args);

        Job j = setupJob();

        if (j.waitForCompletion(true)) {
            return 0;
        }

        // job failed
        return -1;
    }

    private Map<String,String> generateHints() {
        if (jobConfig.resourceGroup != null) {
            log.info("setting resource group: " + jobConfig.resourceGroup);
            Map<String,String> hints = new HashMap<>();
            hints.put("scan_type", jobConfig.resourceGroup);
            return hints;
        }

        return null;
    }

    private Job setupJob() throws IOException, ParseException, AccumuloException, TableNotFoundException, AccumuloSecurityException, URISyntaxException {
        AccumuloClient.ConnectionOptions<Properties> builder;
        if (jobConfig.accumuloClientPropertiesPath != null) {
            builder = Accumulo.newClientProperties().from(jobConfig.accumuloClientPropertiesPath).as(jobConfig.username, getPassword());
        } else {
            builder = Accumulo.newClientProperties().to(jobConfig.instance, jobConfig.zookeepers).as(jobConfig.username, getPassword());
        }

        // using a batch scanner will force only a single thread per tablet
        // see AccumuloRecordReader.initialize()
        if (jobConfig.queryThreads != -1) {
            builder.batchScannerQueryThreads(jobConfig.queryThreads);
        }

        // this will not be applied to the scanner, see AccumuloRecordReader.initialize/ScannerImpl
        if (jobConfig.batchSize != -1) {
            builder.scannerBatchSize(jobConfig.batchSize);
        }

        // add resources to the config
        if (jobConfig.resources != null) {
            log.info("adding resources");
            String[] resources = StringUtils.trimAndRemoveEmptyStrings(jobConfig.resources.split("\\s*,\\s*"));
            for (String resource : resources) {
                log.info("added resource:" + resource);
                configuration.addResource(resource);
            }
        }

        // set all ShardReindexMapper flags
        configuration.setBoolean(ShardReindexMapper.CLEANUP_SHARD, jobConfig.cleanupShard);
        configuration.setBoolean(ShardReindexMapper.PROPAGATE_DELETES, jobConfig.propagateDeletes);
        configuration.setBoolean(ShardReindexMapper.REPROCESS_EVENTS, jobConfig.reprocessEvents);
        configuration.setBoolean(ShardReindexMapper.EXPORT_SHARD, jobConfig.exportShard);
        configuration.setBoolean(ShardReindexMapper.GENERATE_TF, jobConfig.generateTF);
        configuration.setBoolean(ShardReindexMapper.GENERATE_METADATA, !jobConfig.skipMetadata);
        configuration.setBoolean(ShardReindexMapper.FLOOR_TIMESTAMPS, !jobConfig.preserveTimestamps);
        configuration.setBoolean(ShardReindexMapper.ENABLE_REINDEX_COUNTERS, jobConfig.enableCounters);
        configuration.setBoolean(ShardReindexMapper.DUMP_COUNTERS, jobConfig.dumpCounters);

        // Verify the batch mode by converting it to the enum, this will throw an IllegalArgumentException if it cannot be converted
        ShardReindexMapper.BatchMode.valueOf(jobConfig.batchMode);
        configuration.set(ShardReindexMapper.BATCH_MODE, jobConfig.batchMode);

        if (jobConfig.dataTypeHandler != null) {
            configuration.set(ShardReindexMapper.DATA_TYPE_HANDLER, jobConfig.dataTypeHandler);
        }
        if (jobConfig.defaultDataType != null) {
            configuration.set(ShardReindexMapper.DEFAULT_DATA_TYPE, jobConfig.defaultDataType);
        }
        if (jobConfig.eventOverride != null) {
            configuration.set(ShardReindexMapper.EVENT_OVERRIDE, jobConfig.eventOverride);
        }

        // validate reprocess events config
        if (jobConfig.reprocessEvents) {
            if (jobConfig.defaultDataType == null) {
                throw new IllegalStateException("--defaultDataType must be set when reprocessing events");
            }
            if (jobConfig.dataTypeHandler == null) {
                throw new IllegalStateException("--dataTypeHandler must be set when reprocessing events");
            }
        }

        // setup the accumulo helper
        AccumuloHelper.setInstanceName(configuration, jobConfig.instance);
        AccumuloHelper.setPassword(configuration, getPassword().getBytes());
        AccumuloHelper.setUsername(configuration, jobConfig.username);
        AccumuloHelper.setZooKeepers(configuration, jobConfig.zookeepers);
        // TODO convert to this?
        // AccumuloHelper.setClientPropertiesPath(configuration, jobConfig.accumuloClientPropertiesPath);

        // set the work dir
        configuration.set(SPLIT_WORK_DIR, jobConfig.workDir);
        // required for MultiTableRangePartitioner
        configuration.set("ingest.work.dir.qualified", FileSystem.get(new URI(jobConfig.sourceHdfs), configuration).getUri().toString() + jobConfig.workDir);
        configuration.set("output.fs.uri", FileSystem.get(new URI(jobConfig.destHdfs), configuration).getUri().toString());

        // setup and cache tables from config
        Set<String> tableNames = IngestJob.setupAndCacheTables(configuration, false);
        configuration.setInt("splits.num.reduce", jobConfig.reducers);

        // these are required for the partitioner
        // split.work.dir must be set or this won't work
        // job.output.table.names must be set or this won't work
        if (configuration.get("split.work.dir") == null || configuration.get("job.output.table.names") == null) {
            throw new IllegalStateException("split.work.dir and job.output.table.names must be configured");
        }

        // test that each of the output table names exist
        Properties accumuloProperties = builder.build();
        accumuloClient = Accumulo.newClient().from(accumuloProperties).build();
        String[] outputTableNames = configuration.get(JOB_OUTPUT_TABLE_NAMES).split(",");
        validateTablesExist(outputTableNames);

        ShardedTableMapFile.setupFile(configuration);

        // setup the output format
        IngestJob.configureMultiRFileOutputFormatter(configuration, jobConfig.compression, null, 0, 0, false);
        log.info("compression type: " + configuration.get("MultiRFileOutputFormatter.compression", "unknown"));
        // all changes to configuration must be before this line
        Job j = Job.getInstance(getConf());

        // check if using some form of accumulo in input
        if (jobConfig.inputFiles == null) {
            if (jobConfig.startDate == null) {
                throw new IllegalArgumentException("startDate cannot be null when inputFiles are not specified");
            }

            if (jobConfig.endDate == null) {
                throw new IllegalArgumentException("endDate cannot be null when inputFiles are not specified");
            }

            if (jobConfig.accumuloMetadata) {
                // fetch the file list by scanning the accumulo.metadata table
                List<Map.Entry<String,List<String>>> filesForRanges = AccumuloUtil.getFilesFromMetadataBySplit(accumuloClient, jobConfig.table,
                                jobConfig.startDate, jobConfig.endDate);
                List<String> allFiles = new ArrayList<>();
                for (Map.Entry<String,List<String>> split : filesForRanges) {
                    allFiles.addAll(split.getValue());
                }
                jobConfig.inputFiles = String.join(",", allFiles);
            } else if (!jobConfig.accumuloData) {
                // build ranges
                Collection<Range> ranges = null;
                if (jobConfig.reprocessEvents) {
                    ranges = buildSplittableRanges(accumuloClient, jobConfig.maxRangeThreads, jobConfig.blocksPerSplit, jobConfig.batchMode, configuration,
                                    jobConfig.table, jobConfig.startDate, jobConfig.endDate);
                } else {
                    ranges = buildFiRanges(jobConfig.startDate, jobConfig.endDate, jobConfig.splitsPerDay);
                }

                if (ranges.size() == 0) {
                    throw new IllegalArgumentException("no ranges created from start: " + jobConfig.startDate + " end: " + jobConfig.endDate);
                }

                for (Range r : ranges) {
                    log.debug("Accumulo map task table: " + jobConfig.table + " for range: " + r);
                }

                ScannerBase.ConsistencyLevel consistencyLevel = ScannerBase.ConsistencyLevel.IMMEDIATE;
                if (jobConfig.useScanServers) {
                    consistencyLevel = ScannerBase.ConsistencyLevel.EVENTUAL;
                }

                // do not auto adjust ranges because they will be clipped and drop the column qualifier. this will result in full table scans
                InputFormatBuilder.InputFormatOptions options = AccumuloInputFormat.configure().clientProperties(accumuloProperties).table(jobConfig.table)
                                .autoAdjustRanges(false).batchScan(false).ranges(ranges).consistencyLevel(consistencyLevel)
                                .localIterators(jobConfig.accumuloLocal).offlineScan(jobConfig.offline);
                Map<String,String> executionHints = generateHints();
                if (executionHints != null) {
                    options.executionHints(executionHints);
                }
                options.store(j);
            }
        }

        // add to classpath and distributed cache files
        String[] jarNames = StringUtils.trimAndRemoveEmptyStrings(jobConfig.cacheJars.split("\\s*,\\s*"));
        for (String jarName : jarNames) {
            File jar = new File(jarName);
            Path path = new Path(jobConfig.cacheDir, jar.getName());
            log.info("jar: " + jar);
            j.addFileToClassPath(path);
        }

        // set the jar
        j.setJarByClass(this.getClass());

        if (jobConfig.inputFiles != null) {
            // direct from rfiles
            RFileInputFormat.addInputPaths(j, jobConfig.inputFiles);
            j.setInputFormatClass(RFileInputFormat.class);
        } else {
            // set the input format
            j.setInputFormatClass(AccumuloInputFormat.class);
        }
        // setup the mapper
        j.setMapOutputKeyClass(BulkIngestKey.class);
        j.setMapOutputValueClass(Value.class);
        j.setMapperClass(ShardReindexMapper.class);

        // setup the combiner
        j.getConfiguration().setBoolean(BulkIngestKeyDedupeCombiner.USING_COMBINER, true);
        // see IngestJob
        if (jobConfig.useCombiner) {
            j.getConfiguration().setClass(EventMapper.CONTEXT_WRITER_CLASS, DedupeContextWriter.class, ChainedContextWriter.class);
            j.getConfiguration().setClass(DedupeContextWriter.CONTEXT_WRITER_CLASS, TableCachingContextWriter.class, ContextWriter.class);
        } else {
            j.getConfiguration().setClass(EventMapper.CONTEXT_WRITER_CLASS, TableCachingContextWriter.class, ChainedContextWriter.class);
        }

        if (jobConfig.reducers == 0) {
            j.getConfiguration().set(SpillingSortedContextWriter.WORK_DIR, jobConfig.workDir);
            j.getConfiguration().setInt("shard" + TABLES_CONFIGS_TO_CACHE, 10000);
            j.getConfiguration().setInt("shardIndex" + TABLES_CONFIGS_TO_CACHE, 10000);
            j.getConfiguration().setInt("shardReverse" + TABLES_CONFIGS_TO_CACHE, 10000);
            j.getConfiguration().setClass(TableCachingContextWriter.CONTEXT_WRITER_CLASS, SpillingSortedContextWriter.class, ContextWriter.class);
        } else {
            j.getConfiguration().setClass(TableCachingContextWriter.CONTEXT_WRITER_CLASS, BulkContextWriter.class, ContextWriter.class);
        }

        // setup a partitioner
        DelegatingPartitioner.configurePartitioner(j, configuration, tableNames.toArray(new String[0]));

        // setup the reducer
        j.setReducerClass(BulkIngestKeyAggregatingReducer.class);
        j.getConfiguration().setClass(BulkIngestKeyAggregatingReducer.CONTEXT_WRITER_CLASS, BulkContextWriter.class, ContextWriter.class);
        j.getConfiguration().setBoolean(BulkIngestKeyAggregatingReducer.CONTEXT_WRITER_OUTPUT_TABLE_COUNTERS, true);
        j.setOutputKeyClass(BulkIngestKey.class);
        j.setOutputValueClass(Value.class);

        j.setNumReduceTasks(jobConfig.reducers);

        // set output format
        FileOutputFormat.setOutputPath(j, new Path(jobConfig.outputDir));
        j.setOutputFormatClass(MultiRFileOutputFormatter.class);

        // finished with the accumulo client
        this.accumuloClient.close();

        return j;
    }

    private void validateTablesExist(String[] tableNames) throws AccumuloException {
        for (String table : tableNames) {
            try {
                Map<String,String> tableProperties = accumuloClient.tableOperations().getTableProperties(table);
                if (tableProperties == null) {
                    throw new IllegalArgumentException("configured output table: " + table + " does not exist");
                }
            } catch (TableNotFoundException tnfe) {
                throw new IllegalArgumentException("configured output table: " + table + " does not exist");
            }
        }
    }

    private static Callable<List<Range>> getSplitCallable(Configuration config, String split, List<String> files, int blocksPerSplit,
                    Function<Key,Key> eventShiftFunction) {
        return () -> {
            log.info("found " + files.size() + " rfiles for " + split);
            return RFileUtil.getRangeSplits(config, files, new Key(split), new Key(split + '\uFFFF'), blocksPerSplit, eventShiftFunction);
        };
    }

    public static Collection<Range> buildSplittableRanges(AccumuloClient accumuloClient, int maxRangeThreads, final int blocksPerSplit, String batchMode,
                    Configuration config, String table, String startDay, String endDay) throws ParseException, IOException {
        List<Range> allRanges = new ArrayList<>();
        ExecutorService threadPool = null;
        List<Future<List<Range>>> splitTasks = null;
        if (maxRangeThreads > 1) {
            threadPool = Executors.newFixedThreadPool(maxRangeThreads);
            splitTasks = new ArrayList<>();
        }

        log.info("building ranges startDate: " + startDay + " endDate: " + endDay);

        Function<Key,Key> eventShiftFunction = Function.identity();
        if (!batchMode.equals("NONE")) {
            eventShiftFunction = new EventKeyAdjustment();
        }

        // check that these aren't the same
        if (startDay.equals(endDay)) {
            throw new IllegalArgumentException("endDay must be after startDay");
        }

        List<Map.Entry<String,List<String>>> filesBySplit;
        try {
            filesBySplit = AccumuloUtil.getFilesFromMetadataBySplit(accumuloClient, table, startDay, endDay);
        } catch (AccumuloException e) {
            throw new RuntimeException("Failed to lookup rfiles in metadata table", e);
        }

        for (Map.Entry<String,List<String>> fileSplit : filesBySplit) {
            Callable<List<Range>> splitCallable = getSplitCallable(config, fileSplit.getKey(), fileSplit.getValue(), blocksPerSplit, eventShiftFunction);
            if (threadPool != null) {
                splitTasks.add(threadPool.submit(splitCallable));
            } else {
                try {
                    allRanges.addAll(splitCallable.call());
                } catch (Exception e) {
                    throw new RuntimeException("Problem fetching splits", e);
                }
            }
        }

        // wait for any threads to complete
        if (splitTasks != null) {
            for (Future<List<Range>> f : splitTasks) {
                try {
                    allRanges.addAll(f.get());
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException("Failed to fetch split", e);
                }
            }
        }

        return allRanges;
    }

    public static Collection<Range> buildFiRanges(String start, String end, int splitsPerDay) throws ParseException {
        return buildRanges(start, end, splitsPerDay, FI_START, FI_END);
    }

    public static Collection<Range> buildRanges(String start, String end, int splitsPerDay, Text cfStart, Text cfEnd) throws ParseException {
        log.info("building ranges startDate: " + start + " endDate: " + end + " splits: " + splitsPerDay);
        SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyyMMdd");

        List<Range> ranges = new ArrayList<>();

        Date startDate = dateFormatter.parse(start);
        Date endDate = dateFormatter.parse(end);

        Date current = startDate;

        while (!endDate.before(current)) {
            String row = dateFormatter.format(current);
            for (int i = 0; i < splitsPerDay; i++) {
                Text rowText = new Text(row + "_" + i);
                Key startKey = new Key(rowText, cfStart);
                Key endKey = new Key(rowText, cfEnd);
                Range r = new Range(startKey, true, endKey, true);
                if (cfStart.equals(cfEnd)) {
                    endKey = new Key(new Text(rowText.toString() + '\u0000'));
                    r = new Range(startKey, true, endKey, false);
                }

                ranges.add(r);
            }

            Calendar calendar = Calendar.getInstance();
            calendar.setTime(current);
            calendar.add(Calendar.HOUR_OF_DAY, 24);
            current = calendar.getTime();
        }

        return ranges;
    }

    private String getPassword() {
        if (jobConfig.password.toLowerCase().startsWith("env:")) {
            return System.getenv(jobConfig.password.substring(4));
        }

        return jobConfig.password;
    }

    @Override
    public void setConf(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public Configuration getConf() {
        return configuration;
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Running ShardReindexJob");

        // this code proves that clip does not keep the cf in the Key
        // Key start = new Key("a", "fi");
        // Key end = new Key("z", "fi~");
        // Range startRange = new Range(start, true, end, true);
        //
        // Range subRange = new Range(new Key("c"), true, new Key("d"), true);
        //
        // Range clipped = subRange.clip(startRange);
        //
        // System.out.println(clipped.getStartKey());
        //
        // return;

        System.exit(ToolRunner.run(null, new ShardReindexJob(), args));
    }

    // define all job configuration options
    private class JobConfig {
        // startDate, endDate, splitsPerDay, and Table are all used with AccumuloInputFormat
        @Parameter(names = "--startDate", description = "yyyyMMdd start date")
        private String startDate;

        @Parameter(names = "--endDate", description = "yyyyMMdd end date")
        private String endDate;

        @Parameter(names = "--splitsPerDay", description = "splits for each day")
        private int splitsPerDay;

        @Parameter(names = "--table", description = "shard table")
        private String table = "shard";

        @Parameter(names = "--accumuloMetadata", description = "fetch files from the accumulo.metadata table for the given start/end dates")
        private boolean accumuloMetadata = false;

        @Parameter(names = "--accumuloData", description = "read ranges from accumulo instead of from files")
        private boolean accumuloData = false;

        @Parameter(names = "--blocksPerSplit", description = "Number of rfile index blocks per split, -1 to not split rfiles")
        private int blocksPerSplit = -1;

        @Parameter(names = "--useScanServers", description = "Use scan servers for any accumulo scans")
        private boolean useScanServers = false;

        @Parameter(names = "--accumuloLocal", description = "Run the accumulo iterators local in offline mode")
        private boolean accumuloLocal = false;

        @Parameter(names = "--accumuloClientPropertiesPath", description = "Filesystem path to accumulo-client.properties file to use for the accumulo client")
        private String accumuloClientPropertiesPath;

        // alternatively accept RFileInputFormat
        @Parameter(names = "--inputFiles", description = "When set these files will be used for the job. Should be comma delimited hdfs glob strings",
                        required = false)
        private String inputFiles;

        @Parameter(names = "--sourceHdfs", description = "HDFS for --inputFiles", required = true)
        private String sourceHdfs;

        // support for cache jars
        @Parameter(names = "--cacheDir", description = "HDFS path to cache directory", required = true)
        private String cacheDir;

        @Parameter(names = "--cacheJars", description = "jars located in the cacheDir to add to the classpath and distributed cache", required = true)
        private String cacheJars;

        // work dir
        @Parameter(names = "--workDir", description = "Temporary work location in hdfs", required = true)
        private String workDir;

        // support for additional resources
        @Parameter(names = "--resources", description = "configuration resources to be added")
        private String resources;

        @Parameter(names = "--reducers", description = "number of reducers to use", required = true)
        private int reducers;

        @Parameter(names = "--outputDir", description = "output directory that must not already exist", required = true)
        private String outputDir;

        @Parameter(names = "--destHdfs", description = "HDFS for --outputDir", required = true)
        private String destHdfs;

        @Parameter(names = "--cleanupShard", description = "generate delete keys when unused fi is found")
        private boolean cleanupShard;

        @Parameter(names = "--instance", description = "accumulo instance name", required = true)
        private String instance;

        @Parameter(names = "--zookeepers", description = "accumulo zookeepers", required = true)
        private String zookeepers;

        @Parameter(names = "--username", description = "accumulo username", required = true)
        private String username;

        @Parameter(names = "--password", description = "accumulo password", required = true)
        private String password;

        @Parameter(names = "--queryThreads", description = "batch scanner query threads, defaults to table setting")
        private int queryThreads = -1;

        @Parameter(names = "--batchSize", description = "accumulo batch size, defaults to table setting")
        private int batchSize = -1;

        @Parameter(names = "--propagateDeletes", description = "When true deletes are propagated to the indexes")
        private boolean propagateDeletes = false;

        @Parameter(names = "--defaultDataType",
                        description = "The datatype to apply to all data that has an unrecognized type, must have configuration for a Type from the TypeRegistry")
        private String defaultDataType;

        @Parameter(names = "--dataTypeHandler", description = "the DataTypeHandler to use to reprocess events, required with --reprocessEvents")
        private String dataTypeHandler;

        @Parameter(names = "--reprocessEvents", description = "When set event data will be reprocessed to generate everything except index only fields")
        private boolean reprocessEvents;

        @Parameter(names = "--eventOverride", description = "Class to create for each RawRecordContainer instance, must implement RawRecordContainer")
        private String eventOverride;

        @Parameter(names = "--exportShard",
                        description = "exports all sharded data along with the generated indexes. Used in conjunction with --reprocessEvents, and --generateTF")
        private boolean exportShard = false;

        @Parameter(names = "--generateTF",
                        description = "generates new Term Frequency offsets for any field that is not index only. When false existing TF offsets will be output as long as --exportShard is set")
        private boolean generateTF = false;

        @Parameter(names = "--skipMetadata", description = "disable writing DatawaveMetadata for job")
        private boolean skipMetadata = false;

        @Parameter(names = "--resourceGroup", description = "Applies a scan_type hint on accumulo scanners")
        private String resourceGroup;

        @Parameter(names = "--offline", description = "When used with --accumuloData will read rfiles directly in the mapper, table must be offline")
        private boolean offline = false;

        @Parameter(names = "--preserveTimestamps",
                        description = "preserve event timestamps when generating index entries instead of flooring them to the beginning of the day")
        private boolean preserveTimestamps = false;

        @Parameter(names = "--counters", description = "Include generated counters in map reduce job")
        private boolean enableCounters = false;

        @Parameter(names = "--dumpCounters", description = "Write counters to stdout instead of to the task mapred task counters")
        private boolean dumpCounters;

        @Parameter(names = "--compression", description = "Compression to use for generated rfiles")
        private String compression = "zstd";

        @Parameter(names = "--batchMode",
                        description = "if enabled and --reprocessEvents is enabled events will be processed together in batches. NONE,FIELD,EVENT,TLD are valid options")
        private String batchMode = "NONE";

        @Parameter(names = "--useCombiner", description = "Enable the Map based combiners")
        private boolean useCombiner = false;

        @Parameter(names = "--maxRangeThreads", description = "Max number of threads to use for range generation")
        private int maxRangeThreads = 1;

        @Parameter(names = {"-h", "--help"}, description = "display help", help = true)
        private boolean help;
    }
}
