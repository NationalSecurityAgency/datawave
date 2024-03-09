package datawave.ingest.mapreduce.job.reindex;

import static datawave.ingest.mapreduce.job.ShardedTableMapFile.SPLIT_WORK_DIR;

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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.hadoop.mapreduce.AccumuloInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import datawave.ingest.data.config.ingest.AccumuloHelper;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.DelegatingPartitioner;
import datawave.ingest.mapreduce.job.IngestJob;
import datawave.ingest.mapreduce.job.MultiRFileOutputFormatter;
import datawave.ingest.mapreduce.job.RFileInputFormat;
import datawave.ingest.mapreduce.job.ShardedTableMapFile;
import datawave.ingest.mapreduce.job.reduce.BulkIngestKeyAggregatingReducer;
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

    @Override
    public int run(String[] args) throws Exception {
        // parse command line options
        JCommander cmd = JCommander.newBuilder().addObject(jobConfig).build();
        cmd.parse(args);

        log.setLevel(Level.DEBUG);
        Logger.getRootLogger().setLevel(Level.DEBUG);

        Job j = setupJob();

        if (j.waitForCompletion(true)) {
            return 0;
        }

        // job failed
        return -1;
    }

    private Job setupJob() throws IOException, ParseException, AccumuloException, TableNotFoundException, AccumuloSecurityException, URISyntaxException {
        AccumuloClient.ConnectionOptions<Properties> builder = Accumulo.newClientProperties().to(jobConfig.instance, jobConfig.zookeepers)
                        .as(jobConfig.username, getPassword());

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

        // set the propagate deletes flag
        configuration.setBoolean(ShardReindexMapper.CLEANUP_SHARD, jobConfig.cleanupShard);
        configuration.setBoolean(ShardReindexMapper.PROPAGATE_DELETES, jobConfig.propagateDeletes);
        configuration.setBoolean(ShardReindexMapper.REPROCESS_EVENTS, jobConfig.reprocessEvents);
        configuration.setBoolean(ShardReindexMapper.EXPORT_SHARD, jobConfig.exportShard);
        configuration.setBoolean(ShardReindexMapper.GENERATE_TF, jobConfig.generateTF);
        configuration.setBoolean(ShardReindexMapper.GENERATE_METADATA, !jobConfig.skipMetadata);
        configuration.setBoolean(ShardReindexMapper.FLOOR_TIMESTAMPS, !jobConfig.preserveTimestamps);
        if (jobConfig.dataTypeHandler != null) {
            configuration.set(ShardReindexMapper.DATA_TYPE_HANDLER, jobConfig.dataTypeHandler);
        }
        if (jobConfig.eventOverride != null) {
            configuration.set(ShardReindexMapper.EVENT_OVERRIDE, jobConfig.eventOverride);
        }

        // setup the accumulo helper
        AccumuloHelper.setInstanceName(configuration, jobConfig.instance);
        AccumuloHelper.setPassword(configuration, getPassword().getBytes());
        AccumuloHelper.setUsername(configuration, jobConfig.username);
        AccumuloHelper.setZooKeepers(configuration, jobConfig.zookeepers);

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

        ShardedTableMapFile.setupFile(configuration);

        // setup the output format
        IngestJob.configureMultiRFileOutputFormatter(configuration, null, null, 0, 0, false);

        // all changes to configuration must be before this line
        Job j = Job.getInstance(getConf());

        // check if using some form of accumulo in input
        if (jobConfig.inputFiles == null) {
            Properties accumuloProperties = builder.build();
            if (jobConfig.accumuloMetadata) {
                // fetch the file list by scanning the accumulo.metadata table
                jobConfig.inputFiles = org.apache.hadoop.util.StringUtils.join(",", getSplitsFromMetadata(accumuloProperties, jobConfig.table,
                                new Range(new Key(jobConfig.startDate), true, new Key(jobConfig.endDate), true)));
            } else if (!jobConfig.accumuloData) {
                // build ranges
                Collection<Range> ranges = buildRanges(jobConfig.startDate, jobConfig.endDate, jobConfig.splitsPerDay);

                // do not auto adjust ranges because they will be clipped and drop the column qualifier. this will result in full table scans
                AccumuloInputFormat.configure().clientProperties(accumuloProperties).table(jobConfig.table).autoAdjustRanges(false).batchScan(false)
                                .ranges(ranges).store(j);
            }
        }

        // add to classpath and distributed cache files
        String[] jarNames = StringUtils.trimAndRemoveEmptyStrings(jobConfig.cacheJars.split("\\s*,\\s*"));
        for (String jarName : jarNames) {
            File jar = new File(jarName);
            Path path = new Path(jobConfig.cacheDir, jar.getName());
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

        // setup a partitioner
        DelegatingPartitioner.configurePartitioner(j, configuration, tableNames.toArray(new String[0]));

        // setup the reducer
        j.setReducerClass(BulkIngestKeyAggregatingReducer.class);
        j.setOutputKeyClass(BulkIngestKey.class);
        j.setOutputValueClass(Value.class);

        j.setNumReduceTasks(jobConfig.reducers);

        // set output format
        FileOutputFormat.setOutputPath(j, new Path(jobConfig.outputDir));
        j.setOutputFormatClass(MultiRFileOutputFormatter.class);

        return j;
    }

    private Set<String> getSplitsFromMetadata(Properties accumuloProperties, String tableName, Range r) {
        Set<String> rangeSplits = new HashSet<>();

        AccumuloClient client = Accumulo.newClient().from(accumuloProperties).build();
        String tableId = client.tableOperations().tableIdMap().get(tableName);

        if (tableId == null) {
            throw new RuntimeException("Could not locate table: '" + tableName + "'");
        }

        try {
            Scanner s = client.createScanner("accumulo.metadata");

            ScannerBase.ConsistencyLevel consistencyLevel = ScannerBase.ConsistencyLevel.IMMEDIATE;
            if (jobConfig.useScanServers) {
                consistencyLevel = ScannerBase.ConsistencyLevel.EVENTUAL;
            }

            s.setConsistencyLevel(consistencyLevel);
            s.setRange(new Range(new Key(tableId + ";" + r.getStartKey().getRowData()), true, new Key(tableId + ";" + r.getEndKey().getRowData()), true));
            Iterator<Map.Entry<Key,Value>> metarator = s.iterator();
            while (metarator.hasNext()) {
                Map.Entry<Key,Value> next = metarator.next();
                ByteSequence file = next.getKey().getColumnQualifierData();
                rangeSplits.add(file.toString());
            }
            s.close();
            client.close();
        } catch (TableNotFoundException | AccumuloException | AccumuloSecurityException e) {
            throw new RuntimeException("Failed to scan metadata for table", e);
        }

        return rangeSplits;
    }

    public static Collection<Range> buildRanges(String start, String end, int splitsPerDay) throws ParseException {
        SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyyMMdd");

        List<Range> ranges = new ArrayList<>();

        Date startDate = dateFormatter.parse(start);
        Date endDate = dateFormatter.parse(end);

        Date current = startDate;

        while (!endDate.before(current)) {
            String row = dateFormatter.format(current);
            for (int i = 0; i < splitsPerDay; i++) {
                Text rowText = new Text(row + "_" + i);
                Key startKey = new Key(rowText, FI_START);
                Key endKey = new Key(rowText, FI_END);
                Range r = new Range(startKey, true, endKey, true);
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

        @Parameter(names = "--useScanServers", description = "Use scan servers for any accumulo scans")
        private boolean useScanServers = false;

        @Parameter(names = "--accumuloLocal", description = "Run the accumulo iterators local in offline mode")
        private boolean accumuloLocal = false;

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

        @Parameter(names = "--preserveTimestamps",
                        description = "preserve event timestamps when generating index entries instead of flooring them to the beginning of the day")
        private boolean preserveTimestamps = false;

        @Parameter(names = {"-h", "--help"}, description = "display help", help = true)
        private boolean help;
    }
}
