package datawave.ingest.mapreduce.job;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.hadoop.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.hadoopImpl.mapreduce.lib.InputConfigurator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.ingest.AccumuloHelper;
import datawave.ingest.data.config.ingest.IngestHelperInterface;
import datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;
import datawave.ingest.mapreduce.job.reduce.BulkIngestKeyAggregatingReducer;
import datawave.ingest.protobuf.Uid;
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

    private Configuration configuration = new Configuration();
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
        configuration.setBoolean("job.cleanupShard", jobConfig.cleanupShard);
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
        configuration.setBoolean("propagateDeletes", jobConfig.propagateDeletes);

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
            // build ranges
            Collection<Range> ranges = buildRanges(jobConfig.startDate, jobConfig.endDate, jobConfig.splitsPerDay);

            Properties accumuloProperties = builder.build();

            // do not auto adjust ranges because they will be clipped and drop the column qualifier. this will result in full table scans
            AccumuloInputFormat.configure().clientProperties(accumuloProperties).table(jobConfig.table).autoAdjustRanges(false).batchScan(false).ranges(ranges)
                            .store(j);
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
        j.setMapperClass(FiToGiMapper.class);

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

    public static class FiToGiMapper extends Mapper<Key,Value,BulkIngestKey,Value> {
        private static final Logger log = Logger.getLogger(FiToGiMapper.class);
        private final byte[] FI_START_BYTES = FI_START.getBytes();
        private final Value UID_VALUE = new Value(buildIndexValue().toByteArray());
        private final Value EMPTY_VALUE = new Value();

        private TypeRegistry typeRegistry;
        private Map<String,IngestHelperInterface> datatypeHelperCache;
        private boolean cleanupShard;

        private Text shardTable;
        private Text indexTable;
        private Text reverseIndexTable;

        private byte[] lastFiBytes;
        private String field;

        private boolean propagateDeletes;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration config = context.getConfiguration();
            this.typeRegistry = TypeRegistry.getInstance(config);

            this.cleanupShard = config.getBoolean("job.cleanupShard", false);

            this.shardTable = new Text(config.get(ShardedDataTypeHandler.SHARD_TNAME, "shard"));
            this.indexTable = new Text(config.get(ShardedDataTypeHandler.SHARD_GIDX_TNAME, "shardIndex"));
            this.reverseIndexTable = new Text(config.get(ShardedDataTypeHandler.SHARD_GRIDX_TNAME, "shardReverseIndex"));

            this.propagateDeletes = config.getBoolean("propagateDeletes", false);

            this.datatypeHelperCache = new HashMap<>();
        }

        @Override
        protected void map(Key key, Value value, Context context) throws IOException, InterruptedException {
            // This is all a bit awkward since DatawaveKey already parses the Key, but is in the datawave-query-core package. Minimally parse the Key for three
            // purposes
            // 1. Is it an FI key. Ranges should be created to defeat non fi ranges, but no verify and skip when possible
            // 2. Get the field name. Necessary for checking how a field is indexed
            // 3. Get the data type. The data type is used to get the correct IngestHelperInterface which is used to make decisions regarding how a field is
            // indexed. This may vary from data type to data type

            // ensure the key is an fi
            final byte[] cf = key.getColumnFamilyData().getBackingArray();
            if (WritableComparator.compareBytes(cf, 0, 3, FI_START_BYTES, 0, FI_START_BYTES.length) != 0) {
                // increment count of non-fi key
                context.getCounter("key types", "non-fi").increment(1l);
                return;
            }

            // check if it's the same target field as the last one
            if (lastFiBytes == null || WritableComparator.compareBytes(cf, 4, cf.length - 4, lastFiBytes, 4, lastFiBytes.length - 4) != 0) {
                // get the field from the cf
                field = new String(cf, 3, cf.length - 3);
                lastFiBytes = cf;
            }

            // parse the dataType from the cq
            final byte[] cq = key.getColumnQualifierData().getBackingArray();
            String uid = null;
            String dataType = null;
            StringBuilder fieldValue = new StringBuilder();
            int cqLen = cq.length;
            int uidNull = -1;
            for (int i = cqLen - 1; i >= 0; i--) {
                if (cq[i] == '\u0000') {
                    if (uid == null) {
                        uid = new String(cq, i + 1, cqLen - i - 1);
                        uidNull = i;
                    } else if (dataType == null) {
                        dataType = new String(cq, i + 1, uidNull - i - 1);
                        fieldValue.append(new String(cq, 0, i));
                        break;
                    }
                }
            }

            // get the type from the registry or create it if not already created. There is a cache inside the Type class
            IngestHelperInterface helper = null;

            // check the cache
            helper = datatypeHelperCache.get(dataType);
            if (helper == null) {
                for (Type registeredType : typeRegistry.values()) {
                    if (registeredType.outputName().equals(dataType)) {
                        try {
                            log.info("creating type: " + registeredType.typeName() + " for datatype " + dataType);
                            Type type = registeredType;
                            // try to create the type
                            helper = type.getIngestHelper(context.getConfiguration());
                            break;
                        } catch (Exception e) {
                            log.debug("failed to create type " + registeredType.typeName() + " skipping", e);
                        }
                        if (helper != null) {
                            // put it in the cache
                            datatypeHelperCache.put(dataType, helper);
                            break;
                        }
                    }
                }
            }

            if (helper == null) {
                log.error(key);
                throw new IllegalStateException("datatype " + dataType + " not found in Type Registry");
            }

            Text fieldValueText = null;
            Text fieldText = null;
            Text indexCq = null;
            boolean indexed = false;

            if (key.isDeleted() && !propagateDeletes) {
                context.getCounter("deletes", "skipped").increment(1l);
                context.progress();
                return;
            } else if (key.isDeleted()) {
                context.getCounter("deletes", "propagated").increment(1l);
            }

            // test if the field should have a global index built for it and write to context
            if (helper.isIndexedField(field) || helper.isIndexOnlyField(field)) {
                // generate the global index key and emit it
                fieldValueText = new Text(fieldValue.toString());
                fieldText = new Text(field);
                StringBuilder docId = new StringBuilder();
                docId.append(key.getRowData()).append('\u0000').append(dataType);
                indexCq = new Text(docId.toString());

                Key globalIndexKey = new Key(fieldValueText, fieldText, indexCq, key.getColumnVisibility(), key.getTimestamp());
                globalIndexKey.setDeleted(key.isDeleted());
                BulkIngestKey bik = new BulkIngestKey(indexTable, globalIndexKey);
                context.write(bik, UID_VALUE);
                indexed = true;
                context.getCounter("index", field).increment(1l);
            }

            // test if the field should have a reverse global index built for it and write to context
            if (helper.isReverseIndexedField(field)) {
                // reverse the field value
                fieldValueText = new Text(fieldValue.reverse().toString());
                if (fieldText == null) {
                    fieldText = new Text(field);
                    StringBuilder docId = new StringBuilder();
                    docId.append(key.getRowData()).append('\u0000').append(dataType);
                    indexCq = new Text(docId.toString());
                }

                Key globalReverseIndexKey = new Key(fieldValueText, fieldText, indexCq, key.getColumnVisibility(), key.getTimestamp());
                globalReverseIndexKey.setDeleted(key.isDeleted());
                // generate the global reverse index key and emit it
                BulkIngestKey bik = new BulkIngestKey(reverseIndexTable, globalReverseIndexKey);
                context.write(bik, UID_VALUE);
                indexed = true;
                context.getCounter("reverse index", field).increment(1l);
            }

            if (!indexed && cleanupShard) {
                // generate a delete key for this fi entry
                Key deleteKey = new Key(key);
                deleteKey.setDeleted(true);
                BulkIngestKey bik = new BulkIngestKey(shardTable, deleteKey);
                context.write(bik, EMPTY_VALUE);
                context.getCounter("shard cleanup", "fi").increment(1l);
            }

            // report progress to prevent timeouts
            context.progress();
        }

        // create a uid value with no count
        private static Uid.List buildIndexValue() {
            Uid.List.Builder uidBuilder = Uid.List.newBuilder();

            uidBuilder.setIGNORE(true);
            uidBuilder.setCOUNT(1);

            return uidBuilder.build();
        }
    }

    // define all job configuration options
    private class JobConfig {
        // startDate, endDate, splitsPerDay, and Table are all used with AccumuloInputFormat
        @Parameter(names = "--startDate", description = "yyyyMMdd start date", required = false)
        private String startDate;

        @Parameter(names = "--endDate", description = "yyyyMMdd end date", required = false)
        private String endDate;

        @Parameter(names = "--splitsPerDay", description = "splits for each day", required = false)
        private int splitsPerDay;

        @Parameter(names = "--table", description = "shard table", required = false)
        private String table = "shard";

        // alternatively accept RFileInputFormat
        @Parameter(names = "--inputFiles", description = "When set these files will be used for the job", required = false)
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
        @Parameter(names = "--resources", description = "configuration resources to be added", required = false)
        private String resources;

        @Parameter(names = "--reducers", description = "number of reducers to use", required = true)
        private int reducers;

        @Parameter(names = "--outputDir", description = "output directory", required = true)
        private String outputDir;

        @Parameter(names = "--destHdfs", description = "HDFS for --outputDir", required = true)
        private String destHdfs;

        @Parameter(names = "--cleanupShard", description = "generate delete keys when unused fi is found", required = false)
        private boolean cleanupShard;

        @Parameter(names = "--instance", description = "accumulo instance name", required = true)
        private String instance;

        @Parameter(names = "--zookeepers", description = "accumulo zookeepers", required = true)
        private String zookeepers;

        @Parameter(names = "--username", description = "accumulo username", required = true)
        private String username;

        @Parameter(names = "--password", description = "accumulo password", required = true)
        private String password;

        @Parameter(names = "--queryThreads", description = "batch scanner query threads, defaults to table setting", required = false)
        private int queryThreads = -1;

        @Parameter(names = "--batchSize", description = "accumulo batch size, defaults to table setting", required = false)
        private int batchSize = -1;

        @Parameter(names = "--propagateDeletes", description = "When true deletes are propagated to the indexes", required = false)
        private boolean propagateDeletes = false;
    }
}
