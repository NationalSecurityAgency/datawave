package datawave.ingest.mapreduce.job;

import static datawave.ingest.data.config.DataTypeHelper.Properties.DATA_NAME;
import static datawave.ingest.mapreduce.job.ShardedTableMapFile.SPLIT_WORK_DIR;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
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
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.hadoop.mapreduce.AccumuloInputFormat;
import org.apache.hadoop.conf.Configurable;
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
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.data.hash.HashUID;
import datawave.ingest.config.RawRecordContainerImpl;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.ingest.AccumuloHelper;
import datawave.ingest.data.config.ingest.IngestHelperInterface;
import datawave.ingest.mapreduce.ContextWrappedStatusReporter;
import datawave.ingest.mapreduce.handler.DataTypeHandler;
import datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;
import datawave.ingest.mapreduce.job.reduce.BulkIngestKeyAggregatingReducer;
import datawave.ingest.mapreduce.partition.MultiTableRangePartitioner;
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

        if (jobConfig.help) {
            cmd.usage();
            return 0;
        }

        log.setLevel(Level.INFO);
        Logger.getRootLogger().setLevel(Level.INFO);

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
        configuration.setBoolean("reprocessEvents", jobConfig.reprocessEvents);
        configuration.setBoolean("floorTimestamps", jobConfig.floorTimestamps);
        configuration.set("eventClass", jobConfig.eventClass);
        if (jobConfig.defaultDataType != null) {
            configuration.set("defaultDataType", jobConfig.defaultDataType);
        }
        if (jobConfig.dataTypeHandler != null) {
            configuration.set("dataTypeHandler", jobConfig.dataTypeHandler);
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
            try {
                Class inputFormatClass = Class.forName(jobConfig.inputFormatClass);
                if (!RFileInputFormat.class.isAssignableFrom(inputFormatClass)) {
                    throw new IllegalArgumentException("--inputFormatClass must be a type of RFileInputFormat");
                }
                j.setInputFormatClass(inputFormatClass);
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException("Could not set input format class: " + jobConfig.inputFormatClass);
            }
        } else {
            if (jobConfig.inputFormatClass != null) {
                try {
                    Class inputFormatClass = Class.forName(jobConfig.inputFormatClass);
                    j.setInputFormatClass(inputFormatClass);
                } catch (ClassNotFoundException e) {
                    throw new IllegalArgumentException("Could not set input format class: " + jobConfig.inputFormatClass);
                }
            } else {
                j.setInputFormatClass(AccumuloInputFormat.class);
            }
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

        System.exit(ToolRunner.run(null, new ShardReindexJob(), args));
    }

    public static class FiToGiMapper extends Mapper<Key,Value,BulkIngestKey,Value> {
        private static final Logger log = Logger.getLogger(FiToGiMapper.class);
        private final byte[] FI_START_BYTES = FI_START.getBytes();
        private final Value UID_VALUE = new Value(buildIndexValue().toByteArray());
        private final Value EMPTY_VALUE = new Value();

        private TypeRegistry typeRegistry;
        private Map<String,IngestHelperInterface> datatypeHelperCache;
        private String defaultDataType;
        private IngestHelperInterface defaultHelper;

        private Text shardTable;
        private Text indexTable;
        private Text reverseIndexTable;

        private byte[] lastFiBytes;
        private String field;

        private boolean cleanupShard;
        private boolean propagateDeletes;
        private boolean reprocessEvents;
        private Multimap<String,String> dataMap;
        private DataTypeHandler indexHandler;
        private String eventClass;
        private boolean floorTimestamps = true;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration config = context.getConfiguration();
            this.typeRegistry = TypeRegistry.getInstance(config);

            for (Type registeredType : typeRegistry.values()) {
                log.info("Registered type: " + registeredType.typeName() + " as " + registeredType.outputName());
            }

            this.cleanupShard = config.getBoolean("job.cleanupShard", false);

            this.shardTable = new Text(config.get(ShardedDataTypeHandler.SHARD_TNAME, "shard"));
            this.indexTable = new Text(config.get(ShardedDataTypeHandler.SHARD_GIDX_TNAME, "shardIndex"));
            this.reverseIndexTable = new Text(config.get(ShardedDataTypeHandler.SHARD_GRIDX_TNAME, "shardReverseIndex"));

            this.propagateDeletes = config.getBoolean("propagateDeletes", false);

            this.datatypeHelperCache = new HashMap<>();

            this.defaultDataType = config.get("defaultDataType");
            if (defaultDataType != null) {
                this.defaultHelper = typeRegistry.get(defaultDataType).getIngestHelper(config);
                log.info("default data type: " + defaultDataType);
            }

            this.reprocessEvents = config.getBoolean("reprocessEvents", false);
            log.info("reprocessing events: " + this.reprocessEvents);

            floorTimestamps = config.getBoolean("floorTimestamps", floorTimestamps);

            if (reprocessEvents) {
                dataMap = HashMultimap.create();

                eventClass = config.get("eventClass");

                if (defaultDataType == null) {
                    throw new IllegalArgumentException("defaultDataType must be set when reprocessing events");
                }

                // override the data name
                config.set(DATA_NAME, defaultDataType);

                String dataTypeHandler = config.get("dataTypeHandler");
                if (dataTypeHandler == null) {
                    throw new IllegalArgumentException("dataTypeHandler must be set when reprocessing events");
                }

                try {
                    Class dataTypeHandlerClass = Class.forName(dataTypeHandler);
                    this.indexHandler = (DataTypeHandler) dataTypeHandlerClass.newInstance();
                    indexHandler.setup(context);
                } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                    throw new IllegalArgumentException("could not create handler for data type handler: " + dataTypeHandler, e);
                }
            }
        }

        @Override
        protected void map(Key key, Value value, Context context) throws IOException, InterruptedException {
            // This is all a bit awkward since DatawaveKey already parses the Key, but is in the datawave-query-core package. Minimally parse the Key for three
            // purposes
            // 1. Is it an FI key. Ranges should be created to defeat non fi ranges, but no verify and skip when possible
            // 2. Get the field name. Necessary for checking how a field is indexed
            // 3. Get the data type. The data type is used to get the correct IngestHelperInterface which is used to make decisions regarding how a field is
            // indexed. This may vary from data type to data type

            // this is required for the partitioner, see EventMapper
            MultiTableRangePartitioner.setContext(context);

            // ensure the key is an fi
            final byte[] cf = key.getColumnFamilyData().getBackingArray();
            final int fiBaseLength = FI_START_BYTES.length;
            if (cf.length <= fiBaseLength || WritableComparator.compareBytes(cf, 0, fiBaseLength, FI_START_BYTES, 0, fiBaseLength) != 0) {
                // increment count of non-fi key
                context.getCounter("key type", "non-fi").increment(1l);
                if (cf.length == 2) {
                    // tf skip
                    context.getCounter("key type", "tf").increment(1l);
                    context.progress();
                    return;
                }
                if (this.reprocessEvents) {
                    try {
                        reprocessEventData(context, key);
                    } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
                        throw new RuntimeException("Cannot process event for: " + key, e);
                    }
                }
                context.progress();
                return;
            }

            context.getCounter("key type", "fi").increment(1l);

            // check if it's the same target field as the last one
            final int fiBaseOffset = fiBaseLength + 1;
            if (lastFiBytes == null || WritableComparator.compareBytes(cf, fiBaseOffset, cf.length - fiBaseOffset, lastFiBytes, fiBaseOffset,
                            lastFiBytes.length - fiBaseOffset) != 0) {
                // get the field from the cf
                field = new String(cf, fiBaseLength, cf.length - fiBaseLength);
                lastFiBytes = cf;
            }

            // parse the dataType from the cq
            final byte[] cq = key.getColumnQualifierData().getBackingArray();
            String uid = null;
            String dataType = null;
            ByteSequence fieldValue = null;
            int cqLen = cq.length;
            int uidNull = -1;
            for (int i = cqLen - 1; i >= 0; i--) {
                if (cq[i] == '\u0000') {
                    if (uid == null) {
                        uid = new String(cq, i + 1, cqLen - i - 1);
                        uidNull = i;
                    } else if (dataType == null) {
                        dataType = new String(cq, i + 1, uidNull - i - 1);
                        fieldValue = key.getColumnQualifierData().subSequence(0, i);
                        break;
                    }
                }
            }

            // get the type from the registry or create it if not already created
            IngestHelperInterface helper = getIngestHelper(dataType, context.getConfiguration());
            if (helper == null) {
                log.error(key);
                throw new IllegalStateException("datatype " + dataType + " not found in Type Registry");
            }

            context.getCounter("data type", dataType).increment(1l);

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
            if (helper.isIndexedField(field) && (helper.isIndexOnlyField(field) || !reprocessEvents)) {
                // generate the global index key and emit it
                fieldValueText = new Text(fieldValue.toString());
                fieldText = new Text(field);
                StringBuilder docId = new StringBuilder();
                docId.append(key.getRowData()).append('\u0000').append(dataType);
                indexCq = new Text(docId.toString());

                Key globalIndexKey = new Key(fieldValueText, fieldText, indexCq, key.getColumnVisibility(), floorTimestamp(key.getTimestamp()));
                globalIndexKey.setDeleted(key.isDeleted());
                BulkIngestKey bik = new BulkIngestKey(indexTable, globalIndexKey);
                context.write(bik, UID_VALUE);
                indexed = true;
                context.getCounter("index", field).increment(1l);
            }

            // test if the field should have a reverse global index built for it and write to context
            if (helper.isReverseIndexedField(field) && (helper.isIndexOnlyField(field) || !reprocessEvents)) {
                // reverse the field value
                fieldValueText = new Text(reverse(fieldValue.toString()));
                if (fieldText == null) {
                    fieldText = new Text(field);
                    StringBuilder docId = new StringBuilder();
                    docId.append(key.getRowData()).append('\u0000').append(dataType);
                    indexCq = new Text(docId.toString());
                }

                Key globalReverseIndexKey = new Key(fieldValueText, fieldText, indexCq, key.getColumnVisibility(), floorTimestamp(key.getTimestamp()));
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

        public static String reverse(String value) {
            return new StringBuilder(value).reverse().toString();
        }

        private long floorTimestamp(long timestamp) {
            if (this.floorTimestamps) {
                Calendar c = Calendar.getInstance();
                c.setTimeInMillis(timestamp);
                c.set(Calendar.HOUR_OF_DAY, 0);
                c.set(Calendar.MINUTE, 0);
                c.set(Calendar.SECOND, 0);
                c.set(Calendar.MILLISECOND, 0);

                return c.getTimeInMillis();
            }

            return timestamp;
        }

        // create a uid value with no count
        private static Uid.List buildIndexValue() {
            Uid.List.Builder uidBuilder = Uid.List.newBuilder();

            uidBuilder.setIGNORE(true);
            uidBuilder.setCOUNT(1);

            return uidBuilder.build();
        }

        private void reprocessEventData(Context context, Key eventKey)
                        throws IOException, InterruptedException, ClassNotFoundException, InstantiationException, IllegalAccessException {
            // reuse the same map to avoid the object creation time
            dataMap.clear();

            // cf will be datatype\0uid
            ByteSequence dataType = null;
            ByteSequence uid = null;
            ByteSequence cfByteSequence = eventKey.getColumnFamilyData();
            byte[] cf = cfByteSequence.getBackingArray();
            for (int i = 0; i < cf.length; i++) {
                if (cf[i] == '\u0000') {
                    dataType = cfByteSequence.subSequence(0, i);
                    uid = cfByteSequence.subSequence(i + 1, cf.length);

                    break;
                }
            }

            // cq will be field\0value
            ByteSequence field = null;
            ByteSequence value = null;
            ByteSequence cqByteSequence = eventKey.getColumnQualifierData();
            byte[] cq = cqByteSequence.getBackingArray();
            for (int i = 0; i < cq.length; i++) {
                if (cq[i] == '\u0000') {
                    field = cqByteSequence.subSequence(0, i);
                    value = cqByteSequence.subSequence(i + 1, cq.length);

                    break;
                }
            }

            // if any required data is missing malformed data
            if (dataType == null || uid == null || field == null || value == null) {
                log.warn("malformed data: " + eventKey);
                context.getCounter("event", "malformed").increment(1l);
                return;
            }

            context.getCounter("event", "reindex").increment(1l);

            // create an event
            RawRecordContainer event = getEvent(context.getConfiguration());
            log.debug("Creating uid from: " + uid);
            event.setId(HashUID.parse(uid.toString()));

            // create a modified type that has the right output name
            Type type = typeRegistry.get(this.defaultDataType);
            type = new Type(type.typeName(), dataType.toString(), type.getHelperClass(), type.getReaderClass(), type.getDefaultDataTypeHandlers(),
                            type.getFilterPriority(), type.getDefaultDataTypeFilters());
            context.getCounter("event type", dataType.toString()).increment(1l);

            // configure the event
            event.setDataType(type);
            event.setDate(eventKey.getTimestamp());
            event.setVisibility(eventKey.getColumnVisibilityParsed());

            // data in the event is not normalized, set it up for ingest normalization
            String utfSafeValue = new String(value.toArray(), StandardCharsets.UTF_8);
            dataMap.put(field.toString(), utfSafeValue);

            long startTime = System.currentTimeMillis();
            Multimap<String,NormalizedContentInterface> normalizedMap = defaultHelper.normalize(dataMap);
            long endTime = System.currentTimeMillis();
            context.getCounter("reindex", "normalizationTime").increment((endTime - startTime));
            startTime = System.currentTimeMillis();
            Multimap<BulkIngestKey,Value> keys = indexHandler.processBulk(eventKey, event, normalizedMap, new ContextWrappedStatusReporter(context));
            endTime = System.currentTimeMillis();
            context.getCounter("reindex", "processBulkTime").increment((endTime - startTime));

            for (BulkIngestKey generated : keys.keySet()) {
                context.getCounter("table", generated.getTableName().toString()).increment(1l);
                if (generated.getTableName().toString().equals(shardTable.toString())) {
                    // check the type of the key
                    cf = generated.getKey().getColumnFamilyData().getBackingArray();
                    // tf is always 2, fi would be longer than 3 and have a null in the correct spot
                    if (cf.length == 2) {
                        // tf
                        context.getCounter("shard", "tf").increment(1l);
                    } else if (cf.length > 3 && cf[2] == '\u0000') {
                        context.getCounter("shard", "fi").increment(1l);
                        context.getCounter("output type", getDataTypeFromFI(generated.getKey())).increment(1l);
                    } else {
                        // skip anything that isn't fi/tf
                        context.getCounter("reindex", "skip").increment(1l);
                        continue;
                    }
                }
                // write the keys
                for (Value v : keys.get(generated)) {
                    context.write(generated, v);
                }
            }
        }

        private String getDataTypeFromFI(Key fi) {
            final byte[] cq = fi.getColumnQualifierData().getBackingArray();
            boolean uid = false;
            String dataType = null;
            int cqLen = cq.length;
            int uidNull = -1;
            for (int i = cqLen - 1; i >= 0; i--) {
                if (cq[i] == '\u0000') {
                    if (!uid) {
                        uid = true;
                        uidNull = i;
                    } else if (dataType == null) {
                        return new String(cq, i + 1, uidNull - i - 1);
                    }
                }
            }

            return null;
        }

        private RawRecordContainer getEvent(Configuration config) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
            if (eventClass == null) {
                throw new IllegalStateException("eventClass must be set");
            }
            RawRecordContainer container = (RawRecordContainer) Class.forName(eventClass).newInstance();
            if (container instanceof Configurable) {
                ((Configurable) container).setConf(config);
            }

            return container;
        }

        private IngestHelperInterface getIngestHelper(String dataType, Configuration config) {
            // check the cache
            IngestHelperInterface helper = datatypeHelperCache.get(dataType);
            if (helper == null) {
                for (Type registeredType : typeRegistry.values()) {
                    if (registeredType.outputName().equals(dataType) || registeredType.typeName().equals(dataType)) {
                        try {
                            log.info("creating type: " + registeredType.typeName() + " for datatype " + dataType);
                            helper = registeredType.getIngestHelper(config);
                            datatypeHelperCache.put(dataType, helper);
                            break;
                        } catch (Exception e) {
                            // exceptions may be thrown when attempting to instantiate a type, this is very expected and not a real error
                            log.debug("failed to create type " + registeredType.typeName() + " skipping", e);
                        }
                    }
                }
            }

            // assign a default helper if no helper was found for this data type
            if (helper == null) {
                helper = this.defaultHelper;
                datatypeHelperCache.put(dataType, helper);
            }

            return helper;
        }
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

        // alternatively accept RFileInputFormat
        @Parameter(names = "--inputFiles", description = "When set these files will be used for the job. Should be comma delimited hdfs glob strings")
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

        @Parameter(names = "--defaultDataType", description = "The datatype to apply to all data that has an invalid type")
        private String defaultDataType;

        @Parameter(names = "--dataTypeHandler", description = "DataTypeHandler to use to reprocess events")
        private String dataTypeHandler;

        @Parameter(names = "--inputFormatClass", description = "The input format class to apply, defaults to datawave.ingest.mapreduce.job.RFileInputFormat")
        private String inputFormatClass = RFileInputFormat.class.getCanonicalName();

        @Parameter(names = "--reprocessEvents", description = "When set event data will be reprocessed to produce fi, tf, and global index entries")
        private boolean reprocessEvents;

        @Parameter(names = "--eventClass", description = "When set use this class to instantiate events, default datawave.ingest.config.RawRecordContainerImpl")
        private String eventClass = RawRecordContainerImpl.class.getCanonicalName();

        @Parameter(names = "--floorTimestamps", description = "Floor timestamps of generated keys to the current day, default true")
        private boolean floorTimestamps = true;

        @Parameter(names = {"-h", "--help"}, description = "Display help for input parameters", help = true)
        private boolean help;
    }
}
