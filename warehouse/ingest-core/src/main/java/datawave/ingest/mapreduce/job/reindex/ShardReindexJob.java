package datawave.ingest.mapreduce.job.reindex;

import static datawave.ingest.mapreduce.job.ShardedTableMapFile.SPLIT_WORK_DIR;
import static org.apache.accumulo.core.conf.Property.TABLE_CRYPTO_PREFIX;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.crypto.CryptoFactoryLoader;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.spi.crypto.CryptoEnvironment;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.accumulo.hadoop.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.hadoop.mapreduce.InputFormatBuilder;
import org.apache.commons.collections.list.TreeList;
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

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

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
import datawave.ingest.mapreduce.job.writer.BulkContextWriter;
import datawave.ingest.mapreduce.job.writer.ChainedContextWriter;
import datawave.ingest.mapreduce.job.writer.ContextWriter;
import datawave.ingest.mapreduce.job.writer.DedupeContextWriter;
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
        String[] outputTableNames = configuration.get("job.output.table.names").split(",");
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
                jobConfig.inputFiles = org.apache.hadoop.util.StringUtils.join(",", getSplitsFromMetadata(accumuloClient, jobConfig.table,
                                new Range(new Key(jobConfig.startDate), true, new Key(jobConfig.endDate), true), jobConfig.useScanServers));
            } else if (!jobConfig.accumuloData) {
                // build ranges
                Collection<Range> ranges = null;
                if (jobConfig.reprocessEvents) {
                    ranges = buildSplittableRanges(accumuloClient, jobConfig.maxRangeThreads, jobConfig.blocksPerSplit, jobConfig.batchMode, configuration,
                                    jobConfig.table, jobConfig.startDate, jobConfig.endDate, jobConfig.shard, jobConfig.splitsPerDay, jobConfig.useScanServers);
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
        // see IngestJob
        if (jobConfig.useCombiner) {
            j.getConfiguration().setBoolean(BulkIngestKeyDedupeCombiner.USING_COMBINER, true);
            j.getConfiguration().setClass(EventMapper.CONTEXT_WRITER_CLASS, DedupeContextWriter.class, ChainedContextWriter.class);
            j.getConfiguration().setClass(DedupeContextWriter.CONTEXT_WRITER_CLASS, TableCachingContextWriter.class, ContextWriter.class);
        } else {
            j.getConfiguration().setClass(EventMapper.CONTEXT_WRITER_CLASS, TableCachingContextWriter.class, ChainedContextWriter.class);
        }
        j.getConfiguration().setClass(TableCachingContextWriter.CONTEXT_WRITER_CLASS, BulkContextWriter.class, ContextWriter.class);

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

    private static Set<String> getSplitsFromMetadata(AccumuloClient accumuloClient, String tableName, Range r, boolean useScanServers) {
        Set<String> rangeSplits = new HashSet<>();

        String tableId = accumuloClient.tableOperations().tableIdMap().get(tableName);

        if (tableId == null) {
            throw new RuntimeException("Could not locate table: '" + tableName + "'");
        }

        try (Scanner s = accumuloClient.createScanner("accumulo.metadata")) {
            ScannerBase.ConsistencyLevel consistencyLevel = ScannerBase.ConsistencyLevel.IMMEDIATE;
            if (useScanServers) {
                consistencyLevel = ScannerBase.ConsistencyLevel.EVENTUAL;
            }

            s.setConsistencyLevel(consistencyLevel);
            s.setRange(new Range(new Key(tableId + ";" + r.getStartKey().getRowData()), true, new Key(tableId + ";" + r.getEndKey().getRowData()), true));
            s.fetchColumnFamily("file");
            Iterator<Map.Entry<Key,Value>> metarator = s.iterator();
            while (metarator.hasNext()) {
                Map.Entry<Key,Value> next = metarator.next();
                ByteSequence file = next.getKey().getColumnQualifierData();
                rangeSplits.add(file.toString());
            }
        } catch (TableNotFoundException | AccumuloException | AccumuloSecurityException e) {
            throw new RuntimeException("Failed to scan metadata for table", e);
        }

        return rangeSplits;
    }

    public static RFile.Reader getRFileReader(Configuration config, Path rfile) throws IOException {
        log.info("getting reader for " + rfile);
        FileSystem fs = rfile.getFileSystem(config);
        if (!fs.exists(rfile)) {
            throw new FileNotFoundException(rfile + " does not exist");
        }

        CryptoService cs = CryptoFactoryLoader.getServiceForClient(CryptoEnvironment.Scope.TABLE, config.getPropsWithPrefix(TABLE_CRYPTO_PREFIX.name()));
        CachableBlockFile.CachableBuilder cb = new CachableBlockFile.CachableBuilder().fsPath(fs, rfile).conf(config).cryptoService(cs);

        return new RFile.Reader(cb);
    }

    public static Collection<Range> buildSplittableRanges(AccumuloClient accumuloClient, int maxRangeThreads, final int blocksPerSplit, String batchMode,
                    Configuration config, String table, String startDay, String endDay, int shard, int splitsPerDay, boolean useScanServers)
                    throws ParseException, IOException {
        ExecutorService threadPool = null;
        List<Future<List<Range>>> splitTasks = null;
        if (maxRangeThreads > 1) {
            threadPool = Executors.newFixedThreadPool(maxRangeThreads);
            splitTasks = new ArrayList<>();
        }

        log.info("building ranges startDate: " + startDay + " endDate: " + endDay + " splits: " + splitsPerDay);
        log.info("processing shard: " + shard);

        SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyyMMdd");

        List<Range> ranges = new ArrayList<>();

        Date startDate = dateFormatter.parse(startDay);
        Date endDate = dateFormatter.parse(endDay);

        Date current = startDate;
        while (!endDate.before(current)) {
            final Date dateToProcess = current;
            for (int i = 0; i < splitsPerDay; i++) {
                final int split = i;
                if (shard != -1 && i != shard) {
                    // skip
                    continue;
                }

                Callable<List<Range>> callable = () -> {
                    // TODO this isn't thread safe... fix
                    String row = dateFormatter.format(dateToProcess);
                    Text rowText = new Text(row + "_" + split);

                    log.info("Building splits for " + rowText);
                    // test index blocks againts acceptable ranges
                    Range testRange = new Range(new Key(rowText), true, new Key(rowText.toString() + '\u0000'), false);
                    List<Range> splitRanges = new ArrayList<>();

                    // for each split pull all the rfiles and build the splits
                    // add each split range based on config
                    Set<String> splitFiles = getSplitsFromMetadata(accumuloClient, table, testRange, useScanServers);

                    log.info("found " + splitFiles.size() + " rfiles for " + rowText);

                    TreeList indexes = new TreeList();
                    // open each file and read the index blocks writing their start key to get a distribution
                    // TODO done better in Verification job
                    for (String rfile : splitFiles) {
                        try (RFile.Reader rfileReader = getRFileReader(config, new Path(rfile))) {
                            try (FileSKVIterator indexIterator = rfileReader.getIndex()) {
                                while (indexIterator.hasTop()) {
                                    Key top = indexIterator.getTopKey();
                                    if (testRange.contains(top)) {
                                        indexes.add(top);
                                    }
                                    indexIterator.next();
                                }
                            }
                        }
                    }

                    // sort since treeList wont
                    Collections.sort(indexes);

                    // indexes contain all the index blocks, divide into split ranges regardless of file based on config
                    int blocksPerSplitAssigned = blocksPerSplit;
                    if (blocksPerSplitAssigned == -1) {
                        blocksPerSplitAssigned = indexes.size();
                    }

                    log.info(indexes.size() + " index blocks for row " + rowText);

                    double splitCount = Math.ceil((double) indexes.size() / (double) blocksPerSplitAssigned);
                    log.info("Creating + " + splitCount + " splits for " + rowText);
                    int createdSplits = 0;
                    Key splitStart = new Key(rowText);
                    while (createdSplits < splitCount) {
                        Key splitEnd;
                        if (indexes.size() > blocksPerSplitAssigned * (createdSplits + 1)) {
                            splitEnd = (Key) indexes.get((blocksPerSplitAssigned * (createdSplits + 1)));

                            // if using batch mode offset the end to ensure batches are not broken
                            // with small blocks or extremely dense data this count potentially cause a key out of order

                            if (!batchMode.equals("NONE")) {
                                // batch mode enabled
                                ByteSequence cf = splitEnd.getColumnFamilyData();
                                if (!ShardReindexMapper.isKeyD(cf) && !ShardReindexMapper.isKeyTF(cf) && !ShardReindexMapper.isKeyFI(cf)) {
                                    // it's an event key, so bump its cf to include the whole event
                                    Key oldEnd = splitEnd;
                                    splitEnd = splitEnd.followingKey(PartialKey.ROW_COLFAM);
                                    log.debug("Extended event range from " + oldEnd + " to " + splitEnd);
                                }
                            }
                        } else {
                            // end of range
                            splitEnd = new Key(rowText.toString() + '\u0000');
                        }
                        if (splitStart == splitEnd) {
                            log.warn("miscalculated split counts, discarding empty range");
                            createdSplits++;
                            continue;
                        }
                        splitRanges.add(new Range(splitStart, true, splitEnd, false));
                        createdSplits++;
                        splitStart = splitEnd;
                    }

                    log.info("Created " + splitRanges.size() + " ranges for " + rowText);
                    if (log.isDebugEnabled()) {
                        for (Range r : splitRanges) {
                            log.debug(r);
                        }
                    }

                    return splitRanges;
                };

                if (threadPool != null) {
                    splitTasks.add(threadPool.submit(callable));
                } else {
                    try {
                        ranges.addAll(callable.call());
                    } catch (Exception e) {
                        throw new RuntimeException("Problem fetching splits", e);
                    }
                }
            }

            Calendar calendar = Calendar.getInstance();
            calendar.setTime(current);
            calendar.add(Calendar.HOUR_OF_DAY, 24);
            current = calendar.getTime();
        }

        if (splitTasks != null) {
            for (Future<List<Range>> splitTask : splitTasks) {
                try {
                    ranges.addAll(splitTask.get());
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException("Failed to fetch splits", e);
                }
            }
        }

        return ranges;
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

        @Parameter(names = "--shard", description = "reprocess a specific shard of the day, not the entire day")
        private int shard;

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
