package datawave.ingest.mapreduce.job.reindex;

import static datawave.ingest.mapreduce.job.SplitsFile.SPLIT_WORK_DIR;
import static datawave.ingest.mapreduce.job.TableConfigurationUtil.JOB_OUTPUT_TABLE_NAMES;
import static datawave.ingest.mapreduce.job.TableConfigurationUtil.TABLES_CONFIGS_TO_CACHE;
import static datawave.ingest.mapreduce.job.reindex.ReindexedShardPartitioner.MAX_PARTITIONS;
import static datawave.ingest.mapreduce.job.reindex.ReindexedShardPartitioner.MIN_KEYS;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
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
import datawave.ingest.mapreduce.job.SplitsFile;
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
 * Job will read from the sharded data [accumulo online/offline][rfiles] and apply configured ingest rules. Each Key/Value pair will be reprocessed by ingest
 * using the applied resource configuration. Generating bulk loadable rfiles by table in the output directory
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
        configuration.setBoolean(ShardReindexMapper.METADATA_ONLY, jobConfig.metadataOnly);
        configuration.setBoolean(ShardReindexMapper.METADATA_DISABLE_FREQUENCY_COUNTS, jobConfig.disableMetadataCounts);

        if (jobConfig.generateMetadataFromFi) {
            validateMetadataFromFiOptions();
            configuration.setBoolean(ShardReindexMapper.METADATA_GENERATE_FROM_FI, jobConfig.generateMetadataFromFi);
            configuration.setBoolean(ShardReindexMapper.METADATA_GENERATE_EVENT_FROM_FI, jobConfig.generateMetadataEventFromFi);
            configuration.setBoolean(ShardReindexMapper.METADATA_GENERATE_RI_FROM_FI, jobConfig.generateMetadataRiFromFi);
            configuration.setBoolean(ShardReindexMapper.METADATA_GENEREATE_TF_FROM_FI, jobConfig.generateMetadataTfFromFi);
            configuration.setBoolean(ShardReindexMapper.LOOKUP_EVENT_METADATA_FROM_FI, jobConfig.lookupGeneratedEventFromFi);
            configuration.setBoolean(ShardReindexMapper.LOOKUP_RI_METADATA_FROM_FI, jobConfig.lookupGeneratedRiFromFi);
            configuration.setBoolean(ShardReindexMapper.LOOKUP_TF_METADATA_FROM_FI, jobConfig.lookupGeneratedTfFromFi);
        }

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

        // setup the output format
        IngestJob.configureMultiRFileOutputFormatter(configuration, jobConfig.compression, null, 0, 0, false);
        log.info("compression type: " + configuration.get("MultiRFileOutputFormatter.compression", "unknown"));
        // all changes to configuration must be before this line
        Job j = Job.getInstance(getConf());
        Configuration config = j.getConfiguration();
        SplitsFile.setupFile(j, config);

        // check if using some form of accumulo in input
        if (jobConfig.inputFiles == null) {
            if (jobConfig.startDate == null) {
                throw new IllegalArgumentException("startDate cannot be null when inputFiles are not specified");
            }

            if (jobConfig.endDate == null) {
                throw new IllegalArgumentException("endDate cannot be null when inputFiles are not specified");
            }

            AccumuloUtil accumuloUtil = new AccumuloUtil();
            accumuloUtil.setAccumuloClient(accumuloClient);

            if (jobConfig.accumuloMetadata) {
                // fetch the file list by scanning the accumulo.metadata table
                List<Map.Entry<String,List<String>>> filesForRanges = accumuloUtil.getFilesFromMetadataBySplit(jobConfig.table, jobConfig.startDate,
                                jobConfig.endDate);
                List<String> allFiles = new ArrayList<>();
                for (Map.Entry<String,List<String>> split : filesForRanges) {
                    allFiles.addAll(split.getValue());
                }
                jobConfig.inputFiles = String.join(",", allFiles);
            } else {
                // build ranges
                Collection<Range> ranges = null;
                if (jobConfig.reprocessEvents) {
                    RFileUtil rFileUtil = new RFileUtil(configuration);

                    List<String> includeDataTypes = null;
                    List<String> excludeDataTypes = null;
                    if (jobConfig.includeDataTypes != null && jobConfig.excludeDataTypes != null) {
                        throw new IllegalStateException("Cannot set both includeDataTypes and excludeDataTypes");
                    } else if (jobConfig.includeDataTypes != null) {
                        includeDataTypes = Arrays.asList(jobConfig.includeDataTypes.split(","));
                    } else if (jobConfig.excludeDataTypes != null) {
                        excludeDataTypes = Arrays.asList(jobConfig.excludeDataTypes.split(","));
                    }

                    ranges = buildSplittableRanges(accumuloUtil, rFileUtil, jobConfig.maxRangeThreads, jobConfig.blocksPerSplit,
                                    ShardReindexMapper.BatchMode.valueOf(jobConfig.batchMode), jobConfig.table, jobConfig.startDate, jobConfig.endDate,
                                    jobConfig.excludeFI, jobConfig.excludeTF, jobConfig.excludeD, includeDataTypes, excludeDataTypes);
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

        // see IngestJob
        if (jobConfig.useCombiner) {
            // setup the combiner context writer
            j.getConfiguration().setBoolean(BulkIngestKeyDedupeCombiner.USING_COMBINER, true);
            j.getConfiguration().setClass(EventMapper.CONTEXT_WRITER_CLASS, DedupeContextWriter.class, ChainedContextWriter.class);
            j.getConfiguration().setClass(DedupeContextWriter.CONTEXT_WRITER_CLASS, TableCachingContextWriter.class, ContextWriter.class);
            // set the combiner
            j.setCombinerClass(BulkIngestKeyDedupeCombiner.class);
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
            // there are some special considerations for partitioning beyond what would normally be done for an ingest job
            // partitioning the shard table for ideal splits, may cause severely lopsided reducers since the shard table is the source of the input
            // this may create hotspots in the reducers copying data from all the map tasks. To prevent this case, for each shard configure the
            // partitioner to hash across N reducers, this value should be configured based on the range of input shards, reducers, and volume of data
            if (jobConfig.minKeysPerShard != -1 || jobConfig.maxReducersPerShard != -1) {
                j.getConfiguration().set("partitioner.category.shardedTables", ReindexedShardPartitioner.class.getCanonicalName());
                if (jobConfig.minKeysPerShard != -1) {
                    j.getConfiguration().setInt("shardedTables." + MIN_KEYS, jobConfig.minKeysPerShard);
                }
                if (jobConfig.maxReducersPerShard != -1) {
                    j.getConfiguration().setInt("shardedTables." + MAX_PARTITIONS, jobConfig.maxReducersPerShard);
                }
            }

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

    private void validateMetadataFromFiOptions() {
        // only applies to jobs generating metadata from fi
        if (!jobConfig.generateMetadataFromFi
                        && (jobConfig.generateMetadataEventFromFi || jobConfig.generateMetadataRiFromFi || jobConfig.generateMetadataTfFromFi)) {
            throw new IllegalStateException("Cannot generate event/ri/tf entries from fi metadata if not generating metadata from fi");
        }

        if (jobConfig.lookupGeneratedEventFromFi && !jobConfig.generateMetadataEventFromFi) {
            throw new IllegalStateException("cannot lookup generated events from the fi if events are not being created from the fi");
        }

        if (jobConfig.lookupGeneratedRiFromFi && !jobConfig.generateMetadataRiFromFi) {
            throw new IllegalStateException("cannot lookup generated ri from the fi if ri are not being created from the fi");
        }

        if (jobConfig.lookupGeneratedTfFromFi && !jobConfig.generateMetadataTfFromFi) {
            throw new IllegalStateException("cannot lookup generated tf from the fi if tf are not being created from the fi");
        }

        if ((jobConfig.lookupGeneratedTfFromFi || jobConfig.lookupGeneratedRiFromFi || jobConfig.lookupGeneratedEventFromFi)
                        && (jobConfig.username == null || jobConfig.password == null || jobConfig.zookeepers == null || jobConfig.instance == null)) {
            throw new IllegalStateException("must configure accumulo username/password/zookeepers/instance if using lookups for generated fi metadata");
        }
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

    private static Callable<List<Range>> getSplitCallable(RFileUtil rFileUtil, String split, List<String> files, int blocksPerSplit,
                    Function<Key,Key> eventShiftFunction, List<Range> inclusions, List<Range> exclusions) {
        return () -> {
            log.info("found " + files.size() + " rfiles for " + split);

            // can either include or exclude, but not both
            if (!inclusions.isEmpty() && !exclusions.isEmpty()) {
                throw new IllegalStateException("cannot apply inclusions and exclusions to split ranges");
            }

            List<Range> ranges = rFileUtil.getRangeSplits(files, new Key(split), new Key(split, "" + '\uFFFF', "" + '\uFFFF'), blocksPerSplit,
                            eventShiftFunction);

            if (!inclusions.isEmpty()) {
                ranges = applyInclusions(ranges, inclusions);
            } else if (!exclusions.isEmpty()) {
                ranges = applyExclusions(ranges, exclusions);
            }

            return ranges;
        };
    }

    /**
     *
     * @param ranges
     *            ranges to apply exclusions to
     * @param exclusions
     *            non-overlapping disjoint ranges to exclude from ranges
     * @return ranges with the intersection of exclusions removed
     */
    public static List<Range> applyExclusions(List<Range> ranges, List<Range> exclusions) {
        // excluding may create holes in ranges, so every exclusion should be processed in whole before the next and always further reducing the set of ranges
        for (Range exclusion : exclusions) {
            List<Range> workingRanges = new ArrayList<>();
            for (Range orig : ranges) {
                // the clipped range is the intersection
                Range clipped = orig.clip(exclusion, true);
                if (clipped == null) {
                    // no intersection with the exclusion, keep the whole range
                    workingRanges.add(orig);
                } else if (clipped.equals(orig)) {
                    // entire range is excluded continue without adding it
                    continue;
                } else {
                    // partial intersection with the range, will be one of three cases. In all cases, flip the clipped inclusion flag
                    if (clipped.getStartKey().equals(orig.getStartKey()) && clipped.isStartKeyInclusive() == orig.isStartKeyInclusive()) {
                        // the beginning of the range has to be removed
                        workingRanges.add(new Range(clipped.getEndKey(), !clipped.isEndKeyInclusive(), orig.getEndKey(), orig.isEndKeyInclusive()));
                    } else if (clipped.getEndKey().equals(orig.getEndKey()) && clipped.isEndKeyInclusive() == orig.isEndKeyInclusive()) {
                        // the end of the range has to be removed
                        workingRanges.add(new Range(orig.getStartKey(), orig.isStartKeyInclusive(), clipped.getStartKey(), !clipped.isStartKeyInclusive()));
                    } else {
                        // the middle of the range, use clipped on both sides, making adjustments for inclusion flag?
                        Range startRange = new Range(orig.getStartKey(), orig.isStartKeyInclusive(), clipped.getStartKey(), !clipped.isStartKeyInclusive());
                        Range endRange = new Range(clipped.getEndKey(), !clipped.isEndKeyInclusive(), orig.getEndKey(), orig.isEndKeyInclusive());
                        workingRanges.add(startRange);
                        workingRanges.add(endRange);
                    }
                }
            }
            // after passing all the ranges, now reset the ranges to the working ranges for the next exclusion
            ranges = workingRanges;
        }

        return ranges;
    }

    /**
     *
     * @param ranges
     *            ranges to apply inclusions to
     * @param inclusions
     *            non-overlapping disjoint ranges
     * @return the intersection of the ranges with the inclusions
     */
    public static List<Range> applyInclusions(List<Range> ranges, List<Range> inclusions) {
        List<Range> restricted = new ArrayList<>();
        for (Range orig : ranges) {
            for (Range inclusion : inclusions) {
                // clipping provides the overlap of the two ranges against the original range. Since inclusions are non-overlapping always clip the original to
                // find the overlap
                Range clipped = orig.clip(inclusion, true);
                if (clipped != null) {
                    restricted.add(clipped);
                }
            }
        }

        return restricted;
    }

    public static Collection<Range> buildSplittableRanges(AccumuloUtil accumuloUtil, RFileUtil rFileUtil, int maxRangeThreads, final int blocksPerSplit,
                    ShardReindexMapper.BatchMode batchMode, String table, String startDay, String endDay) {
        return buildSplittableRanges(accumuloUtil, rFileUtil, maxRangeThreads, blocksPerSplit, batchMode, table, startDay, endDay, false, false, false,
                        Collections.emptyList(), Collections.emptyList());
    }

    public static Collection<Range> buildSplittableRanges(AccumuloUtil accumuloUtil, RFileUtil rFileUtil, int maxRangeThreads, final int blocksPerSplit,
                    ShardReindexMapper.BatchMode batchMode, String table, String startDay, String endDay, boolean excludeFi, boolean excludeTf,
                    boolean excludeD, List<String> includeDataTypes, List<String> excludeDataTypes) {
        List<Range> allRanges = new ArrayList<>();
        ExecutorService threadPool = null;
        List<Future<List<Range>>> splitTasks = null;
        if (maxRangeThreads > 1) {
            threadPool = Executors.newFixedThreadPool(maxRangeThreads);
            splitTasks = new ArrayList<>();
        }

        log.info("building ranges startDate: " + startDay + " endDate: " + endDay);

        Function<Key,Key> eventShiftFunction = Function.identity();
        if (!ShardReindexMapper.BatchMode.NONE.equals(batchMode)) {
            eventShiftFunction = new EventKeyAdjustment();
        }

        // check that these aren't the same
        if (startDay != null && startDay.equals(endDay)) {
            throw new IllegalArgumentException("endDay must be after startDay");
        }

        List<Map.Entry<String,List<String>>> filesBySplit;
        try {
            filesBySplit = accumuloUtil.getFilesFromMetadataBySplit(table, startDay, endDay);
        } catch (AccumuloException e) {
            throw new RuntimeException("Failed to lookup rfiles in metadata table", e);
        }

        for (Map.Entry<String,List<String>> fileSplit : filesBySplit) {
            String row = fileSplit.getKey();

            // convert to ranges for includes/excludes
            List<Range> includeRanges = new ArrayList<>();
            List<Range> excludeRanges = new ArrayList<>();

            setupIncludesAndExcludeRanges(row, includeRanges, excludeRanges, excludeFi, excludeTf, excludeD, includeDataTypes, excludeDataTypes);

            Callable<List<Range>> splitCallable = getSplitCallable(rFileUtil, row, fileSplit.getValue(), blocksPerSplit, eventShiftFunction, includeRanges,
                            excludeRanges);
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

    private static void setupIncludesAndExcludeRanges(String row, List<Range> includeRanges, List<Range> excludeRanges, boolean excludeFI, boolean excludeTF,
                    boolean excludeD, List<String> includeDataTypes, List<String> excludeDataTypes) {
        Range fiRange = new Range(new Key(row, "fi" + '\u0000'), true, new Key(row, "fi" + '\u0001'), false);
        Range tfRange = new Range(new Key(row, "tf"), true, new Key(row, "tf" + '\u0000'), false);
        Range dRange = new Range(new Key(row, "d"), true, new Key(row, "d" + '\u0000'), false);

        if (!includeDataTypes.isEmpty() && !excludeDataTypes.isEmpty()) {
            throw new IllegalArgumentException("cannot use datatype includes and excludes");
        }

        if (includeDataTypes.size() > 0) {
            // using includeRanges
            if (!excludeFI) {
                includeRanges.add(fiRange);
            }
            if (!excludeTF) {
                includeRanges.add(tfRange);
            }
            if (!excludeD) {
                includeRanges.add(dRange);
            }

            includeRanges.addAll(getDataTypeRanges(row, includeDataTypes));
        } else {
            // using excludeRanges
            if (excludeFI) {
                excludeRanges.add(fiRange);
            }
            if (excludeTF) {
                excludeRanges.add(tfRange);
            }
            if (excludeD) {
                excludeRanges.add(dRange);
            }

            excludeRanges.addAll(getDataTypeRanges(row, excludeDataTypes));
        }
    }

    private static List<Range> getDataTypeRanges(String row, List<String> dataTypes) {
        List<Range> ranges = new ArrayList<>();

        for (String dataType : dataTypes) {
            Range dataTypeRange = new Range(new Key(row, dataType + "\u0000"), true, new Key(row, dataType + "\u0001"), false);
            ranges.add(dataTypeRange);
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

        @Parameter(names = "--table", description = "shard table")
        private String table = "shard";

        @Parameter(names = "--accumuloMetadata", description = "fetch files from the accumulo.metadata table for the given start/end dates")
        private boolean accumuloMetadata = false;

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

        @Parameter(names = "--metadataOnly", description = "generates DatawaveMetadata entries only, no other output is generated")
        private boolean metadataOnly = false;

        @Parameter(names = "--disableMetadataCounts", description = "prevents f entries for metadata from being created when set")
        private boolean disableMetadataCounts = false;

        @Parameter(names = "--generateMetadataFromFi", description = "use fi entries to generate DatawaveMetadata entries")
        private boolean generateMetadataFromFi = false;

        @Parameter(names = "--genereateMetadataRiFromFi",
                        description = "use fi entries to generate ri entries in DatawaveMetadata when configured by the datatypes ingest helper")
        private boolean generateMetadataRiFromFi = false;

        @Parameter(names = "--genereateMetadataEventFromFi",
                        description = "use fi entries to generate e entries in DatawaveMetadata when configured by the datatypes ingest helper")
        private boolean generateMetadataEventFromFi = false;

        @Parameter(names = "--generateMetadataTfFromFi",
                        description = "use fi entries to generate tf entries in DatawaveMetadata when configured by the datatypes ingest helper")
        private boolean generateMetadataTfFromFi = false;

        @Parameter(names = "--lookupGeneratedRiFromFi",
                        description = "when --generateMetadataRiFromFi is set, verify reverse index entries once per field/dataType with an accumulo lookup [potentially expensive]")
        private boolean lookupGeneratedRiFromFi = false;

        @Parameter(names = "--lookupGeneratedEventFromFi",
                        description = "when --generateMetadataEventFromFi is set, verify event entries once per field/dataType with an accumulo lookup [potentially expensive]")
        private boolean lookupGeneratedEventFromFi = false;

        @Parameter(names = "--lookupGeneratedTfFromFi",
                        description = "when --generateMetadataTfFromFi is set, verify tf entries once per field/dataType with an accumulo lookup [potentially expensive]")
        private boolean lookupGeneratedTfFromFi = false;

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

        // parameters that will limit input ranges when using --reprocessEvents and --accumuloMetadata is not set
        @Parameter(names = "--excludeFI", description = "exclude fi from ranges generated when --reprocessEvents is set and --accumuloMetadata is not set")
        private boolean excludeFI = false;

        @Parameter(names = "--excludeTF", description = "exclude tf from ranges generated when --reprocessEvents is set and --accumuloMetadata is not set")
        private boolean excludeTF = false;

        @Parameter(names = "--excludeD", description = "exclude d column from ranges generated when --reprocessEvents is set and --accumuloMetadata is not set")
        private boolean excludeD = false;

        @Parameter(names = "--includeDataTypes",
                        description = "exclude all event data from ranges that doesn't match these comma deliminated data types when --reprocessEvents is set and --accumuloMetadata is not set. May not be combined with --excludeDataTypes")
        private String includeDataTypes;

        @Parameter(names = "--excludeDataTypes",
                        description = "exclude event data from ranges that matches these comma delimited data types when --reprocessEvents is set and --accumuloMetadata is not set. May not be combined with --includeDataTypes")
        private String excludeDataTypes;

        @Parameter(names = "--maxReducersPerShard",
                        description = "when set modify the partitioner of sharded data to spread a shard over multiple reducers. Setting this property will override the standard shard partitioner")
        private int maxReducersPerShard = -1;

        @Parameter(names = "--minKeysPerShard",
                        description = "when using more than 1 reducers per shard, only use an additional reducer if the number of keys emitted to a reducer for a shard is more than this value, -1 to round robin. Setting this property will override the standard shard partitioner")
        private int minKeysPerShard = -1;

        @Parameter(names = {"-h", "--help"}, description = "display help", help = true)
        private boolean help;
    }
}
