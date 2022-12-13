package datawave.mapreduce.shardStats;

import datawave.ingest.data.config.ingest.AccumuloHelper;
import datawave.ingest.mapreduce.handler.shard.NumShards;
import datawave.ingest.mapreduce.job.IngestJob;
import datawave.ingest.mapreduce.job.MultiRFileOutputFormatter;
import datawave.mr.bulk.BulkInputFormat;
import datawave.mr.bulk.MultiRfileInputformat;
import datawave.query.Constants;
import datawave.tables.schema.ShardFamilyConstants;
import datawave.util.StringUtils;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iterators.user.VersioningIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;

import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Set;

/**
 * Map/Reduce job for determining the cardinality and selectivity for each distinct field name/dataype pair that exists in the shard table. Due to the large
 * amount of data required to determine an exact cardinality and selectivity, the {@link com.clearspring.analytics.stream.cardinality.HyperLogLogPlus} class is
 * used to provide an estimate of the cardinality and selectivity. For additional information, read the following papers:
 * <ul>
 * <li>"HyperLogLo: the analysis of a near-optimal cardinality estimation algorithm"</li>
 * <li>"HyperLogLog in Practive: Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm" written by Stefan Huele, Marc NunKesser, and
 * Alexander Hall.</li>
 * </ul>
 * <p>
 * HyperLogLogPlus Notes
 * <p>
 * There are two parameters that are used to control how the hyper log class functions:
 * <ul>
 * <li>a sparse precision is used when the data set is small</li>
 * <li>a normal precision is used when the dataset exceeds a threshold</li>
 * </ul>
 * Valid values for the sparse and normal precision are from 4 to 32, and the normal precision cannot b greater than the sparse precision. setting the sparse
 * precision to 0 will only use the normal precision for all calculations. Setting the sparse precision to 0 has not been tested.
 * <p>
 * Higher precision values translate to more buckets and thus higher memory utilization. The number of buckets that are allocated is defined by the equation:
 * 
 * <pre>
 *         buckets = 2 ** N   where N is the precision
 * </pre>
 * 
 * As the precision is increased, the error percentage decreases and can be modeled by the equation:
 * 
 * <pre>
 *         error percentage = 1.04 / sqrt(M)   where M is the number of buckets
 * </pre>
 * <p>
 * Precision values should be selected based upon the desired error percentage and memory utilization. Based upon the information in the referenced papers,
 * using the highest precision (32) will consume about 1.5K of memory.
 * </p>
 * <p>
 * During development an anomaly was discovered when the sparse and normal precision values differed by more than 6. In random instances, when the cardinality
 * for a large dataset was also large, the estimated cardinality produced by the hyper log class differed from actual cardinality up to 25%. The larger the
 * difference between the sparse and normal precision values, the larger the error percentage. When specifying the sparse and normal precision values, it is
 * best to choose values that are the same. All testing using the same sparse and normal precision resulted in consistent results that were within the expected
 * error margin.
 * </p>
 */
public class StatsJob extends IngestJob {
    
    // default log level for all classes
    static final Level DEFAULT_LOG_LEVEL = Level.INFO;
    
    // default values used by both mapper and reducer
    // constants for hyperloglogplus
    static final int HYPERLOG_SPARSE_DEFAULT_VALUE = 24;
    static final int HYPERLOG_NORMAL_DEFAULT_VALUE = 24;
    static final String HYPERLOG_NORMAL_OPTION = "shardStats.hyperlog.normal";
    static final String HYPERLOG_SPARSE_OPTION = "shardStats.hyperlog.sparse";
    
    static final String OUTPUT_TABLE_NAME = "shardStats.table.name";
    static final String INPUT_TABLE_NAME = "shardStats.input.table";
    static final String STATS_JOB_LOG_LEVEL = "stats.job.log.level";
    static final String STATS_VISIBILITY = "shardStats.visibility";
    
    // instance members
    private String inputTableName;
    private String outputTableName;
    
    public static void main(String[] args) throws Exception {
        System.out.println("Running main");
        System.exit(ToolRunner.run(new Configuration(), new StatsJob(), args));
    }
    
    @Override
    protected Configuration parseArguments(String[] args, Configuration conf) throws ClassNotFoundException, URISyntaxException, IllegalArgumentException {
        Configuration parseConf = super.parseArguments(args, conf);
        
        // force bulk job
        this.outputMutations = false;
        this.useMapOnly = false;
        
        if (null != parseConf) {
            parseStatsOptions(args, parseConf);
            
            parseConf.setStrings(MultiRFileOutputFormatter.CONFIGURED_TABLE_NAMES, this.outputTableName);
            
            this.mapper = StatsHyperLogMapper.class;
            this.inputFormat = MultiRfileInputformat.class;
        }
        
        return parseConf;
    }
    
    @Override
    protected void configureInputFormat(Job job, AccumuloHelper cbHelper, Configuration conf) throws Exception {
        BulkInputFormat.setZooKeeperInstance(conf, cbHelper.getInstanceName(), cbHelper.getZooKeepers());
        
        // add the versioning iterator
        IteratorSetting cfg = new IteratorSetting(100, VersioningIterator.class);
        BulkInputFormat.addIterator(conf, cfg);
        
        // get authorizations
        Connector conn = cbHelper.getConnector();
        SecurityOperations secOps = conn.securityOperations();
        Authorizations auths = secOps.getUserAuthorizations(cbHelper.getUsername());
        
        BulkInputFormat.setInputInfo(job, cbHelper.getUsername(), cbHelper.getPassword(), this.inputTableName, auths);
        final Set<Range> scanShards = calculateRanges(conf);
        BulkInputFormat.setRanges(job, scanShards);
        
        super.configureInputFormat(job, cbHelper, conf);
    }
    
    @Override
    protected void configureJob(Job job, Configuration conf, Path workDirPath, FileSystem outputFs) throws Exception {
        super.configureJob(job, conf, workDirPath, outputFs);
        
        job.setReducerClass(StatsHyperLogReducer.class);
    }
    
    /**
     * Processes the options for the Stats job.
     * 
     * @param inArgs
     *            input arguments to stats job
     * @param conf
     *            hadoop configuration
     */
    private void parseStatsOptions(final String[] inArgs, final Configuration conf) {
        for (int n = 0; n < inArgs.length; n++) {
            String[] args = inArgs[n].split("=");
            JobArg arg = JobArg.getOption(args[0]);
            if (null != arg) {
                switch (arg) {
                // job args
                    case JOB_LOG_LEVEL:
                        Level level = Level.toLevel(args[1], DEFAULT_LOG_LEVEL);
                        log.setLevel(level);
                        log.info("log level set to " + level.toString());
                        break;
                    default:
                        conf.set(arg.key, args[1]);
                        break;
                }
            }
        }
        
        // validate required entries
        String vis = conf.get(STATS_VISIBILITY);
        if (null == vis) {
            throw new IllegalStateException("column visibility property (" + STATS_VISIBILITY + ") is not set");
        }
        log.info("column visibility (" + vis + ")");
        
        this.inputTableName = conf.get(INPUT_TABLE_NAME);
        if (null == this.inputTableName) {
            throw new IllegalStateException("input table property (" + INPUT_TABLE_NAME + ") is not set");
        }
        log.info("input table(" + this.inputTableName + ")");
        
        this.outputTableName = conf.get(OUTPUT_TABLE_NAME);
        if (null == this.outputTableName) {
            throw new IllegalStateException("output table property (" + OUTPUT_TABLE_NAME + ") is not set");
        }
        log.info("output table(" + this.outputTableName + ")");
    }
    
    private Set<Range> calculateRanges(Configuration conf) {
        final Set<Range> ranges = new HashSet<>();
        String[] shardsAndDays = StringUtils.split(this.inputPaths, ',');
        this.inputPaths = "";
        
        // for each datatype of interest
        log.info("using the following ranges");
        for (String shard : shardsAndDays) {
            // if shard is actualy a day, split into shards
            if (shard.indexOf('_') < 0) {
                // shard should be a day
                int numShards = new NumShards(conf).getNumShards(shard);
                if (numShards < 0) {
                    throw new IllegalArgumentException("Cannot determine the number of shards. See: " + NumShards.class.getName());
                }
                
                // create a range for each shard
                for (int n = 0; n < numShards; n++) {
                    String shardNum = shard + '_' + n;
                    Key firstKey = new Key(shardNum, ShardFamilyConstants.FI + '\0');
                    Key endKey = new Key(shardNum, ShardFamilyConstants.FI + '\0' + Constants.MAX_UNICODE_STRING);
                    
                    Range r = new Range(firstKey, true, endKey, false);
                    ranges.add(r);
                    log.info(r.toString());
                }
            } else {
                Key firstKey = new Key(shard, ShardFamilyConstants.FI + '\0');
                Key endKey = new Key(shard, ShardFamilyConstants.FI + '\0' + Constants.MAX_UNICODE_STRING);
                
                Range r = new Range(firstKey, true, endKey, false);
                ranges.add(r);
                log.info(r.toString());
            }
        }
        
        return ranges;
    }
    
    /**
     * Helper enum to manage options with default values.
     */
    private enum JobArg {
        // stats job options
        INPUT_TABLE(INPUT_TABLE_NAME, ""),
        OUTPUT_TABLE(OUTPUT_TABLE_NAME, ""),
        COLUMN_VISIBILITY(STATS_VISIBILITY, ""),
        JOB_LOG_LEVEL(STATS_JOB_LOG_LEVEL, DEFAULT_LOG_LEVEL),
        HYPERLOG_NORMAL_PRECISION(HYPERLOG_NORMAL_OPTION, HYPERLOG_NORMAL_DEFAULT_VALUE),
        HYPERLOG_SPARSE_PRECISION(HYPERLOG_SPARSE_OPTION, HYPERLOG_SPARSE_DEFAULT_VALUE),
        
        // mapper specific options
        MAPPER_INPUT_INTERVAL(StatsHyperLogMapper.STATS_MAPPER_INPUT_INTERVAL, StatsHyperLogMapper.DEFAULT_INPUT_INTERVAL),
        MAPPER_OUTPUT_INTERVAL(StatsHyperLogMapper.STATS_MAPPER_OUTPUT_INTERVAL, StatsHyperLogMapper.DEFAULT_OUTPUT_INTERVAL),
        MAPPER_LOG_LEVEL(StatsHyperLogMapper.STATS_MAPPER_LOG_LEVEL, DEFAULT_LOG_LEVEL),
        
        // reducer specific options
        MIN_COUNT(StatsHyperLogReducer.STATS_MIN_COUNT, StatsHyperLogReducer.DEFAULT_MIN_COUNT),
        REDUCER_COUNTS(StatsHyperLogReducer.STATS_REDUCER_COUNTS, false),
        REDUCER_VALUE_INTERVAL(StatsHyperLogReducer.STATS_REDUCER_VALUE_INTERVAL, StatsHyperLogReducer.DEFAULT_VALUE_INTERVAL),
        REDUCER_LOG_LEVEL(StatsHyperLogReducer.STATS_REDUCER_LOG_LEVEL, DEFAULT_LOG_LEVEL), ;
        
        static JobArg getOption(String option) {
            while (option.startsWith("-")) {
                option = option.substring(1);
            }
            for (JobArg opt : JobArg.values()) {
                if (opt.key.equals(option)) {
                    return opt;
                }
            }
            
            return null;
        }
        
        private String key;
        private Object defaultValue;
        
        JobArg(String kVal, Object dVal) {
            this.key = kVal;
            this.defaultValue = dVal;
        }
        
        @Override
        public String toString() {
            // @formatter:off
            return "JobArg{" +
                    "key='" + key + '\'' +
                    ", defaultValue=" + defaultValue +
                    '}';
            // @formatter:on
        }
    }
}
