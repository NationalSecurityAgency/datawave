package datawave.mapreduce.shardStats;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.partition.MultiTableRangePartitioner;
import datawave.mr.bulk.split.FileRangeSplit;
import datawave.mr.bulk.split.TabletSplitSplit;
import datawave.query.data.parsers.DatawaveKey;
import datawave.util.StringUtils;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static datawave.mapreduce.shardStats.StatsJob.DEFAULT_LOG_LEVEL;
import static datawave.mapreduce.shardStats.StatsJob.HYPERLOG_NORMAL_DEFAULT_VALUE;
import static datawave.mapreduce.shardStats.StatsJob.HYPERLOG_NORMAL_OPTION;
import static datawave.mapreduce.shardStats.StatsJob.HYPERLOG_SPARSE_DEFAULT_VALUE;
import static datawave.mapreduce.shardStats.StatsJob.HYPERLOG_SPARSE_OPTION;
import static datawave.mapreduce.shardStats.StatsJob.OUTPUT_TABLE_NAME;

class StatsHyperLogMapper extends Mapper<Key,Value,BulkIngestKey,Value> {
    private static final Logger log = Logger.getLogger(StatsHyperLogMapper.class);
    
    // mapper parameter keys
    static final String STATS_MAPPER_INPUT_INTERVAL = "stats.mapper.input.interval";
    static final String STATS_MAPPER_LOG_LEVEL = "stats.mapper.log.level";
    static final String STATS_MAPPER_OUTPUT_INTERVAL = "stats.mapper.output.interval";
    static final String STATS_MAPPER_UNIQUE_COUNT = "stats.mapper.uniquecount";
    
    static final int DEFAULT_INPUT_INTERVAL = 10_000_000;
    static final int DEFAULT_OUTPUT_INTERVAL = 100;
    
    private static final char NULL_CHAR = '\0';
    
    // ===========================
    // instance members
    private Text outputTable;
    // total of all 'fi' entries
    private long total;
    // total of all unique output entries
    private long outputTotal;
    // configured visibility for output values
    private ColumnVisibility visibility;
    // maintains the current field name that is being processed
    private String currentFieldName;
    private boolean setPartitionerContext;
    
    // ===========================
    // hyperlog values
    private int normalPrecision;
    private int sparsePrecision;
    
    // ===========================
    // debug parameters
    // log interval heartbeat for output entries
    private int logOutputInterval;
    // log interval heartbeat for input row entries
    private int logInputInterval;
    // sum values to determine an exact unique count for each field name/datatype pair
    private boolean sumUniqueCounts;
    
    private final Map<String,DataTypeInfo> dataTypeMapping = new HashMap<>();
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        if (log.isTraceEnabled()) {
            log.trace("setup(" + context + ")");
        }
        super.setup(context);
        Configuration conf = context.getConfiguration();
        
        MultiTableRangePartitioner.setContext(context);
        
        // set log level if configured
        String logLevel = conf.get(STATS_MAPPER_LOG_LEVEL);
        Level level = Level.toLevel(logLevel, DEFAULT_LOG_LEVEL);
        log.setLevel(level);
        log.info("log level set to " + level.toString());
        
        // set stats configuration data
        this.outputTable = new Text(conf.get(OUTPUT_TABLE_NAME));
        log.info("output table(" + this.outputTable.toString() + ")");
        
        this.logOutputInterval = conf.getInt(STATS_MAPPER_OUTPUT_INTERVAL, DEFAULT_OUTPUT_INTERVAL);
        log.info("output log interval(" + this.logOutputInterval + ")");
        this.logInputInterval = conf.getInt(STATS_MAPPER_INPUT_INTERVAL, DEFAULT_INPUT_INTERVAL);
        log.info("input log interval(" + this.logInputInterval + ")");
        this.sumUniqueCounts = conf.getBoolean(STATS_MAPPER_UNIQUE_COUNT, false);
        log.info("unique counts(" + this.sumUniqueCounts + ")");
        
        // hyperlog precision
        this.normalPrecision = conf.getInt(HYPERLOG_NORMAL_OPTION, HYPERLOG_NORMAL_DEFAULT_VALUE);
        log.info("normal precision(" + this.normalPrecision + ")");
        this.sparsePrecision = conf.getInt(HYPERLOG_SPARSE_OPTION, HYPERLOG_SPARSE_DEFAULT_VALUE);
        log.info("sparse precision(" + this.sparsePrecision + ")");
        
        this.visibility = new ColumnVisibility(conf.get(StatsJob.STATS_VISIBILITY));
        
        // initialize type registry
        TypeRegistry.getInstance(conf);
        
        InputSplit split = context.getInputSplit();
        log.info("Got a split of type " + split.getClass());
        if (log.isInfoEnabled()) {
            if (split instanceof TabletSplitSplit) {
                TabletSplitSplit tabletSplit = (TabletSplitSplit) split;
                log.info("Has " + tabletSplit.getLength() + " file range splits");
                for (int i = 0; i < tabletSplit.getLength(); i++) {
                    FileRangeSplit subSplit = (FileRangeSplit) tabletSplit.get(i);
                    log.info("Got a file range spit of " + subSplit + " with a range of " + subSplit.getRange());
                }
            }
        }
        
        if (log.isTraceEnabled()) {
            log.trace("Completed setup(" + context + ")");
        }
    }
    
    @Override
    protected void map(Key key, Value value, Context context) throws IOException, InterruptedException {
        if (log.isTraceEnabled()) {
            log.trace("map(" + key + ", " + value + ")");
        }
        
        // range should find all field index rows
        String[] colf = StringUtils.split(key.getColumnFamily().toString(), NULL_CHAR);
        if ("fi".equals(colf[0])) {
            this.total++;
            if (0 == this.total % this.logInputInterval) {
                log.info("input row count(" + this.total + ")");
            }
            DatawaveKey dwKey = new DatawaveKey(key);
            if (log.isTraceEnabled()) {
                log.trace("mapper input(" + dwKey.toString() + ")");
            }
            
            String fieldName = dwKey.getFieldName();
            if (null == this.currentFieldName) {
                this.currentFieldName = fieldName;
            } else {
                if (!this.currentFieldName.equals(fieldName)) {
                    flushFieldValues(context);
                    this.currentFieldName = fieldName;
                }
            }
            
            // add value to proper data type
            String dataType = dwKey.getDataType();
            DataTypeInfo typeInfo = this.dataTypeMapping.computeIfAbsent(dataType,
                            k -> new DataTypeInfo(dwKey, this.sumUniqueCounts, this.normalPrecision, this.sparsePrecision));
            typeInfo.add(dwKey.getFieldValue());
        }
        
        context.progress();
        
        if (log.isTraceEnabled()) {
            log.trace("Completed map(" + key + ", " + value + ")");
        }
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (log.isTraceEnabled()) {
            log.trace("context(" + context + ")");
        }
        
        // write last enrty
        flushFieldValues(context);
        
        log.info("totals keys(" + this.outputTotal + ") +  'fi' values(" + this.total + ")");
        
        super.cleanup(context);
    }
    
    private void flushFieldValues(Context context) throws IOException, InterruptedException {
        if (null != this.currentFieldName) {
            Text outRow = new Text();
            Text outFam = new Text();
            Text outQual = new Text();
            Value val = new Value();
            
            for (DataTypeInfo entry : this.dataTypeMapping.values()) {
                this.outputTotal++;
                
                outRow.set(entry.key.getFieldName());
                String row = entry.key.getRow().toString();
                int idx = row.indexOf('_');
                String date = row.substring(0, idx);
                outFam.set(date);
                outQual.set(entry.key.getDataType());
                Key key = new Key(outRow, outFam, outQual, this.visibility, 0);
                
                BulkIngestKey bulkKey = new BulkIngestKey(this.outputTable, key);
                StatsHyperLogSummary sum = entry.getStatsSummary();
                if (log.isDebugEnabled()) {
                    log.debug("output: key(" + bulkKey.getKey() + ")");
                    log.debug("stats(" + sum.statsString() + ")");
                }
                
                val.set(sum.toByteArray());
                context.write(bulkKey, val);
                
                if (0 == (this.outputTotal % this.logOutputInterval)) {
                    log.info("totals => keys(" + this.outputTotal + ") values(" + this.total + ")");
                }
            }
        }
        this.dataTypeMapping.clear();
    }
    
    /**
     * POJO to contain values for a single datatype.
     */
    private static class DataTypeInfo {
        final DatawaveKey key;
        private long count;
        private final HyperLogLogPlus logPlus;
        
        // for generating unique count
        private final boolean unique;
        private final Set<String> uniqueValues = new HashSet<>();
        
        DataTypeInfo(DatawaveKey aKey, boolean generateUniqueCount, int normal, int sparse) {
            this.key = aKey;
            this.unique = generateUniqueCount;
            this.logPlus = new HyperLogLogPlus(normal, sparse);
        }
        
        void add(String val) {
            this.count++;
            this.logPlus.offer(val);
            if (this.unique) {
                this.uniqueValues.add(val);
            }
        }
        
        StatsHyperLogSummary getStatsSummary() throws IOException {
            return new StatsHyperLogSummary(count, logPlus, this.uniqueValues.size());
        }
        
        @Override
        public String toString() {
            // @formatter:off
            return "DataTypeInfo{" +
                    "count=" + count +
                    ", cardinality=" + logPlus.cardinality() +
                    ", uniqueValues=" + uniqueValues.size() +
                    '}';
            // @formatter:on
        }
    }
}
