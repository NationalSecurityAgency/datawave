package datawave.mapreduce.shardStats;

import java.io.IOException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;

import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.reduce.BulkIngestKeyAggregatingReducer;

/**
 * Reducer process for creating index statistics from the shard data. The mapper process creates output keys based upon the field name, date, and datatype.
 */
public class StatsHyperLogReducer extends BulkIngestKeyAggregatingReducer<BulkIngestKey,Value> {
    private static final Logger log = Logger.getLogger(StatsHyperLogReducer.class);

    // reducer parameter keys
    static final String STATS_MIN_COUNT = "shardStats.minCount";
    static final String STATS_REDUCER_COUNTS = "stats.reducer.counts";
    static final String STATS_REDUCER_LOG_LEVEL = "stats.reducer.log.level";
    static final String STATS_REDUCER_VALUE_INTERVAL = "stats.reducer.value.interval";

    // default values
    static final int DEFAULT_MIN_COUNT = 0;
    static final int DEFAULT_VALUE_INTERVAL = 20;

    // debug/diagnostics values
    // total number of keys written to output table
    private int totalKeys;
    // interval for processing values for a single input key
    private int valueInterval;
    // minimum total count required to create an entry for a field name/datatype pair
    private int minCount;
    // produce counts only - do not write to table
    private boolean countsOnly;

    // hyperlog properties
    private int normalPrecision;
    private int sparsePrecision;

    // timestamp for bulk ingest key
    private long timestamp;

    @Override
    public void setup(Configuration conf) throws IOException, InterruptedException {
        super.setup(conf);

        // set log level if configured
        String logLevel = conf.get(STATS_REDUCER_LOG_LEVEL, StatsJob.DEFAULT_LOG_LEVEL.toString());
        Level level = Level.toLevel(logLevel, Level.INFO);
        log.info("log level set to " + level.toString());

        this.valueInterval = conf.getInt(STATS_REDUCER_VALUE_INTERVAL, DEFAULT_VALUE_INTERVAL);
        log.info("log value interval(" + this.valueInterval + ")");

        this.minCount = conf.getInt(STATS_MIN_COUNT, DEFAULT_MIN_COUNT);
        log.info("minimum count(" + this.minCount + ")");

        this.countsOnly = conf.getBoolean(STATS_REDUCER_COUNTS, false);
        log.info("counts only(" + this.countsOnly + ")");

        // hyperlog precision
        this.normalPrecision = conf.getInt(StatsJob.HYPERLOG_NORMAL_OPTION, StatsJob.HYPERLOG_NORMAL_DEFAULT_VALUE);
        log.info("hyperlog normal precision(" + this.normalPrecision + ')');
        this.sparsePrecision = conf.getInt(StatsJob.HYPERLOG_SPARSE_OPTION, StatsJob.HYPERLOG_SPARSE_DEFAULT_VALUE);
        log.info("hyperlog sparse precision(" + this.sparsePrecision + ')');

        this.timestamp = System.currentTimeMillis();
    }

    @Override
    public void finish(TaskInputOutputContext<?,?,BulkIngestKey,Value> context) throws IOException, InterruptedException {
        log.info("reduce total(" + this.totalKeys + ")");
        super.finish(context);
    }

    @Override
    public void doReduce(BulkIngestKey key, Iterable<Value> values, TaskInputOutputContext<?,?,BulkIngestKey,Value> context)
                    throws IOException, InterruptedException {
        log.info("reduce key(" + key.getKey() + ")");
        this.totalKeys++;
        HyperLogLogPlus hllp = new HyperLogLogPlus(this.normalPrecision, this.sparsePrecision);
        HyperLogFieldSummary stats = new HyperLogFieldSummary(hllp);
        int valueCount = 0;
        for (Value val : values) {
            stats.add(val);
            valueCount++;
            if (0 == (valueCount % this.valueInterval) || this.countsOnly) {
                if (this.countsOnly) {
                    StatsHyperLogSummary addStats = new StatsHyperLogSummary(val);
                    log.info("add values(" + addStats.statsString() + ")");
                }
                log.info("value count(" + valueCount + ")");
            }
        }

        log.info("final stats data(" + stats.toString() + ")");
        if (!this.countsOnly) {
            if (this.minCount <= stats.getCount()) {
                // write to bulk output
                StatsCounters counters = stats.toStatsCounters();
                // set timestamp
                Key k = key.getKey();
                k.setTimestamp(this.timestamp);
                writeBulkIngestKey(key, counters.getValue(), context);
            } else {
                log.debug("count is less than minimum: " + key.getKey().toString() + ") count(" + stats.getCount() + ")");
            }
        }

        context.progress();
    }
}
