package datawave.ingest.mapreduce.partition;

import java.text.ParseException;
import java.util.Date;

import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.log4j.Logger;

import datawave.ingest.mapreduce.handler.shard.ShardIdFactory;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.util.time.DateHelper;

/**
 * The ShardIdPartitioner will generate partitions for any table that's rows are in the format yyyyMMdd_n where n is an integer representing a shard id This
 * will evenly distribute shard ids across the reducers, but it does not take into account its tserver location
 */
public class ShardIdPartitioner extends Partitioner<BulkIngestKey,Value> implements Configurable, DelegatePartitioner {
    private static final Logger log = Logger.getLogger(ShardIdPartitioner.class);

    private static final String PREFIX = ShardIdPartitioner.class.getName();
    private static final String BASE_TIME = PREFIX + ".basetime";

    private Configuration conf;
    private long baseTime = -1;
    private static int MILLIS_PER_DAY = 24 * 60 * 60 * 1000;
    private static int SHARD_ID_SPLIT = 8;

    private ShardIdFactory shardIdFactory = null;

    /**
     * Given the shard id and the number of shards, evenly distribute the shards across the reducers by turning the shard id into a consecutive sequence of
     * numbers
     */
    @Override
    public synchronized int getPartition(BulkIngestKey key, Value value, int numReduceTasks) {
        String shardId = key.getKey().getRow().toString();
        try {
            long shardIndex = generateNumberForShardId(shardId, getBaseTime());
            return (int) (shardIndex % numReduceTasks);

        } catch (Exception e) {
            return (shardId.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        long tomorrow = (new Date().getTime() / MILLIS_PER_DAY) + 1;
        conf.setLong(BASE_TIME, tomorrow);
        shardIdFactory = new ShardIdFactory(conf);
    }

    /**
     * The index is equal to the shard (the number after the _) plus an offset which shifts one day after the other Turn a shard ID in to a number which is
     * sequential for all shard ids
     *
     * @param shardId
     *            the shard id
     * @param baseTime
     *            the base timestamp
     * @throws ParseException
     *             for issues parsing the time
     * @return the shard id number
     */
    private long generateNumberForShardId(String shardId, long baseTime) throws ParseException {
        if (shardId.charAt(SHARD_ID_SPLIT) != '_') {
            throw new ParseException("Shard id is not in expected format: yyyyMMdd_n: " + shardId, SHARD_ID_SPLIT);
        }

        // turn the yyyyMMdd into the number of days until base time
        Date date = DateHelper.parse(shardId.substring(0, SHARD_ID_SPLIT));
        long daysFromBaseTime = (baseTime - (date.getTime() / MILLIS_PER_DAY));
        if (daysFromBaseTime < 0) {
            daysFromBaseTime = 0 - daysFromBaseTime;
        }

        // get the shard number
        int shard = Integer.parseInt(shardId.substring(SHARD_ID_SPLIT + 1));

        // now turn the shard id into a number that is sequential (without gaps) with all other shard ids

        return (daysFromBaseTime * shardIdFactory.getNumShards(date.getTime())) + shard;
    }

    private long getBaseTime() throws IllegalArgumentException {
        if (baseTime < 0) {
            baseTime = conf.getLong(BASE_TIME, 0);
            if (baseTime == 0) {
                throw new IllegalArgumentException("Forgot to configure the ShardIdPartitioner");
            }
        }
        return baseTime;
    }

    @Override
    public void configureWithPrefix(String prefix) {/* noop */}

    @Override
    public int getNumPartitions() {
        return Integer.MAX_VALUE;
    }

    @Override
    public void initializeJob(Job job) {}

    @Override
    public boolean needSplits() {
        return false;
    }

    @Override
    public boolean needSplitLocations() {
        return false;
    }
}
