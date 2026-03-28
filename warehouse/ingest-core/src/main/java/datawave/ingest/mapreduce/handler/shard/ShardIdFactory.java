package datawave.ingest.mapreduce.handler.shard;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Multimap;

import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.ConfigurationHelper;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.util.time.DateHelper;

public class ShardIdFactory {

    public static final String NUM_SHARDS = "num.shards";
    public static final String SHARD_ID_GENERATOR = "shardIdFactory.generator";

    private NumShards numShards = null;
    private final List<ShardIdGenerator> generators;

    public ShardIdFactory(Configuration conf) {
        this.numShards = new NumShards(conf);
        this.generators = ConfigurationHelper.getIndexedInstances(conf, SHARD_ID_GENERATOR, ShardIdGenerator.class, 1);
    }

    /**
     * Calculates the shard id of the event .getInstances()
     *
     * @param record
     *            the record container
     * @return Shard id
     */
    public byte[] getShardIdBytes(RawRecordContainer record, Multimap<String,NormalizedContentInterface> eventFields) {
        return getShardId(record, eventFields).getBytes();
    }

    /**
     * this method will return numShards based on date in epoch millis
     *
     * @param date
     *            the date in epoch
     * @return the number of shards
     */
    public int getNumShards(long date) {
        return numShards.getNumShards(date);
    }

    /**
     * this method will return numShards based on date string 'yyyyMMdd'
     *
     * @param date
     *            the date in string format
     * @return the number of shards
     */
    public int getNumShards(String date) {
        return numShards.getNumShards(date);
    }

    /**
     * Calculates the shard id of the event. If this {@link ShardIdFactory} has been configured with a {@link ShardIdGenerator} that applies to the record, the
     * shard id provided by the first applicable generator will be returned. Otherwise, the shard id provided by {@link #getBaseShardId(RawRecordContainer)}
     * will be returned.
     *
     * @param record
     *            the event record
     * @return the shard id
     */
    public String getShardId(RawRecordContainer record, Multimap<String,NormalizedContentInterface> eventFields) {
        String shardId = record.getShardId();

        if (shardId == null) {
            shardId = getBaseShardId(record);
            for (ShardIdGenerator generator : generators) {
                if (generator.isApplicable(record, eventFields)) {
                    int numShards = getNumShards(record.getDate());
                    shardId = generator.getShardId(record, eventFields, shardId, numShards);
                    break;
                }
            }
        }

        return shardId;
    }

    /**
     * Calculates the shard id of the event.
     *
     * @param record
     *            the event record
     * @return the shard id
     */
    public String getBaseShardId(RawRecordContainer record) {
        return getBaseShardId(record, getNumShards(record.getDate()));
    }

    /**
     * A base shard id calculator that can be used externally to this class.
     *
     * @param record
     *            the event record
     * @param numShards
     *            the number of shards
     * @return the shard id
     */
    public static String getBaseShardId(RawRecordContainer record, int numShards) {
        StringBuilder buf = new StringBuilder();
        buf.append(DateHelper.format(record.getDate()));
        buf.append("_");
        int partition = (Integer.MAX_VALUE & record.getId().getShardedPortion().hashCode()) % numShards;
        buf.append(partition);
        return buf.toString();
    }

    /**
     * Get the date portion of the shard id
     *
     * @param shardId
     *            the shard id
     * @return the date yyyyMMdd
     */
    public static String getDateString(String shardId) {
        return shardId.substring(0, shardId.indexOf('_'));
    }

    /**
     * Get the date portion of the shard id
     *
     * @param shardId
     *            the shard id
     * @return the date
     * @throws ParseException
     *             if there is an issue parsing
     */
    public static Date getDate(String shardId) throws ParseException {
        return new SimpleDateFormat("yyyyMMdd").parse(getDateString(shardId));
    }

    public static int getShard(String shardId) {
        return Integer.parseInt(shardId.substring(shardId.indexOf('_') + 1));
    }

    public boolean isMultipleNumShardsConfigured() {
        return numShards.getShardCount() > 1;
    }
}
