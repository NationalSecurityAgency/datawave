package datawave.ingest.mapreduce.handler.shard;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;

import datawave.ingest.data.RawRecordContainer;
import datawave.util.time.DateHelper;

public class ShardIdFactory {

    public static final String NUM_SHARDS = "num.shards";
    private NumShards numShards = null;

    public ShardIdFactory(Configuration conf) {
        this.numShards = new NumShards(conf);
    }

    /**
     * Calculates the shard id of the event
     *
     * @param record
     *            the record container
     * @return Shard id
     */
    public byte[] getShardIdBytes(RawRecordContainer record) {
        return getShardId(record).getBytes();
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
     * Calculates the shard id of the event
     *
     * @param record
     *            the event record
     * @return Shard id
     */
    public String getShardId(RawRecordContainer record) {
        StringBuilder buf = new StringBuilder();
        buf.append(DateHelper.format(record.getDate()));
        buf.append("_");
        int partition = (Integer.MAX_VALUE & record.getId().getShardedPortion().hashCode()) % getNumShards(record.getDate());
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
