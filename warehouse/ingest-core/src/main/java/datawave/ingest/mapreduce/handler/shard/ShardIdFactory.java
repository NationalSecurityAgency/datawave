package datawave.ingest.mapreduce.handler.shard;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.ConfigurationHelper;
import datawave.util.time.DateHelper;

import org.apache.hadoop.conf.Configuration;

public class ShardIdFactory {
    
    public static final String NUM_SHARDS = "num.shards";
    private int numShards = 0;
    
    public ShardIdFactory(Configuration conf) {
        numShards = getNumShards(conf);
    }
    
    public static int getNumShards(Configuration conf) {
        return ConfigurationHelper.isNull(conf, NUM_SHARDS, Integer.class);
    }
    
    /**
     * Calculates the shard id of the event
     * 
     * @param record
     * @return Shard id
     */
    public byte[] getShardIdBytes(RawRecordContainer record) {
        return getShardId(record).getBytes();
    }
    
    /**
     * Calculates the shard id of the event
     * 
     * @param record
     * @return Shard id
     */
    public String getShardId(RawRecordContainer record) {
        return getShardId(record, numShards);
    }
    
    /**
     * Calculates the shard id of the event
     * 
     * @param record
     * @return Shard id
     */
    public static String getShardId(RawRecordContainer record, int numShards) {
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
     * @return the date yyyyMMdd
     */
    public static String getDateString(String shardId) {
        return shardId.substring(0, shardId.indexOf('_'));
    }
    
    /**
     * Get the date portion of the shard id
     * 
     * @param shardId
     * @return the date
     */
    public static Date getDate(String shardId) throws ParseException {
        return new SimpleDateFormat("yyyyMMdd").parse(getDateString(shardId));
    }
    
    public static int getShard(String shardId) {
        return Integer.parseInt(shardId.substring(shardId.indexOf('_') + 1));
    }
    
}
