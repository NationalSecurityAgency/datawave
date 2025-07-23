package datawave.ingest.mapreduce.handler.shard;

import datawave.ingest.data.RawRecordContainer;

/**
 * This interface defines behavior for overriding the default behavior for generating a shard id for records in {@link ShardIdFactory}.
 */
public interface ShardIdGenerator {

    /**
     * Return whether this {@link ShardIdGenerator} applies to the given record and should be used to generate a shard id for the record.
     *
     * @param record
     *            the event record
     * @return true if this generator should be used to get a shard id for the record, or false otherwise
     */
    boolean isApplicable(RawRecordContainer record);

    /**
     * Return a shard id for the given record.
     *
     * @param record
     *            the record
     * @param baseShardId
     *            the base shard id
     * @param numShards
     *            the number of shards
     * @return the shard id
     */
    String getShardId(RawRecordContainer record, String baseShardId, int numShards);
}
