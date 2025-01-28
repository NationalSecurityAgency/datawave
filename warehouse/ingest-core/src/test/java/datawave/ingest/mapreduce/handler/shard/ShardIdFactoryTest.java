package datawave.ingest.mapreduce.handler.shard;

import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.data.hash.HashUID;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.RawRecordContainerImplTest;
import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.ingest.CSVIngestHelper;
import datawave.util.time.DateHelper;

class ShardIdFactoryTest {

    private Configuration conf;

    @BeforeEach
    void setUp() {
        conf = new Configuration();
        conf.set("num.shards", "10");

        Type csva = new Type("csva", CSVIngestHelper.class, null, null, 0, null);

        TypeRegistry.reset();
        TypeRegistry registry = TypeRegistry.getInstance(conf);
        registry.put(csva.typeName(), csva);
    }

    /**
     * Verify that when no {@link ShardIdGenerator} instances have been configured for the factory, that {@link ShardIdFactory#getShardId(RawRecordContainer)}
     * returns the base shard id.
     */
    @Test
    void testGetShardIdWithNoGenerators() {
        RawRecordContainer event = createEvent("1.2.3", DateHelper.parse("20240115"), "csva");
        String shardId = new ShardIdFactory(conf).getShardId(event);
        Assertions.assertEquals("20240115_2", shardId);
    }

    /**
     * Verify that when the factory has shard id generators configured for it, but none of the generators are applicable to the record, that the base shard id
     * is returned.
     */
    @Test
    void testGetShardIdWithNoApplicableGenerators() {
        conf.set(ShardIdFactory.SHARD_ID_GENERATOR + ".1", ConstantPartition.class.getName());
        conf.set(ShardIdFactory.SHARD_ID_GENERATOR + ".1.typeName", "wiki");
        conf.set(ShardIdFactory.SHARD_ID_GENERATOR + ".1.partition", "20");

        conf.set(ShardIdFactory.SHARD_ID_GENERATOR + ".2", ConstantPartition.class.getName());
        conf.set(ShardIdFactory.SHARD_ID_GENERATOR + ".2.typeName", "text");
        conf.set(ShardIdFactory.SHARD_ID_GENERATOR + ".2.partition", "10");

        RawRecordContainer event = createEvent("1.2.3", DateHelper.parse("20240115"), "csva");
        String shardId = new ShardIdFactory(conf).getShardId(event);
        Assertions.assertEquals("20240115_2", shardId);
    }

    /**
     * Verify that when the factory has shard id generators configured for it, and one of the generators are applicable to the record, that the shard id
     * provided by the applicable generator is returned.
     */
    @Test
    void testGetShardIdWithSingleApplicableGenerators() {
        conf.set(ShardIdFactory.SHARD_ID_GENERATOR + ".1", ConstantPartition.class.getName());
        conf.set(ShardIdFactory.SHARD_ID_GENERATOR + ".1.typeName", "wiki");
        conf.set(ShardIdFactory.SHARD_ID_GENERATOR + ".1.partition", "20");

        conf.set(ShardIdFactory.SHARD_ID_GENERATOR + ".2", ConstantPartition.class.getName());
        conf.set(ShardIdFactory.SHARD_ID_GENERATOR + ".2.typeName", "csva");
        conf.set(ShardIdFactory.SHARD_ID_GENERATOR + ".2.partition", "10");

        RawRecordContainer event = createEvent("1.2.3", DateHelper.parse("20240115"), "csva");
        String shardId = new ShardIdFactory(conf).getShardId(event);
        Assertions.assertEquals("20240115_10", shardId);
    }

    /**
     * Verify that when the factory has shard id generators configured for it, and multiple generators are applicable to the record, that the shard id provided
     * by the first applicable generator is returned.
     */
    @Test
    void testGetShardIdWithMultipleApplicableGenerators() {
        conf.set(ShardIdFactory.SHARD_ID_GENERATOR + ".1", ConstantPartition.class.getName());
        conf.set(ShardIdFactory.SHARD_ID_GENERATOR + ".1.typeName", "csva");
        conf.set(ShardIdFactory.SHARD_ID_GENERATOR + ".1.partition", "20");

        conf.set(ShardIdFactory.SHARD_ID_GENERATOR + ".2", ConstantPartition.class.getName());
        conf.set(ShardIdFactory.SHARD_ID_GENERATOR + ".2.typeName", "csva");
        conf.set(ShardIdFactory.SHARD_ID_GENERATOR + ".2.partition", "10");

        RawRecordContainer event = createEvent("1.2.3", DateHelper.parse("20240115"), "csva");
        String shardId = new ShardIdFactory(conf).getShardId(event);
        Assertions.assertEquals("20240115_20", shardId);
    }

    private RawRecordContainer createEvent(String uid, Date date, String dataType) {
        RawRecordContainerImplTest.ValidatingRawRecordContainerImpl event = new RawRecordContainerImplTest.ValidatingRawRecordContainerImpl();
        event.setId(HashUID.parse(uid));
        event.setTimestamp(date.getTime());
        event.setDataType(TypeRegistry.getType(dataType));
        return event;
    }

    public static class ConstantPartition implements ShardIdGenerator {

        private final String typeName;
        private final int partition;

        public ConstantPartition(Configuration conf, String property) {
            this.typeName = conf.getTrimmed(property + ".typeName");
            this.partition = conf.getInt(property + ".partition", 0);
        }

        @Override
        public boolean isApplicable(RawRecordContainer record) {
            return record.getDataType().typeName().equals(typeName);
        }

        @Override
        public String getShardId(RawRecordContainer record, String baseShardId, int numShards) {
            return DateHelper.format(record.getDate()) + "_" + partition;
        }
    }
}
