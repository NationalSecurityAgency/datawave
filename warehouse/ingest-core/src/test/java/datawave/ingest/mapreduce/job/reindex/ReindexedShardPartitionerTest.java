package datawave.ingest.mapreduce.job.reindex;

import static datawave.ingest.mapreduce.job.reindex.ReindexedShardPartitioner.MAX_PARTITIONS;
import static datawave.ingest.mapreduce.job.reindex.ReindexedShardPartitioner.MIN_KEYS;
import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;

import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.DelegatingPartitioner;
import datawave.ingest.mapreduce.job.DelegatingPartitionerTest;

public class ReindexedShardPartitionerTest {

    @Test
    public void roundRobinDefaultTest() {
        Configuration config = new Configuration();
        ReindexedShardPartitioner partitioner = new ReindexedShardPartitioner();
        partitioner.setConf(config);
        partitioner.configureWithPrefix("nonexistent");

        BulkIngestKey bulkIngestKey = new BulkIngestKey(new Text("nonexistent"), new Key());

        int partition = partitioner.getPartition(bulkIngestKey, new Value(), 10);
        assertEquals((partition + 1) % 10, partitioner.getPartition(bulkIngestKey, new Value(), 10));
        assertEquals((partition + 2) % 10, partitioner.getPartition(bulkIngestKey, new Value(), 10));
        assertEquals((partition + 3) % 10, partitioner.getPartition(bulkIngestKey, new Value(), 10));
        assertEquals((partition + 4) % 10, partitioner.getPartition(bulkIngestKey, new Value(), 10));
        assertEquals((partition + 5) % 10, partitioner.getPartition(bulkIngestKey, new Value(), 10));
        assertEquals((partition + 6) % 10, partitioner.getPartition(bulkIngestKey, new Value(), 10));
        assertEquals(partition, partitioner.getPartition(bulkIngestKey, new Value(), 10));
        assertEquals((partition + 1) % 10, partitioner.getPartition(bulkIngestKey, new Value(), 10));
        assertEquals((partition + 2) % 10, partitioner.getPartition(bulkIngestKey, new Value(), 10));
        assertEquals((partition + 3) % 10, partitioner.getPartition(bulkIngestKey, new Value(), 10));
        assertEquals((partition + 4) % 10, partitioner.getPartition(bulkIngestKey, new Value(), 10));
        assertEquals((partition + 5) % 10, partitioner.getPartition(bulkIngestKey, new Value(), 10));
        assertEquals((partition + 6) % 10, partitioner.getPartition(bulkIngestKey, new Value(), 10));
        assertEquals(partition, partitioner.getPartition(bulkIngestKey, new Value(), 10));
    }

    @Test
    public void roundRobinConstrainedTest() {
        Configuration config = new Configuration();
        ReindexedShardPartitioner partitioner = new ReindexedShardPartitioner();
        partitioner.setConf(config);
        partitioner.configureWithPrefix("nonexistent");

        BulkIngestKey bulkIngestKey = new BulkIngestKey(new Text("nonexistent"), new Key());

        int partition = partitioner.getPartition(bulkIngestKey, new Value(), 3);
        assertEquals((partition + 1) % 3, partitioner.getPartition(bulkIngestKey, new Value(), 3));
        assertEquals((partition + 2) % 3, partitioner.getPartition(bulkIngestKey, new Value(), 3));
        assertEquals(partition, partitioner.getPartition(bulkIngestKey, new Value(), 3));
        assertEquals((partition + 1) % 3, partitioner.getPartition(bulkIngestKey, new Value(), 3));
        assertEquals((partition + 2) % 3, partitioner.getPartition(bulkIngestKey, new Value(), 3));
        assertEquals(partition, partitioner.getPartition(bulkIngestKey, new Value(), 3));
    }

    @Test
    public void minKeysOverrideTest() {
        Configuration config = new Configuration();
        config.setInt("nonexistent." + ReindexedShardPartitioner.MIN_KEYS, 5);
        ReindexedShardPartitioner partitioner = new ReindexedShardPartitioner();
        partitioner.setConf(config);
        partitioner.configureWithPrefix("nonexistent");

        BulkIngestKey bulkIngestKey = new BulkIngestKey(new Text("nonexistent"), new Key());

        int partition = partitioner.getPartition(bulkIngestKey, new Value(), 3);
        assertEquals(partition, partitioner.getPartition(bulkIngestKey, new Value(), 3));
        assertEquals(partition, partitioner.getPartition(bulkIngestKey, new Value(), 3));
        assertEquals(partition, partitioner.getPartition(bulkIngestKey, new Value(), 3));
        assertEquals(partition, partitioner.getPartition(bulkIngestKey, new Value(), 3));
        assertEquals((partition + 1) % 3, partitioner.getPartition(bulkIngestKey, new Value(), 3));
        assertEquals((partition + 1) % 3, partitioner.getPartition(bulkIngestKey, new Value(), 3));
        assertEquals((partition + 1) % 3, partitioner.getPartition(bulkIngestKey, new Value(), 3));
        assertEquals((partition + 1) % 3, partitioner.getPartition(bulkIngestKey, new Value(), 3));
        assertEquals((partition + 1) % 3, partitioner.getPartition(bulkIngestKey, new Value(), 3));
        assertEquals((partition + 2) % 3, partitioner.getPartition(bulkIngestKey, new Value(), 3));
        assertEquals((partition + 2) % 3, partitioner.getPartition(bulkIngestKey, new Value(), 3));
        assertEquals((partition + 2) % 3, partitioner.getPartition(bulkIngestKey, new Value(), 3));
        assertEquals((partition + 2) % 3, partitioner.getPartition(bulkIngestKey, new Value(), 3));
        assertEquals((partition + 2) % 3, partitioner.getPartition(bulkIngestKey, new Value(), 3));
        assertEquals(partition, partitioner.getPartition(bulkIngestKey, new Value(), 3));
        assertEquals((partition + 1) % 3, partitioner.getPartition(bulkIngestKey, new Value(), 3));
        assertEquals((partition + 2) % 3, partitioner.getPartition(bulkIngestKey, new Value(), 3));
        assertEquals(partition, partitioner.getPartition(bulkIngestKey, new Value(), 3));
    }

    @Test
    public void maxPartitionsOverrideTest() {
        Configuration config = new Configuration();
        config.setInt("nonexistent." + ReindexedShardPartitioner.MAX_PARTITIONS, 3);

        ReindexedShardPartitioner partitioner = new ReindexedShardPartitioner();
        partitioner.setConf(config);
        partitioner.configureWithPrefix("nonexistent");

        BulkIngestKey bulkIngestKey = new BulkIngestKey(new Text("nonexistent"), new Key());

        int partition = partitioner.getPartition(bulkIngestKey, new Value(), 10);
        assertEquals((partition + 1) % 10, partitioner.getPartition(bulkIngestKey, new Value(), 10));
        assertEquals((partition + 2) % 10, partitioner.getPartition(bulkIngestKey, new Value(), 10));
        assertEquals(partition, partitioner.getPartition(bulkIngestKey, new Value(), 10));
        assertEquals((partition + 1) % 10, partitioner.getPartition(bulkIngestKey, new Value(), 10));
        assertEquals((partition + 2) % 10, partitioner.getPartition(bulkIngestKey, new Value(), 10));
        assertEquals(partition, partitioner.getPartition(bulkIngestKey, new Value(), 10));
    }

    @Test
    public void configWithDelegatingPartitionerTest() throws IOException {
        Configuration config = new Configuration();
        DelegatingPartitioner partitioner = new DelegatingPartitioner();
        config.set("partitioner.category.member.shard", "shardedTables");
        config.set("partitioner.category.shardedTables", ReindexedShardPartitioner.class.getCanonicalName());
        config.setInt("shardedTables." + MIN_KEYS, 5);
        config.setInt("shardedTables." + MAX_PARTITIONS, 3);
        // override the default for verification
        config.set("partitioner.default.delegate", DelegatingPartitionerTest.AlwaysReturnThree.class.getName());

        Job j = Job.getInstance();
        DelegatingPartitioner.configurePartitioner(j, config, new String[] {"shard"});
        partitioner.setConf(config);

        // verify the overrides are working with the table only
        BulkIngestKey bulkIngestKey = new BulkIngestKey(new Text("shard"), new Key());

        int partition = partitioner.getPartition(bulkIngestKey, new Value(), 10);
        assertEquals(partition, partitioner.getPartition(bulkIngestKey, new Value(), 10));
        assertEquals(partition, partitioner.getPartition(bulkIngestKey, new Value(), 10));
        assertEquals(partition, partitioner.getPartition(bulkIngestKey, new Value(), 10));
        assertEquals(partition, partitioner.getPartition(bulkIngestKey, new Value(), 10));
        assertEquals((partition + 1) % 10, partitioner.getPartition(bulkIngestKey, new Value(), 10));
        assertEquals((partition + 1) % 10, partitioner.getPartition(bulkIngestKey, new Value(), 10));
        assertEquals((partition + 1) % 10, partitioner.getPartition(bulkIngestKey, new Value(), 10));
        assertEquals((partition + 1) % 10, partitioner.getPartition(bulkIngestKey, new Value(), 10));
        assertEquals((partition + 1) % 10, partitioner.getPartition(bulkIngestKey, new Value(), 10));
        assertEquals((partition + 2) % 10, partitioner.getPartition(bulkIngestKey, new Value(), 10));
        assertEquals((partition + 2) % 10, partitioner.getPartition(bulkIngestKey, new Value(), 10));
        assertEquals((partition + 2) % 10, partitioner.getPartition(bulkIngestKey, new Value(), 10));
        assertEquals((partition + 2) % 10, partitioner.getPartition(bulkIngestKey, new Value(), 10));
        assertEquals((partition + 2) % 10, partitioner.getPartition(bulkIngestKey, new Value(), 10));
        assertEquals(partition, partitioner.getPartition(bulkIngestKey, new Value(), 10));
        assertEquals((partition + 1) % 10, partitioner.getPartition(bulkIngestKey, new Value(), 10));
        assertEquals((partition + 2) % 10, partitioner.getPartition(bulkIngestKey, new Value(), 10));
        assertEquals(partition, partitioner.getPartition(bulkIngestKey, new Value(), 10));

        // create another key and it should be handled by some other partitioner
        bulkIngestKey = new BulkIngestKey(new Text("shard2"), new Key());
        assertEquals(3, partitioner.getPartition(bulkIngestKey, new Value(), 10));
        assertEquals(3, partitioner.getPartition(bulkIngestKey, new Value(), 10));

    }
}
