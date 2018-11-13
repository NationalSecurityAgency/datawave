package datawave.ingest.mapreduce.partition;

import datawave.ingest.mapreduce.job.BulkIngestKey;
import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class LimitedRowPartitionerTest {
    
    private String prefix = "testing";
    private Text table = new Text("table");
    private int numPartitions = 100;
    
    private LimitedRowPartitioner partitioner;
    
    @Before
    public void setup() {
        partitioner = new LimitedRowPartitioner();
        
        Configuration conf = new Configuration();
        conf.setInt("testing." + PartitionLimiter.MAX_PARTITIONS_PROPERTY, numPartitions);
        
        partitioner.setConf(conf);
        partitioner.configureWithPrefix(prefix);
    }
    
    @Test
    public void shouldRouteKeysWithSameRowToSamePartition() {
        BulkIngestKey key1 = new BulkIngestKey(table, new Key("row1", "fam1", "qual1"));
        BulkIngestKey key2 = new BulkIngestKey(table, new Key("row1", "fam2", "qual2"));
        
        assertEquals(partitioner.getPartition(key1, null, numPartitions), partitioner.getPartition(key2, null, numPartitions));
    }
    
    @Test
    public void shouldRouteKeysWithDiffRowToDiffPartition() {
        BulkIngestKey key1 = new BulkIngestKey(table, new Key("row1", "fam1", "qual1"));
        BulkIngestKey key2 = new BulkIngestKey(table, new Key("row2", "fam2", "qual2"));
        
        assertNotEquals(partitioner.getPartition(key1, null, numPartitions), partitioner.getPartition(key2, null, numPartitions));
    }
}
