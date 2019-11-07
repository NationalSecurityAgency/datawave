package datawave.ingest.mapreduce.partition;

import datawave.ingest.mapreduce.job.BulkIngestKey;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

import org.apache.accumulo.core.data.Key;

public class RowHashingPartitionerTest {
    
    /*
     * Asserts that keys in the same shard (share the same row) will be going to the same reducer.
     */
    @Test
    public void testShardPartitioning() {
        RowHashingPartitioner sdp = new RowHashingPartitioner();
        final int nRed = 30;
        Text tbl = new Text("table");
        for (int i = 0; i < 10; ++i) {
            String shard = "20110101_" + i;
            int expectedReducer = sdp.getPartition(new BulkIngestKey(tbl, new Key(shard, "fluff", "fluff")), null, nRed);
            for (int j = 0; j < 10; ++j) {
                Assert.assertEquals(expectedReducer, sdp.getPartition(new BulkIngestKey(tbl, new Key(shard, Integer.toHexString(j), "u")), null, nRed));
            }
        }
    }
    
    /*
     * Asserts that keys in the same shard with the same column family will be going to the same reducer.
     */
    @Test
    public void testShardAndCFPartitioning() {
        RowHashingPartitioner sdp = new RowHashingPartitioner();
        Configuration conf = new Configuration();
        conf.set(RowHashingPartitioner.COLUMN_FAMILIES, "tf");
        sdp.setConf(conf);
        assertColumnFamilyPartitioning(sdp);
    }
    
    private void assertColumnFamilyPartitioning(RowHashingPartitioner sdp) {
        final int nRed = 31;
        Text tbl = new Text("table");
        for (int i = 0; i < 10; ++i) {
            String shard = "20110101_" + i;
            int expectedReducer = sdp.getPartition(new BulkIngestKey(tbl, new Key(shard, "tf", "fluff")), null, nRed);
            for (int j = 0; j < 2; ++j) {
                Assert.assertEquals(expectedReducer, sdp.getPartition(new BulkIngestKey(tbl, new Key(shard, "tf", Integer.toHexString(j))), null, nRed));
            }
        }
    }
    
    @Test
    public void testConfigureByTable() throws IllegalAccessException, InstantiationException {
        Configuration conf = new Configuration();
        String tableName = "tableX";
        conf.setInt(tableName + "." + RowHashingPartitioner.COLUMN_FAMILIES, 6);
        
        RowHashingPartitioner partitioner = RowHashingPartitioner.class.newInstance();
        partitioner.setConf(conf);
        partitioner.configureWithPrefix(tableName);
        
        assertColumnFamilyPartitioning(partitioner);
    }
    
    @Test
    public void testMultipleColFamiliesPerRow() throws IllegalAccessException, InstantiationException {
        Configuration conf = new Configuration();
        String tableName = "tableX";
        conf.set(tableName + "." + RowHashingPartitioner.COLUMN_FAMILIES, "CITY,STATE");
        
        RowHashingPartitioner partitioner = RowHashingPartitioner.class.newInstance();
        partitioner.setConf(conf);
        partitioner.configureWithPrefix(tableName);
        
        final int nRed = 31;
        Text tbl = new Text("table");
        
        for (int i = 0; i < 10; ++i) {
            String shard = "20110101_" + i;
            int expectedReducerCity = partitioner.getPartition(new BulkIngestKey(tbl, new Key(shard, "CITY", "fluff")), null, nRed);
            int expectedReducerState = partitioner.getPartition(new BulkIngestKey(tbl, new Key(shard, "STATE", "fluff")), null, nRed);
            for (int j = 0; j < 2; ++j) {
                Assert.assertEquals(" failed on row " + shard + " column family CITY", expectedReducerCity,
                                partitioner.getPartition(new BulkIngestKey(tbl, new Key(shard, "CITY", Integer.toHexString(j))), null, nRed));
                Assert.assertEquals(" failed on row " + shard + " column family STATE", expectedReducerState,
                                partitioner.getPartition(new BulkIngestKey(tbl, new Key(shard, "STATE", Integer.toHexString(j))), null, nRed));
            }
        }
    }
    
}
