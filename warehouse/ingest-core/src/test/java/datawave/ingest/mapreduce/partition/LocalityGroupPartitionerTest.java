package datawave.ingest.mapreduce.partition;

import datawave.ingest.mapreduce.job.BulkIngestKey;
import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

public class LocalityGroupPartitionerTest {
    
    public static final int NUM_REDUCERS = 31;
    
    @Test
    public void testGetPartition() throws Exception {
        Configuration conf = new Configuration();
        String tableName = "tableX";
        conf.set(tableName + "." + LocalityGroupPartitioner.COLUMN_FAMILIES, "CITY,STATE");
        
        LocalityGroupPartitioner partitioner = LocalityGroupPartitioner.class.newInstance();
        partitioner.setConf(conf);
        partitioner.configureWithPrefix(tableName);
        
        Text tbl = new Text("table");
        int expectedReducerCity = 0;
        int expectedReducerState = 1;
        for (int i = 0; i < 10; ++i) {
            String shard = "" + i;
            for (int j = 0; j < 2; ++j) {
                Assert.assertEquals(" failed on row " + shard + " column family CITY", expectedReducerCity,
                                partitioner.getPartition(new BulkIngestKey(tbl, new Key(shard, "CITY", Integer.toHexString(j))), null, NUM_REDUCERS));
                Assert.assertEquals(" failed on row " + shard + " column family STATE", expectedReducerState,
                                partitioner.getPartition(new BulkIngestKey(tbl, new Key(shard, "STATE", Integer.toHexString(j))), null, NUM_REDUCERS));
            }
        }
    }
    
    @Test
    public void testGetPartitionNewColFam() throws Exception {
        Configuration conf = new Configuration();
        String tableName = "tableX";
        conf.set(tableName + "." + LocalityGroupPartitioner.COLUMN_FAMILIES, "CITY,STATE");
        
        LocalityGroupPartitioner partitioner = LocalityGroupPartitioner.class.newInstance();
        partitioner.setConf(conf);
        partitioner.configureWithPrefix(tableName);
        
        Text tbl = new Text("table");
        int expectedReducerCity = 0;
        int expectedReducerState = 1;
        int expectedReducerJam = 2;
        for (int i = 0; i < 10; ++i) {
            String shard = "" + i;
            for (int j = 0; j < 2; ++j) {
                Assert.assertEquals(" failed on row " + shard + " column family CITY", expectedReducerCity,
                                partitioner.getPartition(new BulkIngestKey(tbl, new Key(shard, "CITY", Integer.toHexString(j))), null, NUM_REDUCERS));
                Assert.assertEquals(" failed on row " + shard + " column family STATE", expectedReducerState,
                                partitioner.getPartition(new BulkIngestKey(tbl, new Key(shard, "STATE", Integer.toHexString(j))), null, NUM_REDUCERS));
                Assert.assertEquals(" failed on row " + shard + " column family JAM", expectedReducerJam,
                                partitioner.getPartition(new BulkIngestKey(tbl, new Key(shard, "JAM", Integer.toHexString(j))), null, NUM_REDUCERS));
            }
        }
    }
}
