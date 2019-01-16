package datawave.ingest.mapreduce.partition;

import datawave.ingest.mapreduce.job.BulkIngestKey;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

public class LimitedKeyPartitionerTest {
    private static final int NUM_REDUCERS = 1000;
    
    @Test
    public void testLimitedRangeSetViaGenericConfig() throws IllegalAccessException, InstantiationException {
        Configuration conf = new Configuration();
        conf.setInt(PartitionLimiter.MAX_PARTITIONS_PROPERTY, 8);
        
        LimitedKeyPartitioner partitioner = LimitedKeyPartitioner.class.newInstance();
        partitioner.setConf(conf);
        
        assertPartitionsUnderMax(partitioner, 8);
    }
    
    @Test
    public void testLimitedRangeSetViaTableSpecificConfig() throws IllegalAccessException, InstantiationException {
        Configuration conf = new Configuration();
        String tableName = "tableX";
        conf.setInt(tableName + "." + PartitionLimiter.MAX_PARTITIONS_PROPERTY, 6);
        
        LimitedKeyPartitioner partitioner = LimitedKeyPartitioner.class.newInstance();
        partitioner.setConf(conf);
        partitioner.configureWithPrefix(tableName);
        
        assertPartitionsUnderMax(partitioner, 6);
    }
    
    private void assertPartitionsUnderMax(LimitedKeyPartitioner partitioner, int expectedMax) {
        for (int i = 0; i < 1000; i++) {
            Assert.assertTrue(partitioner.getPartition(createKeyFor(Integer.toString(i)), new Value(), NUM_REDUCERS) >= 0);
            Assert.assertTrue(partitioner.getPartition(createKeyFor(Integer.toString(i)), new Value(), NUM_REDUCERS) < expectedMax);
        }
    }
    
    @Test
    public void testHigherMaxThanReducers() throws IllegalAccessException, InstantiationException {
        Configuration conf = new Configuration();
        conf.setInt(PartitionLimiter.MAX_PARTITIONS_PROPERTY, NUM_REDUCERS + 1);
        
        LimitedKeyPartitioner partitioner = LimitedKeyPartitioner.class.newInstance();
        partitioner.setConf(conf);
        assertPartitionsUnderMax(partitioner, NUM_REDUCERS);
    }
    
    @Test(expected = Exception.class)
    public void testNoMaxPartitions() {
        LimitedKeyPartitioner partitioner = new LimitedKeyPartitioner();
        partitioner.getPartition(createKeyFor(""), new Value(), NUM_REDUCERS);
    }
    
    private BulkIngestKey createKeyFor(String row) {
        return new BulkIngestKey(new Text("some table name"), new Key(row));
    }
}
