package datawave.ingest.mapreduce.job;

import datawave.ingest.mapreduce.partition.LimitedKeyPartitioner;
import datawave.ingest.mapreduce.partition.PartitionLimiter;
import datawave.ingest.mapreduce.partition.RowHashingPartitioner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class TablePartitionerOffsetsTest {
    private static final Integer NUM_REDUCERS = 54;
    private Configuration conf;
    
    @Before
    public void before() {
        conf = new Configuration();
        conf.setInt("splits.num.reduce", NUM_REDUCERS);
    }
    
    @Test
    public void testNoConfigurationIsAOk() throws ClassNotFoundException {
        List<Text> tableNames = Arrays.asList();
        PartitionerCache partitionerCache = new PartitionerCache(conf);
        TablePartitionerOffsets offsets = new TablePartitionerOffsets(conf, tableNames, partitionerCache);
        assertOffset(offsets, "table1", null);
    }
    
    private void assertOffset(TablePartitionerOffsets offsets, String tableName, Integer i) {
        Integer expected = (i == null ? null : (Integer) (NUM_REDUCERS - 1 - i));
        Assert.assertEquals(expected, offsets.get(new Text(tableName)));
    }
    
    @Test
    public void testNoOffsets() throws ClassNotFoundException {
        conf.set(DelegatingPartitioner.DEFAULT_DELEGATE_PARTITIONER, RowHashingPartitioner.class.getName());
        List<Text> tableNames = Arrays.asList();
        PartitionerCache partitionerCache = new PartitionerCache(conf);
        TablePartitionerOffsets offsets = new TablePartitionerOffsets(conf, tableNames, partitionerCache);
        assertOffset(offsets, "table1", null);
    }
    
    @Test
    public void testTableOnly() throws ClassNotFoundException {
        conf.set(DelegatingPartitioner.DEFAULT_DELEGATE_PARTITIONER, RowHashingPartitioner.class.getName());
        conf.set(DelegatingPartitioner.PREFIX_DEDICATED_PARTITIONER + "table1", LimitedKeyPartitioner.class.getName());
        conf.setInt("table1." + PartitionLimiter.MAX_PARTITIONS_PROPERTY, 5);
        conf.set(DelegatingPartitioner.PREFIX_DEDICATED_PARTITIONER + "table2", LimitedKeyPartitioner.class.getName());
        conf.setInt("table2." + PartitionLimiter.MAX_PARTITIONS_PROPERTY, 3);
        List<Text> tableNames = Arrays.asList(new Text("table1"), new Text("table2"));
        PartitionerCache partitionerCache = new PartitionerCache(conf);
        partitionerCache.createAndCachePartitioners(tableNames);
        TablePartitionerOffsets offsets = new TablePartitionerOffsets(conf, tableNames, partitionerCache);
        assertOffset(offsets, "table1", 4); // 0 through 4 are ok
        assertOffset(offsets, "table2", 7); // 5 through 7
        assertOffset(offsets, "table3", null);
    }
    
    @Test
    public void testWithCategoryMixedIn() throws ClassNotFoundException {
        conf.set(DelegatingPartitioner.DEFAULT_DELEGATE_PARTITIONER, RowHashingPartitioner.class.getName());
        conf.set(DelegatingPartitioner.PREFIX_DEDICATED_PARTITIONER + "table1", LimitedKeyPartitioner.class.getName());
        conf.setInt("table1." + PartitionLimiter.MAX_PARTITIONS_PROPERTY, 5);
        conf.set(DelegatingPartitioner.PREFIX_SHARED_MEMBERSHIP + "table3", "jam");
        conf.set(DelegatingPartitioner.PREFIX_CATEGORY_PARTITIONER + "jam", LimitedKeyPartitioner.class.getName());
        conf.setInt("jam." + PartitionLimiter.MAX_PARTITIONS_PROPERTY, 3);
        conf.set(DelegatingPartitioner.PREFIX_DEDICATED_PARTITIONER + "table4", LimitedKeyPartitioner.class.getName());
        conf.setInt("table4." + PartitionLimiter.MAX_PARTITIONS_PROPERTY, 11);
        conf.set(DelegatingPartitioner.PREFIX_SHARED_MEMBERSHIP + "table5", "jam");
        List<Text> tableNames = Arrays.asList(new Text("table1"), new Text("table3"), new Text("table4"), new Text("table5"));
        PartitionerCache partitionerCache = new PartitionerCache(conf);
        partitionerCache.createAndCachePartitioners(tableNames);
        TablePartitionerOffsets offsets = new TablePartitionerOffsets(conf, tableNames, partitionerCache);
        assertOffset(offsets, "table1", 4); // 0 through 4
        assertOffset(offsets, "table2", null);
        assertOffset(offsets, "table3", 7); // 5 through 7
        assertOffset(offsets, "table4", 18); // 8 through 18
        assertOffset(offsets, "table5", 7); // same as table 3
    }
}
