package datawave.ingest.mapreduce.job;

import datawave.ingest.mapreduce.partition.DelegatePartitioner;
import datawave.ingest.mapreduce.partition.LimitedKeyPartitioner;
import datawave.ingest.mapreduce.partition.PartitionLimiter;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DelegatingPartitionerTest {
    public static final int DEFAULT_PARTITION = 3;
    public static final int NUM_REDUCERS = 100;
    private DelegatingPartitioner manager;
    private Configuration conf;
    
    @Before
    public void before() {
        manager = new DelegatingPartitioner();
        conf = new Configuration();
        conf.setInt("splits.num.reduce", NUM_REDUCERS);
        conf.set(PartitionerCache.DEFAULT_DELEGATE_PARTITIONER, AlwaysReturnThree.class.getName());
    }
    
    @Test(expected = Exception.class)
    public void testThrowsExceptionOnInconsistentConfig() throws Exception {
        conf.set(DelegatingPartitioner.TABLE_NAMES_WITH_CUSTOM_PARTITIONERS, "table1,table2"); // table 2 is here
        conf.set(PartitionerCache.PREFIX_DEDICATED_PARTITIONER + "table1", AlwaysReturnOne.class.getName());
        // table2 partitioner class name is missing
        manager.setConf(conf);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testDoesntLikeEmptyList() throws Exception {
        conf.set(DelegatingPartitioner.TABLE_NAMES_WITH_CUSTOM_PARTITIONERS, "");
        manager.setConf(conf);
    }
    
    @Test
    public void testRoutesPartitionsBothOverridden() throws Exception {
        conf.set(DelegatingPartitioner.TABLE_NAMES_WITH_CUSTOM_PARTITIONERS, "table1,table2");
        conf.set(PartitionerCache.PREFIX_DEDICATED_PARTITIONER + "table1", AlwaysReturnOne.class.getName());
        conf.set(PartitionerCache.PREFIX_DEDICATED_PARTITIONER + "table2", AlwaysReturnTwo.class.getName());
        manager.setConf(conf);
        
        Assert.assertEquals(1, getPartition("table1"));
        Assert.assertEquals(2, getPartition("table2"));
    }
    
    @Test
    public void testRoutesToTableConfiguredDelegateAndIgnoresOther() throws Exception {
        conf.set(DelegatingPartitioner.TABLE_NAMES_WITH_CUSTOM_PARTITIONERS, "table1");
        limitTable("table1", 1);
        // this shouldn't get used because table2 isn't in the list of table names and
        // if it's missing from there we run the risk that the partitioner can't be successfully created
        // (failed validation)
        conf.set(PartitionerCache.PREFIX_DEDICATED_PARTITIONER + "table2", AlwaysReturnTwo.class.getName());
        manager.setConf(conf);
        
        // 0 because it will only use the one bin
        Assert.assertEquals(NUM_REDUCERS - 1, getPartition("table1"));
        Assert.assertEquals(DEFAULT_PARTITION, getPartition("table2"));
    }
    
    @Test
    public void testOffsetsInOrder() throws Exception {
        conf.set(DelegatingPartitioner.TABLE_NAMES_WITH_CUSTOM_PARTITIONERS, "table1,table2,table3");
        limitTable("table1", 3);
        limitTable("table2", 2);
        limitTable("table3", 4);
        manager.setConf(conf);
        
        for (int i = 0; i < 30; i++) {
            assertBetween(NUM_REDUCERS - 1 - 2, NUM_REDUCERS - 1 - 0, getPartition("table1", true));
            assertBetween(NUM_REDUCERS - 1 - 4, NUM_REDUCERS - 1 - 3, getPartition("table2", true));
            assertBetween(NUM_REDUCERS - 1 - 8, NUM_REDUCERS - 1 - 5, getPartition("table3", true));
        }
    }
    
    private void limitTable(final String tableName, int maxPartitions) {
        conf.set(PartitionerCache.PREFIX_DEDICATED_PARTITIONER + tableName, LimitedKeyPartitioner.class.getName());
        conf.setInt(tableName + "." + PartitionLimiter.MAX_PARTITIONS_PROPERTY, maxPartitions);
    }
    
    private void limitCategory(final String categoryName, int maxPartitions) {
        conf.set(PartitionerCache.PREFIX_CATEGORY_PARTITIONER + categoryName, LimitedKeyPartitioner.class.getName());
        conf.setInt(categoryName + "." + PartitionLimiter.MAX_PARTITIONS_PROPERTY, maxPartitions);
    }
    
    @Test
    public void testOffsetsInScrambledOrder() throws Exception {
        conf.set(DelegatingPartitioner.TABLE_NAMES_WITH_CUSTOM_PARTITIONERS, "table1,table2,table3");
        limitTable("table1", 3);
        limitTable("table2", 2);
        limitTable("table3", 4);
        manager.setConf(conf);
        
        for (int i = 0; i < 30; i++) {
            assertBetween(NUM_REDUCERS - 1 - 4, NUM_REDUCERS - 1 - 3, getPartition("table2", true));
            assertBetween(NUM_REDUCERS - 1 - 8, NUM_REDUCERS - 1 - 5, getPartition("table3", true));
            assertBetween(NUM_REDUCERS - 1 - 2, NUM_REDUCERS - 1 - 0, getPartition("table1", true));
        }
    }
    
    @Test
    public void testOffsetsCumulativelyExceedingNumReducers() throws Exception {
        conf.set(DelegatingPartitioner.TABLE_NAMES_WITH_CUSTOM_PARTITIONERS, "table1,table2,table3");
        limitTable("table1", 3);
        limitTable("table2", 2);
        limitTable("table3", 4);
        manager.setConf(conf);
        
        int numPartitions = 5;
        for (int i = 0; i < 30; i++) {
            assertBetween(numPartitions - 1 - 4, numPartitions - 1 - 3, manager.getPartition(createKeyFor("table2", true), new Value(), numPartitions));
            assertBetween(numPartitions - 1 - 3, numPartitions - 1, manager.getPartition(createKeyFor("table3", true), new Value(), numPartitions));
            assertBetween(numPartitions - 1 - 2, numPartitions - 1, manager.getPartition(createKeyFor("table1", true), new Value(), numPartitions));
        }
    }
    
    @Test
    public void testOffsetPartitionersMixedWithNonOffsetPartitioner() throws Exception {
        conf.set(DelegatingPartitioner.TABLE_NAMES_WITH_CUSTOM_PARTITIONERS, "table1,table2,table4");
        limitTable("table1", 3);
        limitTable("table2", 2);
        conf.set(PartitionerCache.PREFIX_DEDICATED_PARTITIONER + "table4", AlwaysReturnOne.class.getName());
        manager.setConf(conf);
        
        for (int i = 0; i < 30; i++) {
            assertBetween(NUM_REDUCERS - 1 - 4, NUM_REDUCERS - 1 - 3, getPartition("table2", true));
            Assert.assertEquals(1, getPartition("table4"));
            assertBetween(NUM_REDUCERS - 1 - 2, NUM_REDUCERS - 1 - 0, getPartition("table1", true));
        }
    }
    
    @Test
    public void testOffsetPartitionersMixedWithDefaultPartitioner() throws Exception {
        conf.set(DelegatingPartitioner.TABLE_NAMES_WITH_CUSTOM_PARTITIONERS, "table1,table2");
        limitTable("table1", 3);
        limitTable("table2", 2);
        manager.setConf(conf);
        
        for (int i = 0; i < 30; i++) {
            assertBetween(NUM_REDUCERS - 1 - 4, NUM_REDUCERS - 1 - 3, getPartition("table2", true));
            assertBetween(0, NUM_REDUCERS - 1, getPartition("tableX", true));
            assertBetween(NUM_REDUCERS - 1 - 2, NUM_REDUCERS - 1 - 0, getPartition("table1", true));
        }
    }
    
    private void assertBetween(int minInclusive, int maxInclusive, int actual) {
        Assert.assertTrue("actual: " + actual, minInclusive <= actual);
        Assert.assertTrue("actual: " + actual, actual <= maxInclusive);
    }
    
    @Test
    public void testRoutesPartitionsOneOverridden() throws Exception {
        conf.set(PartitionerCache.DEFAULT_DELEGATE_PARTITIONER, AlwaysReturnThree.class.getName());
        conf.set(DelegatingPartitioner.TABLE_NAMES_WITH_CUSTOM_PARTITIONERS, "table1");
        conf.set(DelegatingPartitioner.PREFIX_DEDICATED_PARTITIONER + "table1", AlwaysReturnOne.class.getName());
        manager.setConf(conf);
        
        Assert.assertEquals(1, getPartition("table1"));
        Assert.assertEquals(DEFAULT_PARTITION, getPartition("table2"));
    }
    
    @Test
    public void testRoutesPartitionsNoOverrides() throws Exception {
        conf.set(DelegatingPartitioner.DEFAULT_DELEGATE_PARTITIONER, AlwaysReturnThree.class.getName());
        manager.setConf(conf);
        
        Assert.assertEquals(DEFAULT_PARTITION, getPartition("table1"));
        Assert.assertEquals(DEFAULT_PARTITION, getPartition("table2"));
    }
    
    @Test
    public void testOneInCategoryOneDefault() throws Exception {
        conf.set(DelegatingPartitioner.DEFAULT_DELEGATE_PARTITIONER, AlwaysReturnThree.class.getName());
        conf.set(DelegatingPartitioner.TABLE_NAMES_WITH_CUSTOM_PARTITIONERS, "table1");
        conf.set(DelegatingPartitioner.PREFIX_SHARED_MEMBERSHIP + "table1", "category5");
        conf.set(DelegatingPartitioner.PREFIX_CATEGORY_PARTITIONER + "category5", AlwaysReturnOne.class.getName());
        manager.setConf(conf);
        
        Assert.assertEquals(1, getPartition("table1"));
        Assert.assertEquals(DEFAULT_PARTITION, getPartition("table2"));
    }
    
    @Test
    public void testBothInCategory() throws Exception {
        conf.set(DelegatingPartitioner.DEFAULT_DELEGATE_PARTITIONER, AlwaysReturnThree.class.getName());
        conf.set(DelegatingPartitioner.TABLE_NAMES_WITH_CUSTOM_PARTITIONERS, "table1,table2");
        conf.set(DelegatingPartitioner.PREFIX_SHARED_MEMBERSHIP + "table1", "category5");
        conf.set(DelegatingPartitioner.PREFIX_SHARED_MEMBERSHIP + "table2", "category5");
        conf.set(DelegatingPartitioner.PREFIX_CATEGORY_PARTITIONER + "category5", AlwaysReturnOne.class.getName());
        manager.setConf(conf);
        
        Assert.assertEquals(1, getPartition("table1"));
        Assert.assertEquals(1, getPartition("table2"));
    }
    
    @Test
    public void testCategoriesMixedWithDedicated() throws Exception {
        conf.set(DelegatingPartitioner.DEFAULT_DELEGATE_PARTITIONER, AlwaysReturnThree.class.getName());
        conf.set(DelegatingPartitioner.TABLE_NAMES_WITH_CUSTOM_PARTITIONERS, "table1,table2,table3");
        conf.set(DelegatingPartitioner.PREFIX_SHARED_MEMBERSHIP + "table1", "category5");
        conf.set(DelegatingPartitioner.PREFIX_SHARED_MEMBERSHIP + "table2", "category5");
        conf.set(DelegatingPartitioner.PREFIX_CATEGORY_PARTITIONER + "category5", AlwaysReturnOne.class.getName());
        conf.set(DelegatingPartitioner.PREFIX_DEDICATED_PARTITIONER + "table3", AlwaysReturnTwo.class.getName());
        manager.setConf(conf);
        
        Assert.assertEquals(1, getPartition("table1"));
        Assert.assertEquals(1, getPartition("table2"));
        Assert.assertEquals(2, getPartition("table3"));
    }
    
    @Test
    public void testLimitCategory() throws Exception {
        conf.set(DelegatingPartitioner.TABLE_NAMES_WITH_CUSTOM_PARTITIONERS, "table1,table2,table3");
        conf.set(DelegatingPartitioner.PREFIX_SHARED_MEMBERSHIP + "table1", "category5");
        conf.set(DelegatingPartitioner.PREFIX_SHARED_MEMBERSHIP + "table2", "category5");
        limitCategory("category5", 3);
        limitTable("table3", 4);
        manager.setConf(conf);
        
        for (int i = 0; i < 30; i++) {
            assertBetween(NUM_REDUCERS - 1 - 2, NUM_REDUCERS - 1 - 0, getPartition("table2", true));
            assertBetween(NUM_REDUCERS - 1 - 2, NUM_REDUCERS - 1 - 0, getPartition("table1", true));
            assertBetween(NUM_REDUCERS - 1 - 6, NUM_REDUCERS - 1 - 3, getPartition("table3", true));
        }
    }
    
    @Test
    public void testLimitCategories() throws Exception {
        conf.set(DelegatingPartitioner.TABLE_NAMES_WITH_CUSTOM_PARTITIONERS, "table1,table2,table3");
        conf.set(DelegatingPartitioner.PREFIX_SHARED_MEMBERSHIP + "table2", "category5");
        limitCategory("category5", 3);
        conf.set(DelegatingPartitioner.PREFIX_SHARED_MEMBERSHIP + "table1", "category4");
        conf.set(DelegatingPartitioner.PREFIX_SHARED_MEMBERSHIP + "table3", "category4");
        limitCategory("category4", 4);
        manager.setConf(conf);
        
        for (int i = 0; i < 30; i++) {
            assertBetween(NUM_REDUCERS - 1 - 6, NUM_REDUCERS - 1 - 4, getPartition("table2", true));
            assertBetween(NUM_REDUCERS - 1 - 3, NUM_REDUCERS - 1 - 0, getPartition("table3", true));
            assertBetween(NUM_REDUCERS - 1 - 3, NUM_REDUCERS - 1 - 0, getPartition("table1", true));
        }
    }
    
    @Test
    public void testDelegate() throws Exception {
        conf.set(DelegatingPartitioner.TABLE_NAMES_WITH_CUSTOM_PARTITIONERS, "table1,table2,table3");
        conf.set(DelegatingPartitioner.PREFIX_SHARED_MEMBERSHIP + "table2", "category5");
        limitCategory("category5", 3);
        conf.set(DelegatingPartitioner.PREFIX_SHARED_MEMBERSHIP + "table1", "category4");
        conf.set(DelegatingPartitioner.PREFIX_SHARED_MEMBERSHIP + "table3", "category4");
        conf.set(DelegatingPartitioner.PREFIX_CATEGORY_PARTITIONER + "category4", DelegateReturnsFour.class.getName());
        manager.setConf(conf);
        
        for (int i = 0; i < 30; i++) {
            assertBetween(NUM_REDUCERS - 1 - 2, NUM_REDUCERS - 1 - 0, getPartition("table2", true));
            Assert.assertEquals(4, getPartition("table3", true));
            Assert.assertEquals(4, getPartition("table1", true));
        }
    }
    
    private int getPartition(String tableName) {
        return manager.getPartition(createKeyFor(tableName, false), new Value(), NUM_REDUCERS);
    }
    
    private int getPartition(String tableName, boolean useRandom) {
        return manager.getPartition(createKeyFor(tableName, useRandom), new Value(), NUM_REDUCERS);
    }
    
    private BulkIngestKey createKeyFor(String tableName, boolean useRandom) {
        return new BulkIngestKey(new Text(tableName), new Key(useRandom ? "" + Math.random() : ""));
    }
    
    public static class AlwaysReturnOne extends HardcodedTestPartitioner {
        public AlwaysReturnOne() {
            super(1);
        }
    }
    
    public static class AlwaysReturnTwo extends HardcodedTestPartitioner {
        public AlwaysReturnTwo() {
            super(2);
        }
    }
    
    public static class AlwaysReturnThree extends HardcodedTestPartitioner {
        public AlwaysReturnThree() {
            super(3);
        }
    }
    
    public static class HardcodedTestPartitioner extends Partitioner<BulkIngestKey,Value> {
        int predefinedPartition = 1;
        
        public HardcodedTestPartitioner(int partition) {
            predefinedPartition = partition;
        }
        
        @Override
        public int getPartition(BulkIngestKey bulkIngestKey, Value value, int numPartitions) {
            return predefinedPartition;
        }
    }
    
    public static class DelegateReturnsFour extends Partitioner<BulkIngestKey,Value> implements Configurable, DelegatePartitioner {
        @Override
        public void configureWithPrefix(String prefix) {/* no op */}
        
        @Override
        public int getNumPartitions() {
            return Integer.MAX_VALUE;
        }
        
        @Override
        public void initializeJob(Job job) { /* no op */}
        
        @Override
        public void setConf(Configuration conf) {/* no op */}
        
        @Override
        public Configuration getConf() {
            return null; // deliberate
        }
        
        @Override
        public int getPartition(BulkIngestKey bulkIngestKey, Value value, int numPartitions) {
            return 4; // deliberate
        }
    }
}
