package datawave.ingest.mapreduce.job;

import datawave.ingest.mapreduce.partition.DelegatePartitioner;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class PartitionerCacheTest {
    
    public static final int DEFAULT_PARTITION = 3;
    public static final int NUM_REDUCERS = 100;
    private PartitionerCache cache;
    private Configuration conf;
    
    @Before
    public void before() {
        conf = new Configuration();
        conf.set(PartitionerCache.DEFAULT_DELEGATE_PARTITIONER, DelegatingPartitionerTest.AlwaysReturnThree.class.getName());
    }
    
    @Test
    public void testValidateWillFilterOutTablesWithoutConfigurations() throws Exception {
        conf.set(PartitionerCache.PREFIX_DEDICATED_PARTITIONER + "table1", DelegatingPartitionerTest.AlwaysReturnOne.class.getName());
        cache = new PartitionerCache(conf);
        List<String> result = cache.validatePartitioners(new String[] {"table1", "table2"}, new Job(conf));
        Assert.assertTrue(result.contains("table1"));
        Assert.assertFalse("table2 partitioner class name is missing from the conf, yet it was not filtered out", result.contains("table2"));
    }
    
    @Test
    public void testValidateExpectedCase() throws Exception {
        conf.set(PartitionerCache.PREFIX_DEDICATED_PARTITIONER + "table1", DelegatingPartitionerTest.AlwaysReturnOne.class.getName());
        conf.set(PartitionerCache.PREFIX_DEDICATED_PARTITIONER + "table2", DelegatingPartitionerTest.AlwaysReturnTwo.class.getName());
        cache = new PartitionerCache(conf);
        List<String> result = cache.validatePartitioners(new String[] {"table1", "table2"}, new Job(conf));
        Assert.assertTrue(result.contains("table1"));
        Assert.assertTrue(result.contains("table2"));
    }
    
    @Test
    public void testValidateNoOverrides() throws Exception {
        List<String> result = new PartitionerCache(conf).validatePartitioners(new String[] {"table1", "table2"}, new Job(conf));
        Assert.assertEquals("None of the tables were configured, so the overrides list should be empty", result.size(), 0);
    }
    
    @Test
    public void testValidateWillFilterBadlyConfiguredTablesWithoutThrowingException() throws Exception {
        conf.set(PartitionerCache.PREFIX_DEDICATED_PARTITIONER + "table1", DelegatingPartitionerTest.AlwaysReturnOne.class.getName());
        conf.set(PartitionerCache.PREFIX_DEDICATED_PARTITIONER + "table2", PartitionerThatMustNotBeCreated.class.getName());
        cache = new PartitionerCache(conf);
        List<String> result = new PartitionerCache(conf).validatePartitioners(new String[] {"table1", "table2"}, new Job(conf));
        Assert.assertTrue(result.contains("table1"));
        Assert.assertFalse("table2's partitioner cannot be created, so it should be removed from the list", result.contains("table2"));
    }
    
    @Test
    public void testGetPartitionerReturnsDefaultForTableWithoutOverride() throws Exception {
        conf.set(PartitionerCache.DEFAULT_DELEGATE_PARTITIONER, DelegatingPartitionerTest.AlwaysReturnOne.class.getName());
        conf.set(PartitionerCache.PREFIX_DEDICATED_PARTITIONER + "table1", DelegatingPartitionerTest.AlwaysReturnTwo.class.getName());
        PartitionerCache partitionerCache = new PartitionerCache(conf);
        partitionerCache.createAndCachePartitioners(Arrays.asList(new Text("table1")));
        Assert.assertTrue("table1 was configured to use a different partitioner",
                        partitionerCache.getPartitioner(new Text("table1")) instanceof DelegatingPartitionerTest.AlwaysReturnTwo);
        Assert.assertTrue("table2 was not configured with a partitioner so it should use the default but didn't",
                        partitionerCache.getPartitioner(new Text("table2")) instanceof DelegatingPartitionerTest.AlwaysReturnOne);
    }
    
    @Test
    public void testGetPartitionerReturnsDefaultForTablesWithoutOverrides() throws Exception {
        conf.set(PartitionerCache.DEFAULT_DELEGATE_PARTITIONER, DelegatingPartitionerTest.AlwaysReturnOne.class.getName());
        PartitionerCache partitionerCache = new PartitionerCache(conf);
        partitionerCache.createAndCachePartitioners(Arrays.asList(new Text[] {}));
        Assert.assertTrue("table1 was not configured with a partitioner so it should use the default but didn't",
                        partitionerCache.getPartitioner(new Text("table2")) instanceof DelegatingPartitionerTest.AlwaysReturnOne);
        Assert.assertTrue("table2 was not configured with a partitioner so it should use the default but didn't",
                        partitionerCache.getPartitioner(new Text("table2")) instanceof DelegatingPartitionerTest.AlwaysReturnOne);
    }
    
    @Test
    public void testGetCategoryForNonMember() throws Exception {
        PartitionerCache partitionerCache = new PartitionerCache(conf);
        Assert.assertNull(PartitionerCache.getCategory(conf, new Text("table1")));
    }
    
    @Test
    public void testGetCategoryForMember() throws Exception {
        PartitionerCache partitionerCache = new PartitionerCache(conf);
        Assert.assertNull(PartitionerCache.getCategory(conf, new Text("table1")));
    }
    
    @Test
    public void testCorrectCategoriesForMixedConfigurations() throws Exception {
        conf.set(PartitionerCache.PREFIX_CATEGORY_PARTITIONER + "myCategory", DelegatingPartitionerTest.AlwaysReturnOne.class.getName());
        conf.set(PartitionerCache.PREFIX_CATEGORY_PARTITIONER + "anotherCategory", DelegatingPartitionerTest.AlwaysReturnTwo.class.getName());
        conf.set(PartitionerCache.PREFIX_SHARED_MEMBERSHIP + "table1", "myCategory");
        conf.set(PartitionerCache.PREFIX_SHARED_MEMBERSHIP + "table2", "anotherCategory");
        conf.set(PartitionerCache.PREFIX_SHARED_MEMBERSHIP + "table3", "anotherCategory");
        PartitionerCache partitionerCache = new PartitionerCache(conf);
        Assert.assertEquals(new Text("myCategory"), PartitionerCache.getCategory(conf, new Text("table1")));
        Assert.assertEquals(new Text("anotherCategory"), PartitionerCache.getCategory(conf, new Text("table2")));
        Assert.assertEquals(new Text("anotherCategory"), PartitionerCache.getCategory(conf, new Text("table3")));
        Assert.assertNull(PartitionerCache.getCategory(conf, new Text("table4")));
    }
    
    public class PartitionerThatMustNotBeCreated implements DelegatePartitioner {
        public PartitionerThatMustNotBeCreated() {
            throw new NotImplementedException();
        }
        
        @Override
        public void configureWithPrefix(String prefix) {}
        
        @Override
        public int getNumPartitions() {
            return 0;
        }
        
        @Override
        public void initializeJob(Job job) {}
        
        @Override
        public void setConf(Configuration conf) {}
        
        @Override
        public Configuration getConf() {
            return null;
        }
    }
}
