package datawave.ingest.mapreduce.partition;

import datawave.ingest.mapreduce.handler.shard.ShardIdFactory;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class TabletLocationHashPartitionerTest {
    public static final int MAX_EXPECTED_COLLISIONS = 70;
    
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    
    int TOTAL_TSERVERS = 600;
    int SHARDS_PER_DAY = 170;
    int NUM_DAYS = 1500;
    int NUM_REDUCE_TASKS = 270;
    Configuration conf = new Configuration();
    TabletLocationHashPartitioner partitioner = null;
    
    @Before
    public void setUp() {
        conf = new Configuration();
        partitioner = new TabletLocationHashPartitioner();
        partitioner.setConf(conf);
    }
    
    @After
    public void tearDown() {
        conf.clear();
        conf = null;
        partitioner = null;
    }
    
    @Test
    public void testLocationHashPartitioner() throws Exception {
        conf.setInt(ShardIdFactory.NUM_SHARDS, SHARDS_PER_DAY);
        new TestShardGenerator(conf, temporaryFolder.newFolder(), NUM_DAYS, SHARDS_PER_DAY, TOTAL_TSERVERS, "shard");
        TabletLocationHashPartitioner partitionerTwo = new TabletLocationHashPartitioner();
        partitionerTwo.setConf(conf);
        
        BalancedShardPartitionerTest.assertExpectedCollisions(partitionerTwo, 0, MAX_EXPECTED_COLLISIONS);
        BalancedShardPartitionerTest.assertExpectedCollisions(partitionerTwo, 1, MAX_EXPECTED_COLLISIONS);
        BalancedShardPartitionerTest.assertExpectedCollisions(partitionerTwo, 2, MAX_EXPECTED_COLLISIONS);
        
        BalancedShardPartitionerTest.assertExpectedCollisions(partitionerTwo, 3, MAX_EXPECTED_COLLISIONS);
        BalancedShardPartitionerTest.assertExpectedCollisions(partitionerTwo, 4, MAX_EXPECTED_COLLISIONS);
        BalancedShardPartitionerTest.assertExpectedCollisions(partitionerTwo, 5, MAX_EXPECTED_COLLISIONS);
        BalancedShardPartitionerTest.assertExpectedCollisions(partitionerTwo, 20, MAX_EXPECTED_COLLISIONS);
        BalancedShardPartitionerTest.assertExpectedCollisions(partitionerTwo, 100, MAX_EXPECTED_COLLISIONS);
    }
    
    @Test
    public void testPigeonHolesAreReal() throws Exception {
        ArrayList<String> allLocations = new ArrayList<>();
        ArrayList<String> locationsToUse = new ArrayList<>();
        for (int tserverNum = 1; tserverNum < TOTAL_TSERVERS; tserverNum++) {
            allLocations.add("" + tserverNum);
            if (tserverNum < (TOTAL_TSERVERS - SHARDS_PER_DAY) && tserverNum > (TOTAL_TSERVERS - (2 * SHARDS_PER_DAY))) {
                locationsToUse.add("" + tserverNum);
            }
        }
        Set<Integer> partitionsUsed = new HashSet<>();
        
        int collisions = 0;
        for (String location : locationsToUse) {
            byte[] bytes = location.getBytes();
            int hashCode = location.hashCode() & Integer.MAX_VALUE;
            int partition = (hashCode) % NUM_REDUCE_TASKS;
            if (partitionsUsed.contains(partition)) {
                collisions++;
            }
            partitionsUsed.add(partition);
        }
        Assert.assertEquals(42, collisions);
    }
}
