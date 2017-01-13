package nsa.datawave.ingest.mapreduce.partition;

import nsa.datawave.ingest.mapreduce.handler.shard.ShardIdFactory;
import nsa.datawave.ingest.mapreduce.job.BulkIngestKey;
import nsa.datawave.util.time.DateHelper;
import org.apache.accumulo.core.data.*;
import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.junit.*;

import java.io.IOException;
import java.util.TreeSet;

public class BalancedShardPartitionerTest {
    private static final int TOTAL_TSERVERS = 600;
    private static final int SHARDS_PER_DAY = 170;
    private static final int NUM_DAYS = 1500;
    private static final int NUM_REDUCE_TASKS = 270;
    private static Configuration conf;
    private BalancedShardPartitioner partitioner = null;
    int numShards = ShardIdFactory.getNumShards(conf);
    
    @BeforeClass
    public static void defineShardLocationsFile() throws IOException {
        conf = new Configuration();
        conf.setInt(ShardIdFactory.NUM_SHARDS, SHARDS_PER_DAY);
        new TestShardGenerator(conf, NUM_DAYS, SHARDS_PER_DAY, TOTAL_TSERVERS, "shard");
    }
    
    @Before
    public void setUp() {
        partitioner = new BalancedShardPartitioner();
        partitioner.setConf(conf);
    }
    
    @After
    public void tearDown() {
        partitioner = null;
    }
    
    @Test
    public void testNoCollisionsTodayAndBack2Days() throws Exception {
        assertCollisionsLessThan(0, 0);
        assertCollisionsLessThan(1, 0);
        assertCollisionsLessThan(2, 0);
    }
    
    @Test
    public void testTwoTablesAreOffsetted() throws Exception {
        // create another split files for this test that contains two tables. register the tables names for both shard and error shard
        new TestShardGenerator(conf, NUM_DAYS, SHARDS_PER_DAY, TOTAL_TSERVERS, "shard", "errorShard");
        partitioner.setConf(conf);
        
        // For a shard from today, we can assume that they're well balanced.
        // If offsetting is working, they will not go to the same partitions
        Key shardFromToday = new Key(formatDay(0) + "_1");
        // shard should be in the first group of partitions
        verifyOffsetGroup(0, partitioner.getPartition(new BulkIngestKey(new Text("shard"), shardFromToday), new Value(), 1000));
        // error shard should be in the second group of partitions
        verifyOffsetGroup(1, partitioner.getPartition(new BulkIngestKey(new Text("errorShard"), shardFromToday), new Value(), 1000));
    }
    
    private void verifyOffsetGroup(int group, int partitionId) {
        Assert.assertTrue("partitionId " + partitionId, partitionId >= numShards * group);
        Assert.assertTrue("partitionId " + partitionId, partitionId < numShards * (group + 1));
    }
    
    @Test
    public void testNoCollisionsTwoAhead() throws Exception {
        assertCollisionsLessThan(-1, 45);
        assertCollisionsLessThan(-2, 45);
    }
    
    @Test
    public void testLimitedCollisionsForPastFewMonths() throws Exception {
        // note that hash partitioner is higher than this, but our fake shard assignments are randomized at this point
        assertCollisionsLessThan(3, 45);
        assertCollisionsLessThan(4, 45);
        assertCollisionsLessThan(5, 45);
        assertCollisionsLessThan(20, 45);
        assertCollisionsLessThan(100, 45);
    }
    
    @Test
    public void testMoreCollisionsYearsAgo() throws Exception {
        // hash partitioner is lower for this date range
        assertCollisionsLessThan(950, 75);
        assertCollisionsLessThan(1400, 75);
    }
    
    public void assertCollisionsLessThan(int daysBack, int expectedCollisions) {
        assertExpectedCollisions(partitioner, daysBack, expectedCollisions);
    }
    
    public static void assertExpectedCollisions(Partitioner partitionerIn, int daysBack, int expectedCollisions) {
        String formattedDay = formatDay(daysBack);
        TreeSet<Integer> partitionsUsed = new TreeSet<>();
        int collisions = 0;
        for (int i = 1; i < SHARDS_PER_DAY; i++) {
            String shardId = formattedDay + ("_" + i);
            int partition = partitionerIn.getPartition(new BulkIngestKey(new Text("shard"), new Key(shardId)), new Value(), NUM_REDUCE_TASKS);
            if (partitionsUsed.contains(partition)) {
                collisions++;
            }
            partitionsUsed.add(partition);
        }
        // 9 is what we get by hashing the shardId
        Assert.assertTrue("For " + daysBack + " days ago, we had a different number of collisions: " + collisions, expectedCollisions >= collisions); // this
                                                                                                                                                      // has
                                                                                                                                                      // more to
                                                                                                                                                      // do with
                                                                                                                                                      // the
                                                                                                                                                      // random
                                                                                                                                                      // assignment
                                                                                                                                                      // of the
                                                                                                                                                      // tablets
        
    }
    
    private static String formatDay(int daysBack) {
        return DateHelper.format(System.currentTimeMillis() - (daysBack * DateUtils.MILLIS_PER_DAY));
    }
}
