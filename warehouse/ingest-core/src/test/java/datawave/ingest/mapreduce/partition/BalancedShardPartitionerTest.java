package datawave.ingest.mapreduce.partition;

import datawave.ingest.mapreduce.handler.shard.ShardIdFactory;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.ShardedTableMapFile;
import datawave.util.TableName;
import datawave.util.time.DateHelper;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BalancedShardPartitionerTest {
    private static final int TOTAL_TSERVERS = 600;
    private static final int SHARDS_PER_DAY = 170;
    private static final int NUM_DAYS = 1500; // 4 years and 39 days ago
    private static final int NUM_REDUCE_TASKS = 270;
    private static Configuration conf;
    
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    
    private BalancedShardPartitioner partitioner = null;
    private ShardIdFactory shardIdFactory = new ShardIdFactory(conf);
    
    @BeforeClass
    public static void defineShardLocationsFile() throws IOException {
        conf = new Configuration();
        conf.setInt(ShardIdFactory.NUM_SHARDS, SHARDS_PER_DAY);
    }
    
    @Before
    public void setUp() throws IOException {
        partitioner = new BalancedShardPartitioner();
        // gotta load this every test, or using different values bleeds into other tests
        new TestShardGenerator(conf, temporaryFolder.newFolder(), NUM_DAYS, SHARDS_PER_DAY, TOTAL_TSERVERS, TableName.SHARD);
        partitioner.setConf(conf);
        assertEquals(TableName.SHARD, conf.get(ShardedTableMapFile.CONFIGURED_SHARDED_TABLE_NAMES));
    }
    
    @After
    public void tearDown() {
        partitioner = null;
        conf.unset(BalancedShardPartitioner.MISSING_SHARD_STRATEGY_PROP);
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
        new TestShardGenerator(conf, temporaryFolder.newFolder(), NUM_DAYS, SHARDS_PER_DAY, TOTAL_TSERVERS, TableName.SHARD, TableName.ERROR_SHARD);
        partitioner.setConf(conf);
        assertEquals(new HashSet<>(Arrays.asList(TableName.SHARD, TableName.ERROR_SHARD)),
                        new HashSet<>(conf.getStringCollection(ShardedTableMapFile.CONFIGURED_SHARDED_TABLE_NAMES)));
        
        // For a shard from today, we can assume that they're well balanced.
        // If offsetting is working, they will not go to the same partitions
        String today = formatDay(0);
        Key shardFromToday = new Key(today + "_1");
        // error shard should be in the first group of partitions
        verifyOffsetGroup(0, partitioner.getPartition(new BulkIngestKey(new Text(TableName.ERROR_SHARD), shardFromToday), new Value(), 1000), today);
        // shard should be in the second group of partitions
        verifyOffsetGroup(1, partitioner.getPartition(new BulkIngestKey(new Text(TableName.SHARD), shardFromToday), new Value(), 1000), today);
    }
    
    private void verifyOffsetGroup(int group, int partitionId, String date) {
        int numShards = shardIdFactory.getNumShards(date);
        
        Assert.assertTrue("partitionId " + partitionId + " is not >= " + (numShards * group), partitionId >= numShards * group);
        Assert.assertTrue("partitionId " + partitionId + " is not < " + (numShards * (group + 1)), partitionId < numShards * (group + 1));
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
    
    @Test
    public void testDifferentNumberShardPerDayCollapse() throws IOException {
        // Create SHARDS_PER_DAY splits for today, yesterday and the day before
        // Create 2 splits (_0 and _1) for 3 and 4 days ago.
        //
        // Now use the collapse strategy so days 3 and 4 only get split into 1 reducer
        // per existing split in Accumulo.
        //
        // This emulates a day having fewer splits for performance reasons because you don't expect
        // enough data to warrant the full compliment of splits and the overhead in the tserver of
        // tracking all those splits. The job will still generate data with row ids up to
        // SHARDS_PER_DAY, but simply write the same number of rfiles as splits.
        // See issues #45
        String tableName = "shard2";
        simulateDifferentNumberShardsPerDay("collapse", tableName);
        
        // 1 day ago should get SHARDS_PER_DAY partitions
        assertPartitionsForDay(partitioner, tableName, 1, SHARDS_PER_DAY);
        
        // 3 days ago should get 3 partitions, the _1, the _2 and the next days _0
        assertPartitionsForDay(partitioner, tableName, 3, 3);
        
        // now let's check they went in the right place
        Map<Integer,List<String>> reducers = new TreeMap<>();
        String formattedDay = formatDay(3);
        for (int i = 0; i < SHARDS_PER_DAY; i++) {
            String shardId = formattedDay + ("_" + i);
            int partition = partitioner.getPartition(new BulkIngestKey(new Text(tableName), new Key(shardId)), new Value(), NUM_REDUCE_TASKS);
            if (!reducers.containsKey(partition)) {
                reducers.put(partition, new ArrayList<>());
            }
            reducers.get(partition).add(shardId);
        }
        assertEquals(3, reducers.size());
        // partitions should be sequential, so get the _1 one
        int underscore1 = partitioner.getPartition(new BulkIngestKey(new Text(tableName), new Key(formattedDay + "_1")), new Value(), NUM_REDUCE_TASKS);
        assertEquals(2, reducers.get(underscore1).size()); // _1 should have _0 and _1
        // remember _10 sorts before _2
        int underscore4 = partitioner.getPartition(new BulkIngestKey(new Text(tableName), new Key(formattedDay + "_4")), new Value(), NUM_REDUCE_TASKS);
        assertEquals(103, reducers.get(underscore4).size()); // _4 should have 2, 3, 4, 10-19, 20-29, 30-39,
                                                             // 100-109, 110-119, 120-129, 130-139, 140-149, 150-159, and 160-169
        int underscoreNext0 = partitioner.getPartition(new BulkIngestKey(new Text(tableName), new Key(formatDay(2) + "_0")), new Value(), NUM_REDUCE_TASKS);
        assertEquals(65, reducers.get(underscoreNext0).size()); // next row_0, should have 5, 6, 7, 8, 9, 40-49, 50-59, 60-69, 70-79, 80-89, 90-99
    }
    
    @Test
    public void testDifferentNumberShardsPerDayHash() throws IOException {
        // this is old behavior, where if the split doesn't exist a hash of the shardid
        // is used to partition. You still get SHARDS_PER_DAY number of partitions
        //
        // hashing is the default implementation, so null is passed in
        String tableName = "shard3";
        simulateDifferentNumberShardsPerDay(null, tableName);
        
        // 1 day ago should get SHARDS_PER_DAY partitions
        assertPartitionsForDay(partitioner, tableName, 1, SHARDS_PER_DAY);
        
        // 3 days ago should get more than 3 partitions, can't reliably calculate
        // due to collisions
        assertTrue(getPartitionsForDay(partitioner, tableName, 3).size() > 3);
    }
    
    @Test
    public void testDifferentNumberShardsPerDayCollapseButOutsideRange() throws IOException {
        String tableName = "shard4";
        simulateDifferentNumberShardsPerDay("collapse", tableName);
        
        String formattedDay = formatDay(3);
        String shardId = formattedDay + ("_" + (99999999)); // should go to first partition for 2 days ago
        int partition = partitioner.getPartition(new BulkIngestKey(new Text(tableName), new Key(shardId)), new Value(), NUM_REDUCE_TASKS);
        String nextDay = formatDay(2);
        String nextShardId = nextDay + ("_0");
        int nextPartition = partitioner.getPartition(new BulkIngestKey(new Text(tableName), new Key(nextShardId)), new Value(), NUM_REDUCE_TASKS);
        assertEquals(nextPartition, partition);
    }
    
    private void simulateDifferentNumberShardsPerDay(String missingShardStrategy, String tableName) throws IOException {
        // This emulates today, yesterday and the day before have SHARDS_PER_DAY splits and
        // 3 days ago and 4 days ago only have 2 splits, _0 and _1.
        SortedMap<Text,String> locations = new TreeMap<>();
        long now = System.currentTimeMillis();
        int tserverId = 1;
        Text prevEndRow = new Text();
        for (int daysAgo = 0; daysAgo <= 2; daysAgo++) {
            String day = DateHelper.format(now - (daysAgo * DateUtils.MILLIS_PER_DAY));
            for (int currShard = 0; currShard < SHARDS_PER_DAY; currShard++) {
                locations.put(new Text(day + "_" + currShard), Integer.toString(tserverId++));
            }
        }
        for (int daysAgo = 3; daysAgo <= 4; daysAgo++) {
            String day = DateHelper.format(now - (daysAgo * DateUtils.MILLIS_PER_DAY));
            for (int currShard : Arrays.asList(1, 4)) {
                locations.put(new Text(day + "_" + currShard), Integer.toString(tserverId++));
            }
        }
        new TestShardGenerator(conf, temporaryFolder.newFolder(), locations, tableName);
        partitioner.setConf(conf);
        if (missingShardStrategy != null) {
            conf.set(BalancedShardPartitioner.MISSING_SHARD_STRATEGY_PROP, missingShardStrategy);
        }
        assertEquals(tableName, conf.get(ShardedTableMapFile.CONFIGURED_SHARDED_TABLE_NAMES));
        // check we made enough tservers
        assertEquals(SHARDS_PER_DAY * 3 + 2 + 2, tserverId - 1); // since it already ++'d for next one
    }
    
    private void assertPartitionsForDay(BalancedShardPartitioner partioner, String tableName, int daysAgo, int expectedNumOfPartitions) {
        assertEquals(expectedNumOfPartitions, getPartitionsForDay(partitioner, tableName, daysAgo).size());
    }
    
    private TreeSet<Integer> getPartitionsForDay(BalancedShardPartitioner partitioner, String tableName, int daysAgo) {
        String formattedDay = formatDay(daysAgo);
        TreeSet<Integer> partitionsUsed = new TreeSet<>();
        for (int i = 0; i < SHARDS_PER_DAY; i++) {
            String shardId = formattedDay + ("_" + i);
            int partition = partitioner.getPartition(new BulkIngestKey(new Text(tableName), new Key(shardId)), new Value(), NUM_REDUCE_TASKS);
            partitionsUsed.add(partition);
        }
        return partitionsUsed;
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
            int partition = partitionerIn.getPartition(new BulkIngestKey(new Text(TableName.SHARD), new Key(shardId)), new Value(), NUM_REDUCE_TASKS);
            if (partitionsUsed.contains(partition)) {
                collisions++;
            }
            partitionsUsed.add(partition);
        }
        // 9 is what we get by hashing the shardId
        Assert.assertTrue("For " + daysBack + " days ago, we had a different number of collisions: " + collisions, expectedCollisions >= collisions);
        // this
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
