package datawave.ingest.mapreduce.job;

import datawave.ingest.data.config.ingest.AccumuloHelper;
import datawave.ingest.mapreduce.handler.shard.ShardIdFactory;
import datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;
import datawave.util.TableName;
import datawave.util.time.DateHelper;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class ShardedTableMapFileTest {
    private static final Log LOG = LogFactory.getLog(ShardedTableMapFileTest.class);
    
    public static final String PASSWORD = "123";
    public static final String USERNAME = "root";
    private static final String TABLE_NAME = "unitTestTable";
    private static final int SHARDS_PER_DAY = 10;
    private static Configuration conf;
    
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    
    @BeforeClass
    public static void defineShardLocationsFile() throws IOException {
        conf = new Configuration();
        conf.setInt(ShardIdFactory.NUM_SHARDS, SHARDS_PER_DAY);
        conf.set(ShardedDataTypeHandler.SHARDED_TNAMES, TableName.SHARD);
    }
    
    @Test
    public void testWriteSplitsToFileAndReadThem() throws Exception {
        Configuration conf = new Configuration();
        conf.setInt(ShardIdFactory.NUM_SHARDS, SHARDS_PER_DAY);
        
        conf.set(ShardedDataTypeHandler.SHARDED_TNAMES, TABLE_NAME
                        + ",shard_ingest_unit_test_table_1,shard_ingest_unit_test_table_2,shard_ingest_unit_test_table_3");
        
        String[] tableNames = new String[] {TABLE_NAME};
        conf.set(ShardedTableMapFile.TABLE_NAMES, StringUtils.join(",", tableNames));
        
        Map<Text,String> splits = new HashMap<>();
        splits.put(new Text("zEndRow"), "location2_1234");
        
        Path file = createSplitsFile(splits, conf, 1);
        conf.set(ShardedTableMapFile.SHARDED_MAP_FILE_PATHS_RAW, TABLE_NAME + "=" + file);
        ShardedTableMapFile.setupFile(conf);
        TreeMap<Text,String> result = ShardedTableMapFile.getShardIdToLocations(conf, TABLE_NAME);
        Assert.assertEquals("location2_1234", result.get(new Text("zEndRow")).toString());
        Assert.assertEquals(1, result.size());
    }
    
    @Test(timeout = 240000)
    public void testWriteSplitsToAccumuloAndReadThem() throws Exception {
        
        // Added timeout to this test b/c it could hang infinitely without failing, e.g., whenever
        // MiniAccumuloCluster starts up but tserver subsequently dies. To troubleshoot timeout errors
        // here in the future, the MAC instance's local /tmp/ path should logged in
        // createMiniAccumuloWithTestTableAndSplits method
        
        Configuration conf = new Configuration();
        conf.setInt(ShardIdFactory.NUM_SHARDS, 1);
        conf.setInt(ShardedTableMapFile.SHARDS_BALANCED_DAYS_TO_VERIFY, 1);
        String today = formatDay(0) + "_1";
        
        MiniAccumuloCluster accumuloCluster = null;
        try {
            SortedSet<Text> sortedSet = new TreeSet<>();
            sortedSet.add(new Text(today));
            accumuloCluster = createMiniAccumuloWithTestTableAndSplits(sortedSet);
            
            configureAccumuloHelper(conf, accumuloCluster);
            
            conf.set(ShardedDataTypeHandler.SHARDED_TNAMES, TABLE_NAME
                            + ",shard_ingest_unit_test_table_1,shard_ingest_unit_test_table_2,shard_ingest_unit_test_table_3");
            conf.set(ShardedTableMapFile.TABLE_NAMES, TABLE_NAME);
            setWorkingDirectory(conf);
            ShardedTableMapFile.setupFile(conf);
        } finally {
            if (null != accumuloCluster) {
                accumuloCluster.stop();
            }
        }
        
        TreeMap<Text,String> result = ShardedTableMapFile.getShardIdToLocations(conf, TABLE_NAME);
        Assert.assertNotNull(result.get(new Text(today)).toString());
        Assert.assertEquals(1, result.size());
    }
    
    private MiniAccumuloCluster createMiniAccumuloWithTestTableAndSplits(SortedSet<Text> sortedSet) throws IOException, InterruptedException,
                    AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
        MiniAccumuloCluster accumuloCluster;
        File clusterDir = temporaryFolder.newFolder();
        LOG.info("Created local directory for MiniAccumuloCluster: " + clusterDir.getAbsolutePath());
        accumuloCluster = new MiniAccumuloCluster(clusterDir, PASSWORD);
        accumuloCluster.start();
        
        Connector connector = accumuloCluster.getConnector(USERNAME, PASSWORD);
        TableOperations tableOperations = connector.tableOperations();
        tableOperations.create(TABLE_NAME);
        tableOperations.addSplits(TABLE_NAME, sortedSet);
        
        return accumuloCluster;
    }
    
    private void configureAccumuloHelper(Configuration conf, MiniAccumuloCluster accumuloCluster) {
        AccumuloHelper.setInstanceName(conf, accumuloCluster.getInstanceName());
        AccumuloHelper.setPassword(conf, PASSWORD.getBytes());
        AccumuloHelper.setUsername(conf, USERNAME);
        AccumuloHelper.setZooKeepers(conf, accumuloCluster.getZooKeepers());
    }
    
    private Path createSplitsFile(Map<Text,String> splits, Configuration conf, int expectedNumRows) throws IOException {
        return createSplitsFile(splits, conf, expectedNumRows, "test");
    }
    
    private Path createSplitsFile(Map<Text,String> splits, Configuration conf, int expectedNumRows, String tableName) throws IOException {
        conf.set(FileSystem.FS_DEFAULT_NAME_KEY, URI.create("file:///").toString());
        conf.setLong("fs.local.block.size", 32 * 1024 * 1024);
        FileSystem fs = setWorkingDirectory(conf);
        
        Path path = new Path("splits" + tableName + ".seq");
        Path file = fs.makeQualified(path);
        long actualCount = ShardedTableMapFile.writeSplitsFile(splits, file, conf);
        Map<String,Path> shardedTableMapFiles = new HashMap<>();
        shardedTableMapFiles.put(tableName, path);
        ShardedTableMapFile.addToConf(conf, shardedTableMapFiles);
        Assert.assertEquals("IngestJob#writeSplitsFile failed to create the expected number of rows", expectedNumRows, actualCount);
        
        Assert.assertTrue(fs.exists(file));
        return file;
    }
    
    private FileSystem setWorkingDirectory(Configuration conf) throws IOException {
        FileSystem fs = FileSystem.getLocal(conf);
        File tempWorkDir = temporaryFolder.newFolder();
        fs.setWorkingDirectory(new Path(tempWorkDir.toString()));
        conf.set(ShardedTableMapFile.SPLIT_WORK_DIR, tempWorkDir.toString());
        return fs;
    }
    
    @Test(expected = IOException.class)
    public void testGetAllShardedTableMapFilesWithoutPath() throws Exception {
        Configuration conf = new Configuration();
        File tempWorkDir = temporaryFolder.newFolder();
        conf.set(FileSystem.FS_DEFAULT_NAME_KEY, tempWorkDir.toURI().toString());
        FileSystem fs = FileSystem.get(tempWorkDir.toURI(), conf);
        fs.setWorkingDirectory(new Path(tempWorkDir.toString()));
        Path workDir = fs.makeQualified(new Path("work"));
        conf.set(ShardedTableMapFile.SPLIT_WORK_DIR, workDir.toString());
        
        conf.set(ShardedDataTypeHandler.SHARDED_TNAMES, "shard_ingest_unit_test_table_1,shard_ingest_unit_test_table_2,shard_ingest_unit_test_table_3");
        
        String[] tableNames = new String[] {TABLE_NAME};
        conf.set(ShardedTableMapFile.TABLE_NAMES, StringUtils.join(",", tableNames));
        ShardedTableMapFile.setupFile(conf);
        ShardedTableMapFile.getShardIdToLocations(conf, TABLE_NAME);
    }
    
    @Test
    public void testWriteSplitsFileNewPath() throws Exception {
        Configuration conf = new Configuration();
        Path file = createSplitsFile(new HashMap<>(), conf, 0);
        
        SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(file));
        Text key = new Text();
        Text val = new Text();
        boolean valid = reader.next(key, val);
        Assert.assertFalse(valid);
        reader.close();
    }
    
    @Test
    public void testWriteSplitsFileExistingPath() throws Exception {
        Map<Text,String> splits = new HashMap<>();
        Configuration conf = new Configuration();
        splits.put(new Text(), "hello, world!");
        
        Path file = createSplitsFile(splits, conf, 1);
        SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(file));
        Assert.assertEquals(Text.class, reader.getKeyClass());
        Assert.assertEquals(Text.class, reader.getValueClass());
        Text key = new Text();
        Text val = new Text();
        boolean valid = reader.next(key, val);
        Assert.assertTrue(valid);
        Assert.assertEquals("", key.toString());
        Assert.assertEquals("hello, world!", val.toString());
        valid = reader.next(key, val);
        Assert.assertFalse(valid);
        reader.close();
    }
    
    @Test
    public void testWriteSplitsFileExistingPathMultipleKeyExtents() throws Exception {
        Map<Text,String> splits = new HashMap<>();
        splits.put(new Text("zEndRow"), "location2_1234");
        Configuration conf = new Configuration();
        
        Path file = createSplitsFile(splits, conf, 1);
        
        SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(file));
        Assert.assertEquals(Text.class, reader.getKeyClass());
        Assert.assertEquals(Text.class, reader.getValueClass());
        Text key = new Text();
        Text val = new Text();
        boolean valid = reader.next(key, val);
        Assert.assertTrue(valid);
        Assert.assertEquals("zEndRow", key.toString());
        Assert.assertEquals("location2_1234", val.toString());
        valid = reader.next(key, val);
        Assert.assertFalse(valid);
        reader.close();
    }
    
    @Test
    public void testSingleDaySplitsCreated_AndValid() throws Exception {
        String tableName = "validSplits";
        SortedMap<Text,String> splits = createDistributedLocations(tableName);
        createSplitsFile(splits, conf, splits.size(), tableName);
        Map<Text,String> locations = ShardedTableMapFile.getShardIdToLocations(conf, tableName);
        // three days of splits, all should be good, none of these should error
        ShardedTableMapFile.validateShardIdLocations(conf, tableName, 0, locations);
        ShardedTableMapFile.validateShardIdLocations(conf, tableName, 1, locations);
        ShardedTableMapFile.validateShardIdLocations(conf, tableName, 2, locations);
    }
    
    @Test(expected = IllegalStateException.class)
    public void testMissingAllOfTodaysSplits() throws Exception {
        String tableName = "missingTodaysSplits";
        SortedMap<Text,String> splits = simulateMissingSplitsForDay(0, tableName);
        createSplitsFile(splits, conf, splits.size(), tableName);
        Map<Text,String> locations = ShardedTableMapFile.getShardIdToLocations(conf, tableName);
        // three days of splits, today should be invalid, which makes the rest bad too
        ShardedTableMapFile.validateShardIdLocations(conf, tableName, 0, locations);
        // shouldn't make it here
        fail();
    }
    
    @Test(expected = IllegalStateException.class)
    public void testUnbalancedTodaysSplits() throws Exception {
        String tableName = "unbalancedTodaysSplits";
        SortedMap<Text,String> splits = simulateUnbalancedSplitsForDay(0, tableName);
        createSplitsFile(splits, conf, splits.size(), tableName);
        Map<Text,String> locations = ShardedTableMapFile.getShardIdToLocations(conf, tableName);
        // three days of splits, today should be invalid, which makes the rest bad too
        ShardedTableMapFile.validateShardIdLocations(conf, tableName, 0, locations);
    }
    
    @Test(expected = IllegalStateException.class)
    public void testMissingAllOfYesterdaysSplits() throws Exception {
        String tableName = "missingYesterdaysSplits";
        SortedMap<Text,String> splits = simulateMissingSplitsForDay(1, tableName);
        createSplitsFile(splits, conf, splits.size(), tableName);
        Map<Text,String> locations = ShardedTableMapFile.getShardIdToLocations(conf, tableName);
        assertThat(splits.size(), is(equalTo(locations.size())));
        // three days of splits, today should be valid
        // yesterday and all other days invalid
        ShardedTableMapFile.validateShardIdLocations(conf, tableName, 0, locations);
        // this should cause the exception
        ShardedTableMapFile.validateShardIdLocations(conf, tableName, 1, locations);
    }
    
    @Test(expected = IllegalStateException.class)
    public void testUnbalancedYesterdaysSplits() throws Exception {
        String tableName = "unbalancedYesterdaysSplits";
        SortedMap<Text,String> splits = simulateUnbalancedSplitsForDay(1, tableName);
        createSplitsFile(splits, conf, splits.size(), tableName);
        Map<Text,String> locations = ShardedTableMapFile.getShardIdToLocations(conf, tableName);
        // three days of splits, today should be valid
        // yesterday and all other days invalid
        ShardedTableMapFile.validateShardIdLocations(conf, tableName, 0, locations);
        // this should cause the exception
        ShardedTableMapFile.validateShardIdLocations(conf, tableName, 1, locations);
    }
    
    @Test(expected = IllegalStateException.class)
    public void testUnbalancedMaxMoreThanConfigured() throws Exception {
        String tableName = "unbalancedMoreSplitsThenMaxPer";
        SortedMap<Text,String> splits = simulateMultipleShardsPerTServer(tableName, 3);
        conf.setInt(ShardedTableMapFile.MAX_SHARDS_PER_TSERVER, 2);
        
        createSplitsFile(splits, conf, splits.size(), tableName);
        Map<Text,String> locations = ShardedTableMapFile.getShardIdToLocations(conf, tableName);
        // this should cause the exception
        ShardedTableMapFile.validateShardIdLocations(conf, tableName, 0, locations);
    }
    
    @Test
    public void testUnbalancedButNotMoreThanConfigured() throws Exception {
        String tableName = "unbalancedNotMoreSplitsThenMaxPer";
        SortedMap<Text,String> splits = simulateMultipleShardsPerTServer(tableName, 3);
        conf.setInt(ShardedTableMapFile.MAX_SHARDS_PER_TSERVER, 3);
        
        createSplitsFile(splits, conf, splits.size(), tableName);
        Map<Text,String> locations = ShardedTableMapFile.getShardIdToLocations(conf, tableName);
        // this should NOT cause an exception
        ShardedTableMapFile.validateShardIdLocations(conf, tableName, 0, locations);
    }
    
    private SortedMap<Text,String> simulateUnbalancedSplitsForDay(int daysAgo, String tableName) throws IOException {
        // start with a well distributed set of shards per day for 3 days
        SortedMap<Text,String> locations = createDistributedLocations(tableName);
        // for shards from "daysAgo", peg them to first shard
        String tserverId = "1";
        String date = DateHelper.format(System.currentTimeMillis() - (daysAgo * DateUtils.MILLIS_PER_DAY));
        for (int currShard = 0; currShard < SHARDS_PER_DAY; currShard++) {
            locations.put(new Text(date + "_" + currShard), tserverId);
        }
        
        return locations;
    }
    
    private SortedMap<Text,String> simulateMultipleShardsPerTServer(String tableName, int shardsPerTServer) throws IOException {
        SortedMap<Text,String> locations = new TreeMap<>();
        long now = System.currentTimeMillis();
        int tserverId = 1;
        for (int daysAgo = 0; daysAgo <= 2; daysAgo++) {
            String day = DateHelper.format(now - (daysAgo * DateUtils.MILLIS_PER_DAY));
            
            int currShard = 0;
            while (currShard < SHARDS_PER_DAY) {
                // increment once, apply this tserver shardsPerTServer times
                tserverId++;
                for (int i = 0; i < shardsPerTServer; i++) {
                    if (currShard >= SHARDS_PER_DAY) {
                        break;
                    }
                    locations.put(new Text(day + "_" + currShard++), Integer.toString(tserverId));
                }
            }
        }
        return locations;
    }
    
    private SortedMap<Text,String> simulateMissingSplitsForDay(int daysAgo, String tableName) throws IOException {
        // start with a well distributed set of shards per day for 3 days
        SortedMap<Text,String> locations = createDistributedLocations(tableName);
        // for shards from "daysAgo", remove them
        String day = DateHelper.format(System.currentTimeMillis() - (daysAgo * DateUtils.MILLIS_PER_DAY));
        for (int currShard = 0; currShard < SHARDS_PER_DAY; currShard++) {
            locations.remove(new Text(day + "_" + currShard));
        }
        
        return locations;
    }
    
    private SortedMap<Text,String> createDistributedLocations(String tableName) {
        SortedMap<Text,String> locations = new TreeMap<>();
        long now = System.currentTimeMillis();
        int tserverId = 1;
        for (int daysAgo = 0; daysAgo <= 2; daysAgo++) {
            String day = DateHelper.format(now - (daysAgo * DateUtils.MILLIS_PER_DAY));
            for (int currShard = 0; currShard < SHARDS_PER_DAY; currShard++) {
                locations.put(new Text(day + "_" + currShard), Integer.toString(tserverId++));
            }
        }
        return locations;
    }
    
    private static String formatDay(int daysBack) {
        return DateHelper.format(System.currentTimeMillis() - (daysBack * DateUtils.MILLIS_PER_DAY));
    }
}
