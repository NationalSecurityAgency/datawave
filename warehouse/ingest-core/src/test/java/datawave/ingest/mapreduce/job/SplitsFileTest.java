package datawave.ingest.mapreduce.job;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import datawave.ingest.mapreduce.handler.shard.ShardIdFactory;
import datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;
import datawave.util.TableName;
import datawave.util.time.DateHelper;

public class SplitsFileTest {

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

    @Before
    public void clearCache() {
        TableSplitsCache.getCurrentCache(conf).clear();
    }

    private void createSplitsFile(Map<Text,String> splits, Configuration conf, int expectedNumRows, String tableName) throws IOException {
        conf.set(FileSystem.FS_DEFAULT_NAME_KEY, URI.create("file:///").toString());
        conf.setLong("fs.local.block.size", 32 * 1024 * 1024);
        FileSystem fs = setWorkingDirectory(conf);

        writeBaseSplitsFile(splits, conf, tableName);
        long actualCount = splits.size();
        Map<String,Path> SplitsFiles = new HashMap<>();
        // SplitsFile.addToConf(conf, SplitsFiles);
        Assert.assertEquals("IngestJob#writeSplitsFile failed to create the expected number of rows", expectedNumRows, actualCount);

        // return file;
    }

    private void writeBaseSplitsFile(Map<Text,String> locations, Configuration conf, String tableName) throws IOException {
        File tmpBaseSplitDir = temporaryFolder.newFolder();
        String splitsFile = TableSplitsCache.DEFAULT_SPLITS_CACHE_FILE;

        Path splitsPath = new Path(tmpBaseSplitDir.getAbsolutePath() + "/" + splitsFile);
        FileSystem fs = new Path(tmpBaseSplitDir.getAbsolutePath()).getFileSystem(conf);
        // constructor that takes a created list of locations
        try (PrintStream out = new PrintStream(new BufferedOutputStream(fs.create(splitsPath)))) {

            for (Map.Entry<Text,String> e : locations.entrySet()) {
                out.println(tableName + '\t' + new String(Base64.encodeBase64(e.getKey().toString().getBytes())) + '\t' + e.getValue());
            }
        }
        Assert.assertTrue(fs.exists(splitsPath));

        conf.set(TableSplitsCache.SPLITS_CACHE_DIR, tmpBaseSplitDir.getAbsolutePath());

    }

    private FileSystem setWorkingDirectory(Configuration conf) throws IOException {
        FileSystem fs = FileSystem.getLocal(conf);
        File tempWorkDir = temporaryFolder.newFolder();
        fs.setWorkingDirectory(new Path(tempWorkDir.toString()));
        conf.set(SplitsFile.SPLIT_WORK_DIR, tempWorkDir.toString());
        return fs;
    }

    @Test(expected = IOException.class)
    public void testGetAllSplitsFilesWithoutPath() throws Exception {
        Configuration conf = new Configuration();
        File tempWorkDir = temporaryFolder.newFolder();
        conf.set(FileSystem.FS_DEFAULT_NAME_KEY, tempWorkDir.toURI().toString());
        FileSystem fs = FileSystem.get(tempWorkDir.toURI(), conf);
        fs.setWorkingDirectory(new Path(tempWorkDir.toString()));
        Path workDir = fs.makeQualified(new Path("work"));
        conf.set(SplitsFile.SPLIT_WORK_DIR, workDir.toString());

        conf.set(ShardedDataTypeHandler.SHARDED_TNAMES, "shard_ingest_unit_test_table_1,shard_ingest_unit_test_table_2,shard_ingest_unit_test_table_3");

        String[] tableNames = new String[] {TABLE_NAME};
        SplitsFile.setupFile(conf);
        SplitsFile.getSplitsAndLocations(conf, TABLE_NAME);
    }

    @Test
    public void testSingleDaySplitsCreated_AndValid() throws Exception {
        String tableName = "validSplits";
        SortedMap<Text,String> splits = createDistributedLocations(tableName);
        createSplitsFile(splits, conf, splits.size(), tableName);
        Map<Text,String> locations = SplitsFile.getSplitsAndLocations(conf, tableName);
        // three days of splits, all should be good, none of these should error
        SplitsFile.validateShardIdLocations(conf, tableName, 0, locations);
        SplitsFile.validateShardIdLocations(conf, tableName, 1, locations);
        SplitsFile.validateShardIdLocations(conf, tableName, 2, locations);
    }

    @Test(expected = IllegalStateException.class)
    public void testMissingAllOfTodaysSplits() throws Exception {
        String tableName = "missingTodaysSplits";
        SortedMap<Text,String> splits = simulateMissingSplitsForDay(0, tableName);
        createSplitsFile(splits, conf, splits.size(), tableName);
        Map<Text,String> locations = SplitsFile.getSplitsAndLocations(conf, tableName);
        // three days of splits, today should be invalid, which makes the rest bad too
        SplitsFile.validateShardIdLocations(conf, tableName, 0, locations);
        // shouldn't make it here
        fail();
    }

    @Test(expected = IllegalStateException.class)
    public void testUnbalancedTodaysSplits() throws Exception {
        String tableName = "unbalancedTodaysSplits";
        SortedMap<Text,String> splits = simulateUnbalancedSplitsForDay(0, tableName);
        createSplitsFile(splits, conf, splits.size(), tableName);
        Map<Text,String> locations = SplitsFile.getSplitsAndLocations(conf, tableName);
        // three days of splits, today should be invalid, which makes the rest bad too
        SplitsFile.validateShardIdLocations(conf, tableName, 0, locations);
    }

    @Test(expected = IllegalStateException.class)
    public void testMissingAllOfYesterdaysSplits() throws Exception {
        String tableName = "missingYesterdaysSplits";
        SortedMap<Text,String> splits = simulateMissingSplitsForDay(1, tableName);
        createSplitsFile(splits, conf, splits.size(), tableName);
        Map<Text,String> locations = SplitsFile.getSplitsAndLocations(conf, tableName);
        assertThat(splits.size(), is(equalTo(locations.size())));
        // three days of splits, today should be valid
        // yesterday and all other days invalid
        SplitsFile.validateShardIdLocations(conf, tableName, 0, locations);
        // this should cause the exception
        SplitsFile.validateShardIdLocations(conf, tableName, 1, locations);
    }

    @Test(expected = IllegalStateException.class)
    public void testUnbalancedYesterdaysSplits() throws Exception {
        String tableName = "unbalancedYesterdaysSplits";
        SortedMap<Text,String> splits = simulateUnbalancedSplitsForDay(1, tableName);
        createSplitsFile(splits, conf, splits.size(), tableName);
        Map<Text,String> locations = SplitsFile.getSplitsAndLocations(conf, tableName);
        // three days of splits, today should be valid
        // yesterday and all other days invalid
        SplitsFile.validateShardIdLocations(conf, tableName, 0, locations);
        // this should cause the exception
        SplitsFile.validateShardIdLocations(conf, tableName, 1, locations);
    }

    @Test(expected = IllegalStateException.class)
    public void testUnbalancedMaxMoreThanConfigured() throws Exception {
        String tableName = "unbalancedMoreSplitsThenMaxPer";
        SortedMap<Text,String> splits = simulateMultipleShardsPerTServer(tableName, 3);
        conf.setInt(SplitsFile.MAX_SHARDS_PER_TSERVER, 2);

        createSplitsFile(splits, conf, splits.size(), tableName);
        Map<Text,String> locations = SplitsFile.getSplitsAndLocations(conf, tableName);
        // this should cause the exception
        SplitsFile.validateShardIdLocations(conf, tableName, 0, locations);
    }

    @Test
    public void testUnbalancedButNotMoreThanConfigured() throws Exception {
        String tableName = "unbalancedNotMoreSplitsThenMaxPer";
        SortedMap<Text,String> splits = simulateMultipleShardsPerTServer(tableName, 3);
        conf.setInt(SplitsFile.MAX_SHARDS_PER_TSERVER, 3);

        createSplitsFile(splits, conf, splits.size(), tableName);
        Map<Text,String> locations = SplitsFile.getSplitsAndLocations(conf, tableName);
        // this should NOT cause an exception
        SplitsFile.validateShardIdLocations(conf, tableName, 0, locations);
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

}
