package datawave.ingest.mapreduce.job;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.accumulo.core.data.LoadPlan;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
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
        SplitsFile.setupFile(Job.getInstance(conf), conf);
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

    @Test
    public void testPlanning() {
        SortedSet<Text> rfileRows = new TreeSet<>();
        rfileRows.add(new Text("20160602_0"));
        rfileRows.add(new Text("20170601_0"));
        rfileRows.add(new Text("20170601_1"));
        rfileRows.add(new Text("20170602_1"));
        rfileRows.add(new Text("20170602_0a1"));
        rfileRows.add(new Text("20170602_0a11"));
        rfileRows.add(new Text("20170602_0a111"));
        rfileRows.add(new Text("20170602_0b1"));
        rfileRows.add(new Text("20170602_0c1"));
        rfileRows.add(new Text("20170603_0"));
        rfileRows.add(new Text("20170603_0a11"));
        rfileRows.add(new Text("20170603_0a12"));
        rfileRows.add(new Text("20170603_0b"));
        rfileRows.add(new Text("20170603_0c"));
        rfileRows.add(new Text("20170603_0d"));
        rfileRows.add(new Text("20170601_9"));
        rfileRows.add(new Text("20200601_9"));

        Set<LoadPlan.TableSplits> expectedExtents = new HashSet<>();
        expectedExtents.add(new LoadPlan.TableSplits(new Text("20170601_0"), new Text("20170601_1")));
        expectedExtents.add(new LoadPlan.TableSplits(new Text("20170601_8"), new Text("20170601_9")));
        expectedExtents.add(new LoadPlan.TableSplits(new Text("20170602_0"), new Text("20170602_1")));
        expectedExtents.add(new LoadPlan.TableSplits(new Text("20170603_9"), null));
        expectedExtents.add(new LoadPlan.TableSplits(new Text("20170603_0c"), new Text("20170603_1")));
        expectedExtents.add(new LoadPlan.TableSplits(null, new Text("20170601_0")));
        expectedExtents.add(new LoadPlan.TableSplits(new Text("20170602_9c"), new Text("20170603_0")));
        expectedExtents.add(new LoadPlan.TableSplits(new Text("20170603_0a"), new Text("20170603_0b")));
        expectedExtents.add(new LoadPlan.TableSplits(new Text("20170603_0b"), new Text("20170603_0c")));

        var splitResolver = SplitsFile.createSplitResolver(getSplits());
        Set<LoadPlan.TableSplits> extents = rfileRows.stream().map(splitResolver).collect(Collectors.toCollection(HashSet::new));

        assertEquals(expectedExtents, extents);
    }

    private ArrayList<Text> getSplits() {
        var arr = new ArrayList<Text>();
        arr.add(new Text("20170601_0")); // 0
        arr.add(new Text("20170601_1")); // 1
        arr.add(new Text("20170601_2")); // 2
        arr.add(new Text("20170601_3")); // 3
        arr.add(new Text("20170601_4")); // 4
        arr.add(new Text("20170601_5")); // 5
        arr.add(new Text("20170601_6")); // 6
        arr.add(new Text("20170601_7")); // 7
        arr.add(new Text("20170601_8")); // 8
        arr.add(new Text("20170601_9")); // 9
        arr.add(new Text("20170602_0")); // 10
        arr.add(new Text("20170602_1")); // 11
        arr.add(new Text("20170602_2")); // 12
        arr.add(new Text("20170602_3")); // 13
        arr.add(new Text("20170602_4")); // 14
        arr.add(new Text("20170602_5")); // 15
        arr.add(new Text("20170602_6")); // 16
        arr.add(new Text("20170602_7")); // 17
        arr.add(new Text("20170602_8")); // 18
        arr.add(new Text("20170602_9")); // 19
        arr.add(new Text("20170602_9a")); // 20
        arr.add(new Text("20170602_9b")); // 21
        arr.add(new Text("20170602_9c")); // 22
        arr.add(new Text("20170603_0")); // 23
        arr.add(new Text("20170603_0a")); // 24
        arr.add(new Text("20170603_0b")); // 25
        arr.add(new Text("20170603_0c")); // 26
        arr.add(new Text("20170603_1")); // 27
        arr.add(new Text("20170603_2")); // 28
        arr.add(new Text("20170603_3")); // 29
        arr.add(new Text("20170603_4")); // 30
        arr.add(new Text("20170603_5")); // 31
        arr.add(new Text("20170603_6")); // 32
        arr.add(new Text("20170603_7")); // 34
        arr.add(new Text("20170603_8")); // 35
        arr.add(new Text("20170603_9")); // 36
        return arr;
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
