package datawave.ingest.mapreduce.job;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.google.common.collect.Lists;

import datawave.ingest.mapreduce.handler.shard.ShardIdFactory;
import datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;
import datawave.table.constants.TableName;
import datawave.util.time.DateHelper;

public class SplitsFileTest {

    private static final String TABLE_NAME = "unitTestTable";
    private static final int SHARDS_PER_DAY = 10;
    private static Configuration conf;
    private SplitsFile uut;

    @TempDir
    private java.nio.file.Path tempDir;

    @BeforeAll
    public static void defineShardLocationsFile() throws IOException {
        conf = new Configuration();
        conf.setInt(ShardIdFactory.NUM_SHARDS, SHARDS_PER_DAY);
        conf.set(ShardedDataTypeHandler.SHARDED_TNAMES, TableName.SHARD);
    }

    @BeforeEach
    public void setUp() throws IOException {
        // Wipe the underlying cache that the SplitsFile uses
        // state is saved that will impact multiple test iterations
        TableSplitsCache.clear();
        uut = new SplitsFile();
    }

    private void createSplitsFile(Map<Text,String> splits, Configuration conf, int expectedNumRows, String tableName) throws IOException {
        conf.set(FileSystem.FS_DEFAULT_NAME_KEY, URI.create("file:///").toString());
        conf.setLong("fs.local.block.size", 32 * 1024 * 1024);
        FileSystem fs = setWorkingDirectory(conf);

        writeBaseSplitsFile(splits, conf, tableName);
        long actualCount = splits.size();
        assertEquals(expectedNumRows, actualCount, "IngestJob#writeSplitsFile failed to create the expected number of rows");
    }

    private void writeBaseSplitsFile(Map<Text,String> locations, Configuration conf, String tableName) throws IOException {
        File tmpBaseSplitDir = tempDir.toFile();
        String splitsFile = TableSplitsCache.DEFAULT_SPLITS_CACHE_FILE;

        Path splitsPath = new Path(tmpBaseSplitDir.getAbsolutePath() + "/" + splitsFile);
        FileSystem fs = new Path(tmpBaseSplitDir.getAbsolutePath()).getFileSystem(conf);
        conf.set(SplitsConstants.SPLITS_CACHE_DIR, tmpBaseSplitDir.getAbsolutePath());

        // constructor that takes a created list of locations
        try (PrintStream out = new PrintStream(new BufferedOutputStream(fs.create(splitsPath)))) {
            TableSplitsCache.getCurrentCache(conf).writeHeaderLine(out, Collections.singleton(tableName));
            TableSplitsCache.getCurrentCache(conf).writeLocations(out, tableName, Lists.newArrayList(locations.keySet()), locations);
        }
        assertTrue(fs.exists(splitsPath));

        conf.set(SplitsConstants.SPLITS_CACHE_DIR, tmpBaseSplitDir.getAbsolutePath());

    }

    private FileSystem setWorkingDirectory(Configuration conf) throws IOException {
        FileSystem fs = FileSystem.getLocal(conf);
        File tempWorkDir = tempDir.toFile();
        fs.setWorkingDirectory(new Path(tempWorkDir.toString()));
        conf.set(SplitsFile.SPLIT_WORK_DIR, tempWorkDir.toString());
        return fs;
    }

    @Test
    public void testGetAllSplitsFilesWithoutPath() throws Exception {
        Configuration conf = new Configuration();
        File tempWorkDir = tempDir.toFile();
        conf.set(FileSystem.FS_DEFAULT_NAME_KEY, tempWorkDir.toURI().toString());
        FileSystem fs = FileSystem.get(tempWorkDir.toURI(), conf);
        fs.setWorkingDirectory(new Path(tempWorkDir.toString()));
        Path workDir = fs.makeQualified(new Path("work"));
        conf.set(SplitsFile.SPLIT_WORK_DIR, workDir.toString());
        conf.set(ShardedDataTypeHandler.SHARDED_TNAMES, "shard_ingest_unit_test_table_1,shard_ingest_unit_test_table_2,shard_ingest_unit_test_table_3");

        uut.init(conf);

        assertThrows(IOException.class, () -> uut.setupJob(Job.getInstance(conf), conf));
    }

    @Test
    public void testSingleDaySplitsCreated_AndValid() throws Exception {
        String tableName = "validSplits";
        SortedMap<Text,String> splits = createDistributedLocations(tableName);
        createSplitsFile(splits, conf, splits.size(), tableName);

        uut.init(conf);

        Map<Text,String> locations = uut.getSplitsAndLocations(tableName);
        // three days of splits, all should be good, none of these should error
        uut.validateShardIdLocations(conf, tableName, 0, locations);
        uut.validateShardIdLocations(conf, tableName, 1, locations);
        uut.validateShardIdLocations(conf, tableName, 2, locations);
    }

    @Test
    public void testMissingAllOfTodaysSplits() throws Exception {
        String tableName = "missingTodaysSplits";
        SortedMap<Text,String> splits = simulateMissingSplitsForDay(0, tableName);
        createSplitsFile(splits, conf, splits.size(), tableName);

        uut.init(conf);

        Map<Text,String> locations = uut.getSplitsAndLocations(tableName);
        // three days of splits, today should be invalid, which makes the rest bad too
        assertThrows(IllegalStateException.class, () -> uut.validateShardIdLocations(conf, tableName, 0, locations));
    }

    @Test
    public void testUnbalancedTodaysSplits() throws Exception {
        String tableName = "unbalancedTodaysSplits";
        SortedMap<Text,String> splits = simulateUnbalancedSplitsForDay(0, tableName);
        createSplitsFile(splits, conf, splits.size(), tableName);

        uut.init(conf);

        Map<Text,String> locations = uut.getSplitsAndLocations(tableName);
        // three days of splits, today should be invalid, which makes the rest bad too
        assertThrows(IllegalStateException.class, () -> uut.validateShardIdLocations(conf, tableName, 0, locations));
    }

    @Test
    public void testMissingAllOfYesterdaysSplits() throws Exception {
        String tableName = "missingYesterdaysSplits";
        SortedMap<Text,String> splits = simulateMissingSplitsForDay(1, tableName);
        createSplitsFile(splits, conf, splits.size(), tableName);

        uut.init(conf);

        Map<Text,String> locations = uut.getSplitsAndLocations(tableName);
        assertEquals(splits.size(), locations.size());
        // three days of splits, today should be valid
        // yesterday and all other days invalid
        uut.validateShardIdLocations(conf, tableName, 0, locations);
        // this should cause the exception
        assertThrows(IllegalStateException.class, () -> uut.validateShardIdLocations(conf, tableName, 1, locations));
    }

    @Test
    public void testUnbalancedYesterdaysSplits() throws Exception {
        String tableName = "unbalancedYesterdaysSplits";
        SortedMap<Text,String> splits = simulateUnbalancedSplitsForDay(1, tableName);
        createSplitsFile(splits, conf, splits.size(), tableName);

        uut.init(conf);

        Map<Text,String> locations = uut.getSplitsAndLocations(tableName);
        // three days of splits, today should be valid
        // yesterday and all other days invalid
        uut.validateShardIdLocations(conf, tableName, 0, locations);
        // this should cause the exception
        assertThrows(IllegalStateException.class, () -> uut.validateShardIdLocations(conf, tableName, 1, locations));
    }

    @Test
    public void testUnbalancedMaxMoreThanConfigured() throws Exception {
        String tableName = "unbalancedMoreSplitsThenMaxPer";
        SortedMap<Text,String> splits = simulateMultipleShardsPerTServer(tableName, 3);
        conf.setInt(SplitsFile.MAX_SHARDS_PER_TSERVER, 2);
        createSplitsFile(splits, conf, splits.size(), tableName);

        uut.init(conf);

        Map<Text,String> locations = uut.getSplitsAndLocations(tableName);
        // this should cause the exception
        assertThrows(IllegalStateException.class, () -> uut.validateShardIdLocations(conf, tableName, 0, locations));
    }

    @Test
    public void testUnbalancedButNotMoreThanConfigured() throws Exception {
        String tableName = "unbalancedNotMoreSplitsThenMaxPer";
        SortedMap<Text,String> splits = simulateMultipleShardsPerTServer(tableName, 3);
        conf.setInt(SplitsFile.MAX_SHARDS_PER_TSERVER, 3);
        createSplitsFile(splits, conf, splits.size(), tableName);

        uut.init(conf);

        Map<Text,String> locations = uut.getSplitsAndLocations(tableName);
        // this should NOT cause an exception
        uut.validateShardIdLocations(conf, tableName, 0, locations);
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
