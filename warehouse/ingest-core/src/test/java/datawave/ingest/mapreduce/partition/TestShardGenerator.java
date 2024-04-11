package datawave.ingest.mapreduce.partition;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import datawave.ingest.mapreduce.job.TableSplitsCache;
import datawave.util.time.DateHelper;

/**
 * Creates a splits file for the shard table that's relatively balanced
 */
public class TestShardGenerator {
    private int numDays;
    private int shardsPerDay;
    private int totalTservers;
    private int daysWithoutCollisions;
    private Configuration conf;
    private Random randomishGenerator = new Random(System.currentTimeMillis());
    ArrayList<String> availableTServers;

    private static final String BALANCEDISH_SHARDS_LST = "balancedish_shards.lst";

    public TestShardGenerator(Configuration conf, File tmpDir, int numDays, int shardsPerDay, int totalTservers, String... tableNames) throws IOException {
        this.conf = conf;
        this.numDays = numDays;
        this.shardsPerDay = shardsPerDay;
        this.totalTservers = totalTservers;
        this.daysWithoutCollisions = totalTservers / shardsPerDay;
        registerSomeTServers();
        Map<Text,String> locations = simulateTabletAssignments(tableNames);
        // adding sorting here since it now happens when we generate the splits file
        conf.set(TableSplitsCache.SPLITS_CACHE_DIR, tmpDir.getAbsolutePath());
        TableSplitsCache.getCurrentCache(conf).clear();
        Map<Text,String> sortedLocations = TableSplitsCache.getCurrentCache(conf).reverseSortByShardIds(locations);
        String tmpDirectory = tmpDir + "/";
        Path splitsPath = new Path(tmpDir.getAbsolutePath() + "/all-splits.txt");

        FileSystem fs = new Path(tmpDir.getAbsolutePath()).getFileSystem(conf);
        // constructor that takes a created list of locations
        try (PrintStream out = new PrintStream(new BufferedOutputStream(fs.create(splitsPath)))) {

            for (String table : tableNames) {

                for (Map.Entry<Text,String> entry : sortedLocations.entrySet()) {
                    out.println(table + "\t" + new String(Base64.encodeBase64(entry.getKey().toString().getBytes())).trim() + "\t" + entry.getValue());
                }
            }
        }

        // writeSplits(locations, tmpDirectory, BALANCEDISH_SHARDS_LST);
    }

    public TestShardGenerator(Configuration conf, File tmpDir, Map<Text,String> locations, String... tableNames) throws IOException {
        this.conf = conf;
        TableSplitsCache.getCurrentCache(conf).clear();
        FileSystem fs = new Path(tmpDir.getAbsolutePath()).getFileSystem(conf);
        // constructor that takes a created list of locations
        String tmpDirectory = tmpDir + "/";
        Path splitsPath = new Path(tmpDir.getAbsolutePath() + "/all-splits.txt");
        conf.set(TableSplitsCache.SPLITS_CACHE_DIR, tmpDir.getAbsolutePath());
        Map<Text,String> sortedLocations = TableSplitsCache.getCurrentCache(conf).reverseSortByShardIds(locations);

        try (PrintStream out = new PrintStream(new BufferedOutputStream(fs.create(splitsPath)))) {
            for (String table : tableNames) {

                for (Map.Entry<Text,String> entry : sortedLocations.entrySet()) {
                    out.println(table + "\t" + new String(Base64.encodeBase64(entry.getKey().toString().getBytes())) + "\t" + entry.getValue());
                }
            }
        }
        conf.set(TableSplitsCache.SPLITS_CACHE_DIR, tmpDir.getAbsolutePath());

        // writeSplits(locations, tmpDirectory, BALANCEDISH_SHARDS_LST);
    }

    // create splits for all the shards from today back NUM_DAYS,
    // creating SHARDS_PER_DAY
    private Map<Text,String> simulateTabletAssignments(String[] tableNames) {
        Map<Text,String> locations = new TreeMap<>();
        long now = System.currentTimeMillis();

        HashSet<String> alreadyUsed = new HashSet<>();
        int daysInGroup = 1;
        for (int daysAgo = -2; daysAgo < numDays - 1; daysAgo++) {
            if (daysInGroup % daysWithoutCollisions == 0) {
                alreadyUsed = new HashSet<>();
            }
            daysInGroup++;
            String today = DateHelper.format(now - (daysAgo * DateUtils.MILLIS_PER_DAY));

            for (int currShard = 1; currShard < shardsPerDay; currShard++) {
                // don't assign the same tserver to two shards within one day
                String tserver = getAnotherRandomTserver();
                while (alreadyUsed.contains(tserver)) {
                    tserver = getAnotherRandomTserver();
                }
                alreadyUsed.add(tserver);
                for (String tableName : tableNames) {
                    locations.put(new Text((today + "_" + currShard).trim()), tserver);
                }
            }
        }
        return locations;
    }

    private String getAnotherRandomTserver() {
        int randomIndex = Math.abs(randomishGenerator.nextInt()) % totalTservers;
        return availableTServers.get(randomIndex);
    }

    private void registerSomeTServers() {
        availableTServers = new ArrayList<>();
        for (int i = 0; i < totalTservers; i++) {
            availableTServers.add("" + i);
        }
    }
}
