package datawave.ingest.mapreduce.partition;

import datawave.ingest.mapreduce.job.ShardedTableMapFile;
import datawave.util.time.DateHelper;
import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

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
        String tmpDirectory = tmpDir + "/";
        writeSplits(locations, tmpDirectory, BALANCEDISH_SHARDS_LST);
        registerSplitsFileForShardTable(tmpDirectory, BALANCEDISH_SHARDS_LST, tableNames);
    }
    
    public TestShardGenerator(Configuration conf, File tmpDir, Map<Text,String> locations, String... tableNames) throws IOException {
        this.conf = conf;
        // constructor that takes a created list of locations
        String tmpDirectory = tmpDir + "/";
        writeSplits(locations, tmpDirectory, BALANCEDISH_SHARDS_LST);
        registerSplitsFileForShardTable(tmpDirectory, BALANCEDISH_SHARDS_LST, tableNames);
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
                    locations.put(new Text(today + "_" + currShard), tserver);
                }
            }
        }
        return locations;
    }
    
    private void writeSplits(Map<Text,String> locations, String directory, String fileName) throws IOException {
        Path shardedMapFile = new Path(directory, fileName);
        ShardedTableMapFile.writeSplitsFileLegacy(locations, shardedMapFile, conf);
    }
    
    private void registerSplitsFileForShardTable(String directory, String fileName, String... tableNames) {
        Map<String,Path> shardedTableMapFiles = new HashMap<>();
        for (String tableName : tableNames) {
            shardedTableMapFiles.put(tableName, new Path(directory + fileName));
        }
        ShardedTableMapFile.addToConf(conf, shardedTableMapFiles);
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
