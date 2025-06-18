package datawave.ingest.mapreduce.job;

import static datawave.ingest.mapreduce.job.SplitsConstants.SPLITS_CACHE_DIR;
import static datawave.ingest.mapreduce.job.SplitsConstants.SPLITS_CACHE_FILE;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.ingest.mapreduce.handler.shard.ShardIdFactory;
import datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;
import datawave.util.time.DateHelper;

public class SplitsFile implements SplitsCache {
    private static final Logger log = LoggerFactory.getLogger(SplitsFile.class);

    public static final String SPLIT_WORK_DIR = "split.work.dir";
    public static final String MAX_SHARDS_PER_TSERVER = "shardedMap.max.shards.per.tserver";
    public static final String SHARD_VALIDATION_ENABLED = "shardedMap.validation.enabled";
    public static final String SHARDS_BALANCED_DAYS_TO_VERIFY = "shards.balanced.days.to.verify";
    public static final String CONFIGURED_SHARDED_TABLE_NAMES = ShardedDataTypeHandler.SHARDED_TNAMES + ".configured";
    public static final String DIST_CACHE_LABEL = "splitsFile";

    private static final int NUMBER_MILLIS_BACK = 0;
    private static final String TODAY = DateHelper.format(NUMBER_MILLIS_BACK);

    private final ConcurrentHashMap<String,Map<Text,Integer>> shardPartitionsByTable;
    private TableSplitsCache instance;
    private Configuration conf;

    public SplitsFile() {
        this.shardPartitionsByTable = new ConcurrentHashMap<>();
    }

    @Override
    public void init(Configuration conf) {
        this.conf = conf;
        this.instance = TableSplitsCache.getCurrentCache(conf);
    }

    @Override
    public void setupJob(Job job) throws IOException {

        Path baseSplitsPath = TableSplitsCache.getSplitsPath(conf);
        FileSystem sourceFs = baseSplitsPath.getFileSystem(conf);
        FileSystem destFs = new Path(conf.get(SPLIT_WORK_DIR)).getFileSystem(conf);

        // want validation turned off by default
        boolean doValidation = conf.getBoolean(SHARD_VALIDATION_ENABLED, false);

        try {
            log.info("Base splits: " + baseSplitsPath);

            Path destSplits = new Path(conf.get(SPLIT_WORK_DIR) + "/" + conf.get(SPLITS_CACHE_FILE, TableSplitsCache.DEFAULT_SPLITS_CACHE_FILE));
            log.info("Dest splits: " + destSplits);

            FileUtil.copy(sourceFs, baseSplitsPath, destFs, destSplits, false, conf);
            conf.set(SPLITS_CACHE_DIR, conf.get(SPLIT_WORK_DIR));

            // // if we want the freshest splits, go ahead and update them in the work dir
            // if (TableSplitsCache.shouldRefreshSplits(conf)) {
            // TableSplitsCache.getCurrentCache(conf).update();
            // }

            job.addCacheFile(new URI(destSplits.toString() + "#" + DIST_CACHE_LABEL));

            if (doValidation) {
                validate(conf);
            }

        } catch (Exception e) {
            log.error("Unable to use splits file because " + e.getMessage());
            throw new IOException(e);
        }
    }

    @Override
    public void close() {
        // no code
    }

    @Override
    public boolean hasSplits() {
        try {
            return !instance.getSplits().isEmpty();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public String getExactLocation(String table, Text key, Supplier<String> defaultFn) {
        String location;
        try {
            Map<Text,String> locationByTable = instance.getSplitsAndLocationByTable(table);
            String keyValue = locationByTable.get(key);
            location = keyValue == null ? defaultFn.get() : keyValue;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return location;
    }

    @Override
    public int getExactIndex(String table, Text key) {
        List<Text> splits;
        try {
            splits = instance.getSplits(table);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return splits != null ? Collections.binarySearch(splits, key) : -1;
    }

    @Override
    public int getExactPartition(String tableName, Text shardId) {
        Map<Text,Integer> assignments;
        try {
            assignments = lazilyCreateAssignments(tableName);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        Integer partitionId = assignments.get(shardId);
        if (partitionId != null) {
            return partitionId;
        }
        return -1;
    }

    @Override
    public int getNearestPartition(String tableName, Text shardId) {
        Map<Text,Integer> assignments = null;
        try {
            assignments = lazilyCreateAssignments(tableName);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        Integer partitionId = assignments.get(shardId);
        if (partitionId != null) {
            return partitionId;
        }

        List<Text> keys = new ArrayList<>(assignments.keySet());
        Collections.sort(keys);
        int closestAssignment = Collections.binarySearch(keys, shardId);
        if (closestAssignment >= 0) {
            log.warn("Something is screwy, found {} on the second try", shardId);
            return assignments.get(shardId);
        }
        // insertion point in the index of the key greater
        Text shardString = keys.get(Math.abs(closestAssignment + 1));
        return assignments.get(shardString);
    }

    public void validate(Configuration conf) throws IOException {
        TableSplitsCache cache = TableSplitsCache.getCurrentCache(conf);
        int daysToVerify = conf.getInt(SHARDS_BALANCED_DAYS_TO_VERIFY, 2) - 1;

        for (String table : conf.getStrings(ShardedDataTypeHandler.SHARDED_TNAMES)) {
            validateShardIdLocations(conf, table, daysToVerify, cache.getSplitsAndLocationByTable(table));
        }

    }

    public void validateShardIdLocations(Configuration conf, String tableName, int daysToVerify, Map<Text,String> shardIdToLocation) {
        ShardIdFactory shardIdFactory = new ShardIdFactory(conf);
        // assume true unless proven otherwise
        boolean isValid = true;
        int maxShardsPerTserver = conf.getInt(MAX_SHARDS_PER_TSERVER, 1);

        for (int daysAgo = 0; daysAgo <= daysToVerify; daysAgo++) {
            long inMillis = System.currentTimeMillis() - (daysAgo * DateUtils.MILLIS_PER_DAY);
            String datePrefix = DateHelper.format(inMillis);
            int expectedNumberOfShards = shardIdFactory.getNumShards(datePrefix);
            boolean shardsExist = shardsExistForDate(shardIdToLocation, datePrefix, expectedNumberOfShards);
            if (!shardsExist) {
                log.error("Shards for " + datePrefix + " for table " + tableName + " do not exist!");
                isValid = false;
                continue;
            }
            boolean shardsAreBalanced = shardsAreBalanced(shardIdToLocation, datePrefix, maxShardsPerTserver);
            if (!shardsAreBalanced) {
                log.error("Shards for " + datePrefix + " for table " + tableName + " are not balanced!");
                isValid = false;
            }
        }
        if (!isValid) {
            throw new IllegalStateException("Shard validation failed for " + tableName + ". Please ensure that "
                            + "shards have been generated. Check log for details about the dates in question");
        }
    }

    /**
     * For a given tablename, provides the mapping from {@code shard id -> partition}
     *
     * @param tableName
     *            the name of the table
     * @return a list of the shard mappings
     * @throws IOException
     *             if there is an issue with read or write
     */
    private Map<Text,Integer> lazilyCreateAssignments(String tableName) throws IOException {
        Map<Text,Integer> assignments = this.shardPartitionsByTable.get(tableName);
        if (null == assignments) {
            assignments = getPartitionsByShardId(tableName);
            this.shardPartitionsByTable.put(tableName, assignments);
        }
        return assignments;
    }

    /**
     * Loads the splits file for the table name and uses it to assign partitions.
     *
     * @param tableName
     *            name of the table
     * @return a map of the partitions
     * @throws IOException
     *             if there is an issue with read or write
     */
    private HashMap<Text,Integer> getPartitionsByShardId(String tableName) throws IOException {
        if (log.isDebugEnabled())
            log.debug("Loading splits data for " + tableName);

        List<Text> sortedSplits = instance.getSplits(tableName);
        Map<Text,String> shardIdToLocation = instance.getSplitsAndLocationByTable(tableName);

        if (log.isDebugEnabled())
            log.debug("Assigning partitioners for each shard in " + tableName);
        return assignPartitionsForEachShard(sortedSplits, shardIdToLocation);
    }

    /**
     * 1. sorts the the tablet assignments by shard id, starting with the most recent going backwards<br>
     * 2. assigns partitions to each tservers, starting from the beginning, but skipping future dates<br>
     * 3. assigns partitions to each shardid by looking up its tserver's assignments<br>
     * 4. returns the {@code shard id -> partition} map
     * <p>
     * e.g.,<br>
     * 1. sorted assignments<br>
     * 2. tserver map<br>
     * 3. shard map: {@code future->tserver7 *no change* future->2 shard4->tserver2 tserver2->0 shard4->0
     * shard3->tserver3 tserver3->1 shard3->1 shard2->tserver2 *no change* shard2->0 shard1->tserver7 tserver7->2 shard1->2}
     *
     * @param shardIdToLocations
     *            the map of shard ids and their location
     * @return shardId to
     */
    private HashMap<Text,Integer> assignPartitionsForEachShard(List<Text> sortedShardIds, Map<Text,String> shardIdToLocations) {
        int totalNumUniqueTServers = calculateNumberOfUniqueTservers(shardIdToLocations);

        HashMap<String,Integer> partitionsByTServer = getTServerAssignments(totalNumUniqueTServers, sortedShardIds, shardIdToLocations);
        HashMap<Text,Integer> partitionsByShardId = getShardIdAssignments(shardIdToLocations, partitionsByTServer);

        if (log.isDebugEnabled())
            log.debug("Number of shardIds assigned: " + partitionsByShardId.size());

        return partitionsByShardId;
    }

    private int calculateNumberOfUniqueTservers(Map<Text,String> shardIdToLocations) {

        int totalNumUniqueTServers = new HashSet(shardIdToLocations.values()).size();
        if (log.isDebugEnabled())
            log.debug("Total TServers involved: " + totalNumUniqueTServers);
        return totalNumUniqueTServers;
    }

    private HashMap<String,Integer> getTServerAssignments(int totalNumTServers, List<Text> sortedShardIds, Map<Text,String> shardIdsToTservers) {
        HashMap<String,Integer> partitionsByTServer = new HashMap<>(totalNumTServers);
        int nextAvailableSlot = 0;
        boolean alreadySkippedFutureShards = false;
        for (Text shard : sortedShardIds) {
            if (alreadySkippedFutureShards || !isFutureShard(shard)) { // short circuiting for performance
                alreadySkippedFutureShards = true;
                String location = shardIdsToTservers.get(shard);
                Integer assignedPartition = partitionsByTServer.get(location);
                if (null == assignedPartition) {
                    assignedPartition = nextAvailableSlot;
                    partitionsByTServer.put(location, assignedPartition);
                    nextAvailableSlot++;
                }
                if (partitionsByTServer.size() == totalNumTServers) {
                    // all the tservers have been assigned partitions, so we can stop
                    return partitionsByTServer;
                }
            }
        }
        return partitionsByTServer;
    }

    private boolean isFutureShard(Text shardId) {
        String shardIdStr = shardId.toString().intern();
        if (shardIdStr.length() < 8) {
            return true;
        }
        return shardIdStr.substring(0, 8).compareTo(TODAY) > 0;
    }

    private HashMap<Text,Integer> getShardIdAssignments(Map<Text,String> shardIdsToTservers, HashMap<String,Integer> partitionsByTServer) {
        HashMap<Text,Integer> partitionsByShardId = new HashMap<>();
        for (Map.Entry<Text,String> entry : shardIdsToTservers.entrySet()) {
            partitionsByShardId.put(entry.getKey(), partitionsByTServer.get(entry.getValue()));
        }
        return partitionsByShardId;
    }

    private boolean prefixMatches(byte[] prefixBytes, byte[] keyBytes, int keyLen) {
        // if key length is less than prefix size, no use comparing
        if (prefixBytes.length > keyLen) {
            return false;
        }
        for (int i = 0; i < prefixBytes.length; i++) {
            if (prefixBytes[i] != keyBytes[i]) {
                return false;
            }
        }
        // at this point didn't fail match, so should be good
        return true;
    }

    /**
     * Existence check for the shard splits for the specified date
     *
     * @param locations
     *            mapping of shard to tablet
     * @param datePrefix
     *            to check
     * @param expectedNumberOfShards
     *            that should exist
     * @return if the number of shards for the given date are as expected
     */
    private boolean shardsExistForDate(Map<Text,String> locations, String datePrefix, int expectedNumberOfShards) {
        int count = 0;
        byte[] prefixBytes = datePrefix.getBytes();
        for (Text key : locations.keySet()) {
            if (prefixMatches(prefixBytes, key.getBytes(), key.getLength())) {
                count++;
            }
        }
        return count == expectedNumberOfShards;
    }

    /**
     * Checks that the shard splits for the given date have been assigned to unique tablets.
     *
     * @param locations
     *            mapping of shard to tablet
     * @param datePrefix
     *            to check
     * @return if the shards are distributed in a balanced fashion
     */
    private boolean shardsAreBalanced(Map<Text,String> locations, String datePrefix, int maxShardsPerTserver) {
        // assume true unless proven wrong
        boolean dateIsBalanced = true;

        Map<String,MutableInt> tabletsSeenForDate = new HashMap<>();
        byte[] prefixBytes = datePrefix.getBytes();

        for (Map.Entry<Text,String> entry : locations.entrySet()) {
            Text key = entry.getKey();
            // only check entries for specified date
            if (prefixMatches(prefixBytes, key.getBytes(), key.getLength())) {
                String value = entry.getValue();
                MutableInt cnt = tabletsSeenForDate.get(value);
                if (null == cnt) {
                    cnt = new MutableInt(0);
                }
                // increment here before checking
                cnt.increment();

                // if shard is assigned to more tservers than allowed, then the shards are not balanced
                if (cnt.intValue() > maxShardsPerTserver) {
                    log.warn(cnt.toInteger() + " Shards for " + datePrefix + " assigned to tablet " + value);
                    dateIsBalanced = false;
                }

                tabletsSeenForDate.put(value, cnt);
            }
        }

        return dateIsBalanced;
    }

    public Map<Text,String> getSplitsAndLocations(Configuration conf, String tableName) throws IOException {
        return instance.getSplitsAndLocationByTable(tableName);
    }

    public List<Text> getSplits(Configuration conf, String tableName) throws IOException {
        return instance.getSplits(tableName);
    }
}
