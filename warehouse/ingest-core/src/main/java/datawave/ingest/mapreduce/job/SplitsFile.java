package datawave.ingest.mapreduce.job;

import static datawave.ingest.mapreduce.job.SplitsConstants.SPLITS_CACHE_DIR;
import static datawave.ingest.mapreduce.job.SplitsConstants.SPLITS_CACHE_FILE;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import org.apache.accumulo.core.data.LoadPlan;
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

    private final long now;
    private final String today;

    private ConcurrentHashMap<String,NavigableMap<Text,Integer>> shardPartitionsByTable;
    private TableSplitsCache instance;

    public SplitsFile() {
        this.shardPartitionsByTable = new ConcurrentHashMap<>();
        this.now = System.currentTimeMillis();
        this.today = formatDay(0);
    }

    @Override
    public void init(Configuration conf) {
        this.instance = TableSplitsCache.getCurrentCache(conf);
    }

    @Override
    public void setupJob(Job job, Configuration conf) throws IOException {

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
            log.error("Unable to use splits file because {}", e.getMessage(), e);
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
    public int getSplitsCount(String table) {
        int count = 0;
        try {
            List<Text> splits = instance.getSplits(table);
            count = splits == null ? -1 : splits.size();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return count;
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
        NavigableMap<Text,Integer> assignments = lazilyCreateAssignments(tableName);
        Integer partitionId = assignments.get(shardId);
        if (partitionId != null) {
            return partitionId;
        }
        return -1;
    }

    @Override
    public int getNearestPartition(String tableName, Text shardId) {
        NavigableMap<Text,Integer> assignments = lazilyCreateAssignments(tableName);
        Integer partitionId = assignments.get(shardId);
        if (partitionId != null) {
            return partitionId;
        }
        if (assignments.isEmpty()) {
            // no assignments to collapse against, fall back to a hash-based partition
            return (shardId.hashCode() & Integer.MAX_VALUE);
        }

        // nearest key greater than shardId; if shardId sorts after every key (e.g. today's shard hasn't been
        // created yet), fall back to the most recent existing key
        Map.Entry<Text,Integer> nearest = assignments.ceilingEntry(shardId);
        return nearest != null ? nearest.getValue() : assignments.lastEntry().getValue();
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
     * For a given tablename, provides the mapping from {@code shard id -> partition}. Uses {@code computeIfAbsent} so concurrent first-callers for the same
     * table don't each redundantly build the full assignment map.
     *
     * @param tableName
     *            the name of the table
     * @return a list of the shard mappings
     */
    private NavigableMap<Text,Integer> lazilyCreateAssignments(String tableName) {
        return this.shardPartitionsByTable.computeIfAbsent(tableName, t -> {
            try {
                return getPartitionsByShardId(t);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
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
    private TreeMap<Text,Integer> getPartitionsByShardId(String tableName) throws IOException {
        if (log.isDebugEnabled())
            log.debug("Loading splits data for " + tableName);

        List<Text> sortedSplits = instance.getSplits(tableName);
        Map<Text,String> shardIdToLocation = instance.getSplitsAndLocationByTable(tableName);

        if (log.isDebugEnabled())
            log.debug("Assigning partitioners for each shard in " + tableName);
        return assignPartitionsForEachShard(sortedSplits, shardIdToLocation);
    }

    /**
     * Assigns each tablet server a partition slot in order of most-recent shard ID first (skipping future-dated shards), then maps every shard ID to its tablet
     * server's partition.
     *
     * @param shardIdToLocations
     *            the map of shard ids and their location
     * @return the shard id to partition assignments
     */
    private TreeMap<Text,Integer> assignPartitionsForEachShard(List<Text> sortedShardIds, Map<Text,String> shardIdToLocations) {
        int totalNumUniqueTServers = calculateNumberOfUniqueTservers(shardIdToLocations);

        HashMap<String,Integer> partitionsByTServer = getTServerAssignments(totalNumUniqueTServers, sortedShardIds, shardIdToLocations);
        TreeMap<Text,Integer> partitionsByShardId = getShardIdAssignments(shardIdToLocations, partitionsByTServer);

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
        return shardIdStr.substring(0, 8).compareTo(today) > 0;
    }

    private String formatDay(int numDaysBack) {
        return DateHelper.format(now - (DateUtils.MILLIS_PER_DAY * numDaysBack));
    }

    private TreeMap<Text,Integer> getShardIdAssignments(Map<Text,String> shardIdsToTservers, HashMap<String,Integer> partitionsByTServer) {
        TreeMap<Text,Integer> partitionsByShardId = new TreeMap<>();
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
                    log.warn("{} Shards for {} assigned to tablet {}", cnt.toInteger(), datePrefix, value);
                    dateIsBalanced = false;
                }

                tabletsSeenForDate.put(value, cnt);
            }
        }

        return dateIsBalanced;
    }

    public Map<Text,String> getSplitsAndLocations(String tableName) throws IOException {
        return instance.getSplitsAndLocationByTable(tableName);
    }

    public List<Text> getSplits(String tableName) throws IOException {
        return instance.getSplits(tableName);
    }

    /**
     * Finds two contiguous table splits that contain the specified row should reside
     *
     * @param lookupRow
     *            Row value to be mapped
     * @param tableSplits
     *            Splits for the table in question
     * @return KeyExtent mapping for the given row
     */
    static LoadPlan.TableSplits findContainingSplits(Text lookupRow, List<Text> tableSplits) {
        int position = Collections.binarySearch(tableSplits, lookupRow);
        if (position < 0) {
            position = -1 * (position + 1);
        }

        Text prevRow = position == 0 ? null : tableSplits.get(position - 1);
        Text endRow = position == tableSplits.size() ? null : tableSplits.get(position);

        return new LoadPlan.TableSplits(prevRow, endRow);
    }

    /**
     * @param table
     * @return map of splits to tablet locations for the table
     * @throws IOException
     */
    public Map<Text,String> getSplitsAndLocationByTable(String table) throws IOException {
        return instance.getSplitsAndLocationByTable(table);
    }
}
