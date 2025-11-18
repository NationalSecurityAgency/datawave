package datawave.ingest.mapreduce.job;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;

import datawave.ingest.mapreduce.handler.shard.ShardIdFactory;
import datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;
import datawave.util.time.DateHelper;

public class SplitsFile {
    private static final Logger log = Logger.getLogger(SplitsFile.class);

    public static final String SPLIT_WORK_DIR = "split.work.dir";
    public static final String MAX_SHARDS_PER_TSERVER = "shardedMap.max.shards.per.tserver";
    public static final String SHARD_VALIDATION_ENABLED = "shardedMap.validation.enabled";
    public static final String SHARDS_BALANCED_DAYS_TO_VERIFY = "shards.balanced.days.to.verify";
    public static final String CONFIGURED_SHARDED_TABLE_NAMES = ShardedDataTypeHandler.SHARDED_TNAMES + ".configured";
    public static final String DIST_CACHE_LABEL = "splitsFile";

    public static void setupFile(Job job, Configuration conf) throws IOException, URISyntaxException, AccumuloSecurityException, AccumuloException {

        Path baseSplitsPath = TableSplitsCache.getSplitsPath(conf);
        FileSystem sourceFs = baseSplitsPath.getFileSystem(conf);
        FileSystem destFs = new Path(conf.get(SPLIT_WORK_DIR)).getFileSystem(conf);

        // want validation turned off by default
        boolean doValidation = conf.getBoolean(SHARD_VALIDATION_ENABLED, false);

        try {
            log.info("Base splits: " + baseSplitsPath);

            Path destSplits = new Path(
                            conf.get(SPLIT_WORK_DIR) + "/" + conf.get(TableSplitsCache.SPLITS_CACHE_FILE, TableSplitsCache.DEFAULT_SPLITS_CACHE_FILE));
            log.info("Dest splits: " + destSplits);

            FileUtil.copy(sourceFs, baseSplitsPath, destFs, destSplits, false, conf);
            conf.set(TableSplitsCache.SPLITS_CACHE_DIR, conf.get(SPLIT_WORK_DIR));

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
            throw e;
        }
    }

    private static void validate(Configuration conf) throws IOException {
        TableSplitsCache cache = TableSplitsCache.getCurrentCache(conf);
        int daysToVerify = conf.getInt(SHARDS_BALANCED_DAYS_TO_VERIFY, 2) - 1;

        for (String table : conf.getStrings(ShardedDataTypeHandler.SHARDED_TNAMES)) {
            validateShardIdLocations(conf, table, daysToVerify, cache.getSplitsAndLocationByTable(table));
        }

    }

    public static void validateShardIdLocations(Configuration conf, String tableName, int daysToVerify, Map<Text,String> shardIdToLocation) {
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

    private static boolean prefixMatches(byte[] prefixBytes, byte[] keyBytes, int keyLen) {
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
    private static boolean shardsExistForDate(Map<Text,String> locations, String datePrefix, int expectedNumberOfShards) {
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
    private static boolean shardsAreBalanced(Map<Text,String> locations, String datePrefix, int maxShardsPerTserver) {
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

    public static Map<String,List<Text>> getSplits(Configuration conf) throws IOException {
        return TableSplitsCache.getCurrentCache(conf).getSplits();

    }

    public static Map<Text,String> getSplitsAndLocations(Configuration conf, String tableName) throws IOException {
        return TableSplitsCache.getCurrentCache(conf).getSplitsAndLocationByTable(tableName);
    }

    public static List<Text> getSplits(Configuration conf, String tableName) throws IOException {
        return TableSplitsCache.getCurrentCache(conf).getSplits(tableName);
    }
}
