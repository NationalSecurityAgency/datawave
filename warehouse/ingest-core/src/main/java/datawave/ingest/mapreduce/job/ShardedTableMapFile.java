package datawave.ingest.mapreduce.job;

import datawave.ingest.data.config.ConfigurationHelper;
import datawave.ingest.data.config.ingest.AccumuloHelper;
import datawave.ingest.mapreduce.handler.shard.ShardIdFactory;
import datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;
import datawave.util.StringUtils;
import datawave.util.time.DateHelper;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.admin.Locations;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

/**
 * Extracted from IngestJob
 */
public class ShardedTableMapFile {
    public static final String SHARDS_BALANCED_DAYS_TO_VERIFY = "shards.balanced.days.to.verify";
    private static final String PREFIX = ShardedTableMapFile.class.getName();
    private static final int MAX_RETRY_ATTEMPTS = 10;
    
    private static final Logger log = Logger.getLogger(ShardedTableMapFile.class);
    
    public static final String TABLE_NAMES = "job.table.names";
    public static final String SHARD_TSERVER_MAP_FILE = PREFIX + ".shardTServerMapFile";
    public static final String SPLIT_WORK_DIR = "split.work.dir";
    
    public static final String CONFIGURED_SHARDED_TABLE_NAMES = ShardedDataTypeHandler.SHARDED_TNAMES + ".configured";
    public static final String SHARDED_MAP_FILE_PATHS_RAW = "shardedMap.file.paths.raw";
    public static final String SHARD_VALIDATION_ENABLED = "shardedMap.validation.enabled";
    public static final String MAX_SHARDS_PER_TSERVER = "shardedMap.max.shards.per.tserver";
    
    public static void setupFile(Configuration conf) throws IOException, URISyntaxException, AccumuloSecurityException, AccumuloException {
        // want validation turned off by default
        boolean doValidation = conf.getBoolean(ShardedTableMapFile.SHARD_VALIDATION_ENABLED, false);
        
        Map<String,Path> map = loadMap(conf, doValidation);
        if (null == map) {
            log.fatal("Receieved a null mapping of sharded tables to split files, exiting...");
            throw new RuntimeException("Receieved a null mapping of sharded tables to split files, exiting...");
        }
        addToConf(conf, map);
    }
    
    private static SequenceFile.Reader getReader(Configuration conf, String tableName) throws IOException {
        String shardMapFileName = conf.get(SHARD_TSERVER_MAP_FILE + "." + tableName);
        try {
            return new SequenceFile.Reader(FileSystem.get(conf), new Path(shardMapFileName), conf);
        } catch (Exception e) {
            throw new IOException("Failed to create sequence file reader for " + shardMapFileName, e);
        }
    }
    
    public static TreeMap<Text,String> getShardIdToLocations(Configuration conf, String tableName) throws IOException {
        TreeMap<Text,String> locations = new TreeMap<>();
        
        SequenceFile.Reader reader = ShardedTableMapFile.getReader(conf, tableName);
        
        Text shardID = new Text();
        Text location = new Text();
        
        try {
            while (reader.next(shardID, location)) {
                locations.put(new Text(shardID), location.toString());
            }
        } finally {
            reader.close();
        }
        return locations;
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
        
        for (Entry<Text,String> entry : locations.entrySet()) {
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
    
    public static void addToConf(Configuration conf, Map<String,Path> map) {
        for (Map.Entry<String,Path> entry : map.entrySet()) {
            log.info("Loading sharded partitioner for table '" + entry.getKey() + "' with shardedMapFile '" + entry.getValue() + "'");
            conf.set(SHARD_TSERVER_MAP_FILE + "." + entry.getKey(), entry.getValue().toString());
        }
        Set<String> var = map.keySet();
        conf.setStrings(CONFIGURED_SHARDED_TABLE_NAMES, var.toArray(new String[var.size()]));
    }
    
    private static Map<String,Path> loadMap(Configuration conf, boolean doValidation) throws IOException, URISyntaxException, AccumuloSecurityException,
                    AccumuloException {
        AccumuloHelper accumuloHelper = null;
        Path workDir = new Path(conf.get(SPLIT_WORK_DIR));// todo make sure this is set in ingest job
        String[] tableNames = StringUtils.split(conf.get(TABLE_NAMES), ",");// todo make sure this is set in ingest job
        
        Map<String,String> shardedTableMapFilePaths = extractShardedTableMapFilePaths(conf);
        // Get a list of "sharded" tables
        String[] shardedTableNames = ConfigurationHelper.isNull(conf, ShardedDataTypeHandler.SHARDED_TNAMES, String[].class);
        Set<String> configuredShardedTableNames = new HashSet<>(Arrays.asList(shardedTableNames));
        
        // Remove all "sharded" tables that we aren't actually outputting to
        configuredShardedTableNames.retainAll(Arrays.asList(tableNames));
        
        Map<String,Path> shardedTableMapFiles = new HashMap<>();
        
        // Pull the list of table that we "shard":
        // Use the sequence file of splits for the current table, or pull them off of the configured Instance
        for (String shardedTableName : configuredShardedTableNames) {
            Path shardedMapFile;
            
            // If an existing splits file was provided on the command line, use it.
            // Otherwise, calculate one from the Accumulo instance
            if (shardedTableMapFilePaths.containsKey(shardedTableName) && null != shardedTableMapFilePaths.get(shardedTableName)) {
                shardedMapFile = new Path(shardedTableMapFilePaths.get(shardedTableName));
            } else {
                if (null == accumuloHelper) {
                    accumuloHelper = new AccumuloHelper();
                    accumuloHelper.setup(conf);
                }
                shardedMapFile = createShardedMapFile(log, conf, workDir, accumuloHelper, shardedTableName, doValidation);
            }
            
            // Ensure that we either computed, or were given, a valid path to the shard mappings
            if (!shardedMapFile.getFileSystem(conf).exists(shardedMapFile)) {
                log.fatal("Could not find the supplied shard map file: " + shardedMapFile);
                return null;
            } else {
                shardedTableMapFiles.put(shardedTableName, shardedMapFile);
            }
        }
        
        return shardedTableMapFiles;
    }
    
    static Map<String,String> extractShardedTableMapFilePaths(Configuration conf) {
        Map<String,String> shardedTableMapFilePaths = new HashMap<>();
        String commaSeparatedFileNamesByTable = conf.get(SHARDED_MAP_FILE_PATHS_RAW);
        if (null == commaSeparatedFileNamesByTable) {
            return shardedTableMapFilePaths;
        }
        
        String[] pairs = StringUtils.split(commaSeparatedFileNamesByTable, ',');
        
        for (String pair : pairs) {
            int index = pair.indexOf('=');
            if (index < 0) {
                log.warn("WARN: Skipping bad tableName=/path/to/tableNameSplits.seq property: " + pair);
            } else {
                String tableName = pair.substring(0, index), splitsFile = pair.substring(index + 1);
                
                log.info("Using splits file '" + splitsFile + "' for table '" + tableName + "'");
                
                shardedTableMapFilePaths.put(tableName, splitsFile);
            }
        }
        return shardedTableMapFilePaths;
    }
    
    /**
     * Build a file that maps shard IDs (row keys in the sharded table) to the tablet server where they are currently stored.
     *
     * @param log
     *            logger for reporting errors
     * @param conf
     *            hadoop configuration
     * @param workDir
     *            base dir in HDFS where the file is written
     * @param accumuloHelper
     *            Accumulo helper to query shard locations
     * @param shardedTableName
     *            name of the shard table--the table whose locations we are querying
     * @param validateShardLocations
     *            if validation of shards mappings should be performed
     * @return the path to the sharded table map file
     * @throws IOException
     * @throws URISyntaxException
     */
    public static Path createShardedMapFile(Logger log, Configuration conf, Path workDir, AccumuloHelper accumuloHelper, String shardedTableName,
                    boolean validateShardLocations) throws IOException, URISyntaxException {
        Path shardedMapFile = null;
        // minus one to make zero based indexed
        int daysToVerify = conf.getInt(SHARDS_BALANCED_DAYS_TO_VERIFY, 2) - 1;
        
        if (null != shardedTableName) {
            // Read all the metadata entries for the sharded table so that we can
            // get the mapping of shard IDs to tablet locations.
            log.info("Reading metadata entries for " + shardedTableName);
            
            Map<Text,String> splitToLocations = getLocations(log, accumuloHelper, shardedTableName);
            if (validateShardLocations) {
                validateShardIdLocations(conf, shardedTableName, daysToVerify, splitToLocations);
            }
            
            // Now write all of the assignments out to a file stored in HDFS
            // we're ok with putting the sharded table file in the hdfs workdir. why is that not good enough for the non sharded splits?
            shardedMapFile = new Path(workDir, shardedTableName + "_shards.lst");
            log.info("Writing shard assignments to " + shardedMapFile);
            long count = writeSplitsFile(splitToLocations, shardedMapFile, conf);
            log.info("Wrote " + count + " shard assignments to " + shardedMapFile);
        }
        
        return shardedMapFile;
    }
    
    /**
     * Continually scans the metdata table attempting to get the split locations for the shard table.
     *
     * @param log
     *            logger for reporting errors
     * @param accumuloHelper
     *            Accumulo helper to query shard locations
     * @param shardedTableName
     *            name of the shard table--the table whose locations we are querying
     * @return a map of split (endRow) to the location of those tablets in accumulo
     */
    public static Map<Text,String> getLocations(Logger log, AccumuloHelper accumuloHelper, String shardedTableName) {
        // split (endRow) -> String location mapping
        Map<Text,String> splitToLocation = new TreeMap<>();
        
        boolean keepRetrying = true;
        int attempts = 0;
        while (keepRetrying && attempts < MAX_RETRY_ATTEMPTS) {
            try {
                TableOperations tableOps = accumuloHelper.getConnector().tableOperations();
                attempts++;
                // if table does not exist don't want to catch the errors and end up in infinite loop
                if (!tableOps.exists(shardedTableName)) {
                    log.error("Table " + shardedTableName + " not found, skipping split locations for missing table");
                } else {
                    Range range = new Range();
                    Locations locations = tableOps.locate(shardedTableName, Collections.singletonList(range));
                    List<TabletId> tabletIds = locations.groupByRange().get(range);
                    tabletIds.forEach(tId -> splitToLocation.put(tId.getEndRow(), locations.getTabletLocation(tId)));
                }
                // made it here, no errors so break out
                keepRetrying = false;
            } catch (Exception e) {
                log.warn(e.getMessage() + " ... retrying ...");
                UtilWaitThread.sleep(3000);
            }
        }
        
        return splitToLocation;
    }
    
    /**
     * Writes the contents of splits out to a sequence file on the given FileSystem.
     *
     * @param splits
     *            map of split points for a table
     * @param file
     *            the file to which the splits should be written
     * @param conf
     *            hadoop configuration
     * @return the number of entries written to the splits file
     * @throws IOException
     *             if file system interaction fails
     */
    public static long writeSplitsFile(Map<Text,String> splits, Path file, Configuration conf) throws IOException {
        FileSystem fs = file.getFileSystem(conf);
        if (fs.exists(file))
            fs.delete(file, false);
        
        long count = 0;
        // reusable value for writing
        Text value = new Text();
        SequenceFile.Writer writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(file), SequenceFile.Writer.keyClass(Text.class),
                        SequenceFile.Writer.valueClass(Text.class));
        for (Entry<Text,String> entry : splits.entrySet()) {
            count++;
            value.set(entry.getValue());
            writer.append(entry.getKey(), value);
        }
        writer.close();
        return count;
    }
    
    /**
     * Writes the contents of splits out to a sequence file on the given FileSystem.
     *
     * @param splits
     *            map of split points for a table
     * @param file
     *            the file to which the splits should be written
     * @param conf
     *            hadoop configuration
     * @return the number of entries written to the splits file
     * @throws IOException
     *             if file system interaction fails
     */
    public static long writeSplitsFileLegacy(Map<Text,String> splits, Path file, Configuration conf) throws IOException {
        return writeSplitsFile(splits, file, conf);
    }
    
}
