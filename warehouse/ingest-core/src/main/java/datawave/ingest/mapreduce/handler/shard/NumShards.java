package datawave.ingest.mapreduce.handler.shard;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import datawave.ingest.data.config.ConfigurationHelper;
import datawave.ingest.data.config.ingest.AccumuloHelper;
import datawave.util.time.DateHelper;

public class NumShards {
    private static final Logger log = Logger.getLogger(NumShards.class);

    private static final String DEFAULT_NUM_SHARDS_CACHE_DIR = "/data/numShardsCache/";
    private static final String DEFAULT_NUM_SHARDS_CACHE_FILENAME = "numshards.txt";
    private static final long DEFAULT_CACHE_TIMEOUT = 1000L * 60L * 60L * 12L;

    private final Configuration conf;

    private boolean initialized = false;

    // setting this property as part of the configuration will avoid metadata table lookup
    public static final String PREFETCHED_MULTIPLE_NUMSHARDS_CONFIGURATION = "preteched.multiple.shard.configuration";

    // multiple sharding can be enabled, but not configured. this is to allow deployment with enabled release and configure afterward
    public static final String ENABLE_MULTIPLE_NUMSHARDS = "multiple.numshards.enable";

    // configuration for overriding DEFAULT CACHE variables
    public static final String MULTIPLE_NUMSHARDS_CACHE_PATH = "multiple.numshards.cache.path";
    public static final String MULTIPLE_NUMSHARDS_CACHE_FILENAME = "multiple.numshards.cache.filename";
    public static final String MULTIPLE_NUMSHARDS_CACHE_TIMEOUT = "multiple.numshards.cache.timeout";

    // populating two caches in order to support lookup by milli/dateString without doing conversion
    private TreeMap<Long,Integer> milliToNumShardsCache = new TreeMap<>();
    private TreeMap<String,Integer> yyyyMMddToNumShardsCache = new TreeMap<>();

    // seems like a good thing to calculate
    private int maxNumShards = 0;
    private int minNumShards = 0;
    private int shardCount = 0;

    private Path numShardsCachePath = null;
    private int defaultNumShards = 0;

    private static final int MAX_NUMBER_OF_RETRIES_CACHEFILE = 10;

    public static final Text NUM_SHARDS = new Text("num_shards");
    public static final Text NUM_SHARDS_CF = new Text("ns");

    private AccumuloHelper aHelper = null;

    public NumShards(Configuration conf) {
        defaultNumShards = ConfigurationHelper.isNull(conf, ShardIdFactory.NUM_SHARDS, Integer.class);
        maxNumShards = this.defaultNumShards;
        minNumShards = this.defaultNumShards;

        // populating cache with default numShards
        milliToNumShardsCache.put(Long.MIN_VALUE, this.defaultNumShards);
        yyyyMMddToNumShardsCache.put("", this.defaultNumShards);

        shardCount++;

        this.conf = conf;
        this.numShardsCachePath = new Path(conf.get(MULTIPLE_NUMSHARDS_CACHE_PATH, DEFAULT_NUM_SHARDS_CACHE_DIR),
                        conf.get(MULTIPLE_NUMSHARDS_CACHE_FILENAME, DEFAULT_NUM_SHARDS_CACHE_FILENAME));
    }

    /**
     * this method will take configuration (comma-separated yyyyMMdd_numShards) i.e.) 20170101_13,20170201_17
     *
     * @param multipleNumShardsConfiguration
     *            name of the numshards keys
     *
     */
    private void configureDyanmicNumShards(String multipleNumShardsConfiguration) {
        // this could happen if the feature is enabled, but not yet configured. treat it like it's not enabled.
        if (multipleNumShardsConfiguration != null && multipleNumShardsConfiguration.isEmpty()) {
            initialized = true;
            return;
        }

        try {
            for (String multipleNumShardsConfigEntry : multipleNumShardsConfiguration.split(",")) {
                String[] numShardsStringsSplit = multipleNumShardsConfigEntry.split("_");
                if (numShardsStringsSplit.length != 2) {
                    throw new IllegalArgumentException(
                                    "Unable to configure multiple numshards cache with the specified config: [" + multipleNumShardsConfiguration + "]");
                }

                int numShardForDay = Integer.parseInt(numShardsStringsSplit[1]);
                maxNumShards = Math.max(maxNumShards, numShardForDay);
                minNumShards = Math.min(minNumShards, numShardForDay);

                milliToNumShardsCache.put(DateHelper.parse(numShardsStringsSplit[0]).getTime(), numShardForDay);
                yyyyMMddToNumShardsCache.put(numShardsStringsSplit[0], numShardForDay);

                shardCount++;
            }
            initialized = true;
        } catch (NumberFormatException nfe) {
            throw new IllegalArgumentException(
                            "Unable to configure multiple numshards cache with the specified config: [" + multipleNumShardsConfiguration + "]", nfe);
        } catch (RuntimeException re) {
            throw new IllegalArgumentException(
                            "Unable to configure multiple numshards cache with the specified config: [" + multipleNumShardsConfiguration + "]", re);
        }
    }

    public int getNumShards(long date) {
        if (!initialized) {
            configure();
        }
        return milliToNumShardsCache.floorEntry(date).getValue();
    }

    public int getNumShards(String date) {
        if (!initialized) {
            configure();
        }
        return yyyyMMddToNumShardsCache.floorEntry(date).getValue();
    }

    private void configure() {
        // no need to go thru this, unless the system is enabled for multiple numshards
        if (conf.getBoolean(ENABLE_MULTIPLE_NUMSHARDS, false)) {
            // setting PREFETCHED_MULTIPLE_NUMSHARDS_CONFIGURATION will bypass the metadata table lookup/cache lookup
            String multipleNumShardsConfig = conf.get(PREFETCHED_MULTIPLE_NUMSHARDS_CONFIGURATION);

            // try to grab the configuration from metadata table
            if (multipleNumShardsConfig == null) {
                multipleNumShardsConfig = readMultipleNumShardsConfig();
            }

            // this helper will throw a runtime-exception, if the configuration isn't just right
            configureDyanmicNumShards(multipleNumShardsConfig);
        }
    }

    /**
     * this will read multiple numshards cache from the filesystem and return a formatted string. comma-separated date based shards. i.e.)
     * 20170101_11,20170201_13
     *
     * @return a formatted string of the date based shards
     */
    public String readMultipleNumShardsConfig() {
        if (isCacheValid()) {
            log.info(String.format("Loading the numshards cache (@ '%s')...", this.numShardsCachePath.toUri().toString()));
            try (BufferedReader in = new BufferedReader(
                            new InputStreamReader(this.numShardsCachePath.getFileSystem(this.conf).open(this.numShardsCachePath)))) {
                return in.lines().collect(Collectors.joining(","));
            } catch (IOException ioe) {
                throw new RuntimeException("Could not read numshards cache file. See documentation for using generateMultipleNumShardsCache.sh");
            }
        } else {
            throw new RuntimeException("Multiple Numshards cache is invalid. See documentation for using generateMultipleNumShardsCache.sh");
        }
    }

    public boolean isCacheValid() {
        FileStatus fileStatus = null;
        try {
            fileStatus = this.numShardsCachePath.getFileSystem(this.conf).getFileStatus(this.numShardsCachePath);
        } catch (IOException ioe) {
            log.warn("Clould not get the FileStatus of the multiple numShards file");
        }

        return null != fileStatus && fileStatus.getModificationTime() >= System.currentTimeMillis()
                        - (conf.getLong(MULTIPLE_NUMSHARDS_CACHE_TIMEOUT, DEFAULT_CACHE_TIMEOUT));
    }

    public int getMaxNumShards() {
        return maxNumShards;
    }

    public int getMinNumShards() {
        return minNumShards;
    }

    public int getShardCount() {
        return shardCount;
    }

    public boolean isInitialized() {
        return initialized;
    }

    public void updateCache() throws AccumuloException, AccumuloSecurityException, TableNotFoundException, IOException {
        FileSystem fs = this.numShardsCachePath.getFileSystem(this.conf);
        String metadataTableName = ConfigurationHelper.isNull(this.conf, ShardedDataTypeHandler.METADATA_TABLE_NAME, String.class);
        log.info("Reading the " + metadataTableName + " for multiple numshards configuration");

        if (this.aHelper == null) {
            this.aHelper = new AccumuloHelper();
            this.aHelper.setup(conf);
        }

        ArrayList<String> nsEntries = new ArrayList<>();
        try (AccumuloClient client = aHelper.newClient()) {
            ensureTableExists(client, metadataTableName);

            try (Scanner scanner = client.createScanner(metadataTableName, new Authorizations())) {
                scanner.setRange(Range.exact(NUM_SHARDS, NUM_SHARDS_CF));

                for (Map.Entry<Key,Value> entry : scanner) {
                    nsEntries.add(entry.getKey().getColumnQualifier().toString());
                }
            }
        }

        // create a new temporary file
        int count = 1;
        Path tmpShardCacheFile = new Path(this.numShardsCachePath.getParent(), numShardsCachePath.getName() + "." + count);

        while (!fs.createNewFile(tmpShardCacheFile) && count < MAX_NUMBER_OF_RETRIES_CACHEFILE) {
            count++;
            tmpShardCacheFile = new Path(this.numShardsCachePath.getParent(), numShardsCachePath.getName() + "." + count);
        }

        // now attempt to write them out
        try (PrintStream out = new PrintStream(new BufferedOutputStream(fs.create(tmpShardCacheFile)))) {

            for (String nsEntry : nsEntries) {
                out.println(nsEntry);
            }
            out.close();

            boolean isCacheLoaded = false;
            int numOfTries = 0;

            while (!isCacheLoaded && numOfTries++ < MAX_NUMBER_OF_RETRIES_CACHEFILE) {
                // now move the temporary file to the file cache
                try {
                    fs.delete(this.numShardsCachePath, false);
                    // Note this rename will fail if the file already exists (i.e. the delete failed or somebody just replaced it)
                    // but this is OK...
                    if (!fs.rename(tmpShardCacheFile, this.numShardsCachePath)) {
                        throw new IOException("Failed to rename temporary multiple numshards cache file");
                    }

                    isCacheLoaded = true;
                } catch (Exception e) {
                    log.warn("Unable to rename " + tmpShardCacheFile + " to " + this.numShardsCachePath + " probably because somebody else replaced it", e);
                    try {
                        fs.delete(tmpShardCacheFile, false);
                    } catch (Exception e2) {
                        log.error("Unable to clean up " + tmpShardCacheFile, e2);
                    }
                }
            }
        } catch (Exception e) {
            log.error("Unable to create new multiple numshards cache file", e);
        }

    }

    private void ensureTableExists(AccumuloClient client, String metadataTableName) throws AccumuloException, AccumuloSecurityException {
        TableOperations tops = client.tableOperations();
        if (!tops.exists(metadataTableName)) {
            log.info("Creating table: " + metadataTableName);
            try {
                tops.create(metadataTableName);
            } catch (TableExistsException tee) {
                log.error(metadataTableName + " already exists someone got here first");
            }
        }
    }

    public AccumuloHelper getaHelper() {
        return aHelper;
    }

    public void setaHelper(AccumuloHelper aHelper) {
        this.aHelper = aHelper;
    }
}
