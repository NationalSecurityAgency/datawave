package datawave.ingest.mapreduce.job;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.clientImpl.ClientConfConverter;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.ClientInfo;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataServicer;
import org.apache.accumulo.core.singletons.SingletonReservation;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.log4j.Logger;

import com.google.common.collect.Maps;

import datawave.ingest.config.BaseHdfsFileCacheUtil;
import datawave.ingest.mapreduce.partition.BalancedShardPartitioner;
import datawave.ingest.mapreduce.partition.DelegatePartitioner;
import datawave.util.StringUtils;

/**
 * This class encapsulates the split points found in the accumulo.metadata table. Methods are also supplied to distribute the split points via a distributed job
 * cache. When the splits cache is updated, the current file will be compared to the new file to ensure that the MAX_SPLIT_DECREASE and
 * MAX_SPLIT_PERCENTAGE_DECREASE thresholds have not been exceeded.
 */
public class TableSplitsCache extends BaseHdfsFileCacheUtil {

    public static final String REFRESH_SPLITS = "datawave.ingest.refresh.splits";
    public static final String SPLITS_CACHE_DIR = "datawave.ingest.splits.cache.dir";
    public static final String SPLITS_CACHE_FILE = "datawave.ingest.splits.cache.fileName";

    public static final String MAX_SPLIT_DECREASE = "datawave.ingest.splits.max.decrease.number";
    public static final String MAX_SPLIT_PERCENTAGE_DECREASE = "datawave.ingest.splits.max.decrease.percentage";
    private static final Logger log = Logger.getLogger(TableSplitsCache.class);
    private static final String DEFAULT_SPLITS_CACHE_DIR = "/data/splitsCache";
    public static final String DEFAULT_SPLITS_CACHE_FILE = "all-splits.txt";
    private static final short DEFAULT_MAX_SPLIT_DECREASE = 42;
    private static final double DEFAULT_MAX_SPLIT_PERCENTAGE_DECREASE = .5;
    private static final boolean DEFAULT_REFRESH_SPLITS = true;
    private static final String NO_LOCATION = "noloc";
    private static TableSplitsCache cache;
    private volatile boolean cacheFileRead = false;
    private Object semaphore = new Object();

    private Path splitsPath = null;
    private Map<String,Map<Text,String>> splitLocations = new HashMap<>();
    private Map<String,List<Text>> splits = new HashMap<String,List<Text>>();

    private PartitionerCache partitionerCache;

    private Map<Text,String> getSplitsWithLocation(String table) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        SortedMap<KeyExtent,String> tabletLocations = new TreeMap<>();

        Properties props = Accumulo.newClientProperties().to(accumuloHelper.getInstanceName(), accumuloHelper.getZooKeepers())
                        .as(accumuloHelper.getUsername(), new PasswordToken(accumuloHelper.getPassword())).build();
        ClientInfo info = ClientInfo.from(props);
        ClientContext context = new ClientContext(SingletonReservation.noop(), info, ClientConfConverter.toAccumuloConf(info.getProperties()), Threads.UEH);

        MetadataServicer.forTableName(context, table).getTabletLocations(tabletLocations);

        return tabletLocations.entrySet().stream().filter(k -> k.getKey().endRow() != null).collect(
                        Collectors.toMap(e -> e.getKey().endRow(), e -> e.getValue() == null ? NO_LOCATION : e.getValue(), (o1, o2) -> o1, TreeMap::new));
    }

    /**
     * @param conf
     *            the configuration
     */

    public TableSplitsCache(Configuration conf) {

        super(conf);
        this.splitsPath = getSplitsPath(conf);
        this.partitionerCache = new PartitionerCache(conf);
        this.cacheFileRead = false;

    }

    public static TableSplitsCache getCurrentCache(Configuration conf) {
        if (null == cache) {
            cache = new TableSplitsCache(conf);
        }

        return cache;
    }

    public static void clear() {
        cache = null;
    }

    /**
     * @param tableSplits
     *            split points(end-row names) for a table
     * @param maxSplits
     *            maximum number of splits to return
     * @return split points grouped into fewer evenly grouped splits so as not to exceed maxSplits
     */
    public static List<Text> trimSplits(List<Text> tableSplits, int maxSplits) {
        if (tableSplits.size() <= maxSplits) {
            return tableSplits;
        }
        double r = (maxSplits + 1) / (double) (tableSplits.size());
        double pos = 0;

        List<Text> subset = new ArrayList<>();

        int numberSplitsUsed = 0;
        for (int i = 0; i < tableSplits.size() && numberSplitsUsed < maxSplits; i++) {
            pos += r;
            while (pos > 1) {
                subset.add(tableSplits.get(i));
                numberSplitsUsed++;
                pos -= 1;
            }
        }

        return subset;
    }

    @Override
    public void setCacheFilePath(Configuration conf) {
        this.cacheFilePath = new Path(conf.get(SPLITS_CACHE_DIR, DEFAULT_SPLITS_CACHE_DIR), conf.get(SPLITS_CACHE_FILE, DEFAULT_SPLITS_CACHE_FILE));
    }

    public void setCacheFilePath(Path path) {
        this.cacheFilePath = path;
    }

    public static Path getSplitsPath(Configuration conf) {
        return new Path(conf.get(SPLITS_CACHE_DIR, DEFAULT_SPLITS_CACHE_DIR), conf.get(SPLITS_CACHE_FILE, DEFAULT_SPLITS_CACHE_FILE));
    }

    @Override
    protected boolean shouldRefreshCache(Configuration conf) {
        return shouldRefreshSplits(conf);
    }

    public static boolean shouldRefreshSplits(Configuration conf) {
        return (conf.getBoolean(REFRESH_SPLITS, DEFAULT_REFRESH_SPLITS));
    }

    /**
     * @return the file status
     */
    public FileStatus getFileStatus() {
        FileStatus splitsStatus = null;
        try {
            splitsStatus = FileSystem.get(this.splitsPath.toUri(), conf).getFileStatus(this.splitsPath);
        } catch (IOException ex) {
            String logMessage = ex instanceof FileNotFoundException ? "No splits file present" : "Could not get the FileStatus of the splits file";
            log.warn(logMessage);
        }
        return splitsStatus;
    }

    private Set<String> getIngestTableNames() {
        Set<String> tableNames = TableConfigurationUtil.extractTableNames(conf);
        if (tableNames.isEmpty()) {
            log.error("Missing data types or one of the following helperClass,readerClass,handlerClassNames,filterClassNames");
            throw new IllegalArgumentException("Missing data types or one of the following helperClass,readerClass,handlerClassNames,filterClassNames");
        }
        return tableNames;
    }

    /**
     * updates the splits file if the splits in the new file have not decreased beyond the maximum deviation allowed
     *
     * @param fs
     *            the filesystem
     * @param tmpSplitsFile
     *            a path to the splits file
     * @throws IOException
     *             for issues with read or write
     */
    @Override
    public void writeCacheFile(FileSystem fs, Path tmpSplitsFile) throws IOException {
        initAccumuloHelper();
        Map<String,Integer> splitsPerTable = new HashMap<>();
        Set<String> tableNames = getIngestTableNames();

        try {
            updatePartitionerCache(tableNames);
        } catch (ClassNotFoundException cnf) {
            throw new IOException("Unable to generate splits. Invalid partitioner configuration ", cnf);
        }

        try (PrintStream out = new PrintStream(new BufferedOutputStream(fs.create(tmpSplitsFile)))) {
            // gather the splits and write to PrintStream
            for (String table : tableNames) {
                Partitioner<BulkIngestKey,Value> partitioner = partitionerCache.getPartitioner(new Text(table));

                log.info("Retrieving splits for " + table);
                Map<Text,String> splitLocations = getSplitsWithLocation(table);
                List<Text> splits = new ArrayList<>(splitLocations.keySet());
                splitsPerTable.put(table, splits.size());

                log.info("Writing " + splits.size() + " splits.");

                // write splits according to what each table's configured partitioner will need
                if (partitioner instanceof DelegatePartitioner) {
                    if (!((DelegatePartitioner) partitioner).needSplits()) {
                        log.info("Splits not required for " + partitioner.getClass().getName() + ", " + table);
                        continue;
                    } else {
                        if (((DelegatePartitioner) partitioner).needSplitLocations()) {
                            // only write locations if we need them for partitioner logic
                            if (partitioner instanceof BalancedShardPartitioner) {
                                splitLocations = reverseSortByShardIds(splitLocations);
                            }
                            writeLocations(out, table, splits, splitLocations);

                        } else {
                            writeSplits(out, table, splits);
                        }
                    }

                } else {
                    writeSplits(out, table, splits);
                }

            }
            if (null != getFileStatus() && exceedsMaxSplitsDeviation(splitsPerTable)) {
                // if the file exists and the new file would exceed the deviation threshold, don't replace it
                throw new IOException("Splits file will not be replaced");
            }
        } catch (IOException | AccumuloSecurityException | AccumuloException | TableNotFoundException ex) {
            log.error("Unable to write new splits file", ex);
            throw new IOException(ex);
        }

    }

    private void writeSplits(PrintStream out, String table, List<Text> splits) {
        for (Text split : splits) {
            out.println(table + this.delimiter + new String(Base64.encodeBase64(split.getBytes())));
        }
    }

    private void writeLocations(PrintStream out, String table, List<Text> splits, Map<Text,String> splitLocations) {
        for (Text split : splits) {
            out.println(table + this.delimiter + new String(Base64.encodeBase64(split.getBytes())) + "\t" + splitLocations.get(split));
        }
    }

    public TreeMap<Text,String> reverseSortByShardIds(Map<Text,String> shardIdToLocations) {

        TreeMap<Text,String> shardIdsToTservers = Maps.newTreeMap((o1, o2) -> o2.compareTo(o1));
        shardIdsToTservers.putAll(shardIdToLocations);
        return shardIdsToTservers;
    }

    private void updatePartitionerCache(Set<String> tableNames) throws ClassNotFoundException {
        ArrayList<Text> tables = new ArrayList<>();
        for (String t : tableNames) {
            if (partitionerCache.hasPartitionerOverride(new Text(t))) {
                tables.add(new Text(t));
            }
        }
        partitionerCache.createAndCachePartitioners(tables);

    }

    private boolean exceedsMaxSplitsDeviation(Map<String,Integer> tmpSplitsPerTable) {
        Map<String,Integer> currentSplitsPerTable = getCurrentSplitsPerTable();
        if (!currentSplitsPerTable.isEmpty()) {
            Set<String> currentTables = currentSplitsPerTable.keySet();
            for (String tableName : currentTables) {
                if (tableExceedsMaxSplitDeviation(tmpSplitsPerTable.get(tableName), currentSplitsPerTable.get(tableName), tableName)) {

                    log.warn(tableName
                                    + "Splits have decreased by greater than MAX_SPLIT_DECREASE or MAX_SPLIT_PERCENTAGE_DECREASE. Splits file will not be replaced. To force replacement, delete the current file and run generateSplitsFile.sh");
                    return true;
                }
            }
        }
        return false;

    }

    boolean tableExceedsMaxSplitDeviation(Integer newCount, Integer currentCount, String tableName) {
        double maxSplitPercentDecrease = conf.getDouble(MAX_SPLIT_PERCENTAGE_DECREASE, DEFAULT_MAX_SPLIT_PERCENTAGE_DECREASE);
        int maxSplitDecrease = conf.getInt(MAX_SPLIT_DECREASE, DEFAULT_MAX_SPLIT_DECREASE);
        return currentCount * (1 - maxSplitPercentDecrease) > newCount && currentCount - newCount > maxSplitDecrease;
    }

    private Map<String,Integer> getCurrentSplitsPerTable() {
        Map<String,Integer> currentSplitsPerTable = new HashMap<>();
        if (splitLocations.isEmpty())
            try {
                read();
            } catch (IOException ex) {
                log.warn("Failed to read splits file", ex);
            }
        for (String tableName : this.splitLocations.keySet()) {
            currentSplitsPerTable.put(tableName, this.splitLocations.get(tableName).size());
        }
        return currentSplitsPerTable;
    }

    @Override
    public void read() throws IOException {
        if (cacheFileRead) {
            return;
        }

        synchronized (semaphore) {
            if (cacheFileRead) {
                return;
            }

            try {
                log.info("Attempting to read splits from distributed cache");
                File distCacheSplitsFile = new File("./" + SplitsFile.DIST_CACHE_LABEL);
                FileInputStream fis = new FileInputStream(distCacheSplitsFile);
                BufferedReader bis = new BufferedReader(new InputStreamReader(fis));
                readCache(bis);
                cacheFileRead = true;
                log.info("Great success! High five!");
            } catch (IOException e) {
                log.info("Unable to read splits file from Distributed Cache.  Proceeding to HDFS");
                super.read();
                cacheFileRead = true;
            }

        }
    }

    /**
     * Read a stream into a map of table name to split points
     *
     * @param in
     *            an input stream containing the split points to read
     * @throws IOException
     *             for issues with read or write
     */
    @Override
    protected void readCache(BufferedReader in) throws IOException {
        this.splitLocations = new HashMap<>();
        this.splits = new HashMap<>();
        // since we do not know whether -XX:+UseStringDeduplication is being used, lets do it ourselves
        Map<String,String> locationDedup = new HashMap<>();
        String line;
        String tableName = null;
        Map<Text,String> tmpSplitLocations = null;
        List<Text> tmpSplits = null;

        while ((line = in.readLine()) != null) {
            String[] parts = StringUtils.split(line, this.delimiter);
            if (tableName == null || !tableName.equals(parts[0])) {
                if (null == tmpSplitLocations || tmpSplitLocations.isEmpty()) {
                    this.splitLocations.remove(tableName);
                }
                tableName = parts[0];
                tmpSplitLocations = new HashMap<>();
                tmpSplits = new ArrayList<>();
                this.splitLocations.put(tableName, Collections.unmodifiableMap(tmpSplitLocations));
                this.splits.put(tableName, Collections.unmodifiableList(tmpSplits));
            }
            if (parts.length >= 2) {
                Text split = new Text(Base64.decodeBase64(parts[1]));
                tmpSplits.add(split);
                if (parts.length == 3) {
                    tmpSplitLocations.put(split, dedup(locationDedup, parts[2]));
                }
            }

        }
        if (null == tmpSplitLocations || tmpSplitLocations.isEmpty()) {
            this.splitLocations.remove(tableName);
        }
        in.close();
    }

    private String dedup(Map<String,String> dedupMap, String value) {
        String dedup = dedupMap.get(value);
        if (dedup == null) {
            dedupMap.put(value, value);
            return value;
        }
        return dedup;
    }

    /**
     *
     * @param table
     *            name of the table
     * @return list of splits for the table
     * @throws IOException
     *             for issues with read or write
     */
    public List<Text> getSplits(String table) throws IOException {
        if (this.splits.isEmpty()) {
            read();
        }
        List<Text> splitList = this.splits.get(table);
        return (splitList == null ? Collections.emptyList() : splitList);
    }

    /**
     *
     * @param table
     *            name of the table
     * @param maxSplits
     *            number of maximum splits
     * @return a list of the splits
     * @throws IOException
     *             for issues with read or write
     */
    public List<Text> getSplits(String table, int maxSplits) throws IOException {
        return trimSplits(getSplits(table), maxSplits);
    }

    /**
     * @return map of table name to list of splits for the table
     * @throws IOException
     *             for issues with read or write
     */
    public Map<String,List<Text>> getSplits() throws IOException {
        if (this.splits.isEmpty())
            read();
        return Collections.unmodifiableMap(splits);
    }

    /**
     * @param table
     * @return map of splits to tablet locations for the table
     * @throws IOException
     */
    public Map<Text,String> getSplitsAndLocationByTable(String table) throws IOException {
        if (this.splitLocations.isEmpty())
            read();
        if (this.splitLocations.containsKey(table)) {
            return this.splitLocations.get(table);
        } else {
            return Collections.emptyMap();
        }
    }

    /**
     * @return map of splits to table name to map of splits to table locations for the table
     * @throws java.io.IOException
     */
    public Map<String,Map<Text,String>> getSplitsAndLocation() throws IOException {
        if (this.splitLocations.isEmpty())
            read();
        return Collections.unmodifiableMap(splitLocations);
    }

}
