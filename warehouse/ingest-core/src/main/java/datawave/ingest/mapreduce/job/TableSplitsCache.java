package datawave.ingest.mapreduce.job;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import datawave.ingest.config.BaseHdfsFileCacheUtil;
import datawave.util.StringUtils;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/**
 * This class encapsulates the split points found in the accumulo.metadata table. Methods are also supplied to distribute the split points via a distributed job
 * cache. When the splits cache is updated, the current file will be compared to the new file to ensure that the MAX_SPLIT_DECREASE and
 * MAX_SPLIT_PERCENTAGE_DECREASE thresholds have not been exceeded.
 */
public class TableSplitsCache extends BaseHdfsFileCacheUtil {
    
    public static final String REFRESH_SPLITS = "datawave.ingest.refresh.splits";
    public static final String SPLITS_CACHE_DIR = "datawave.ingest.splits.cache.dir";
    public static final String MAX_SPLIT_DECREASE = "datawave.ingest.splits.max.decrease.number";
    public static final String MAX_SPLIT_PERCENTAGE_DECREASE = "datawave.ingest.splits.max.decrease.percentage";
    private static final Logger log = Logger.getLogger(TableSplitsCache.class);
    private static final String DEFAULT_SPLITS_CACHE_DIR = "/data/splitsCache";
    private static final String SPLITS_CACHE_FILE = "all-splits.txt";
    private static final short DEFAULT_MAX_SPLIT_DECREASE = 42;
    private static final double DEFAULT_MAX_SPLIT_PERCENTAGE_DECREASE = .5;
    private static final boolean DEFAULT_REFRESH_SPLITS = true;
    private Path splitsPath = null;
    private Map<String,List<Text>> splits = null;
    
    /**
     *
     * @param conf
     *            the configuration
     */
    
    public TableSplitsCache(Configuration conf) {
        super(conf);
        this.splitsPath = getSplitsPath(conf);
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
        
        ArrayList<Text> subset = new ArrayList<>(maxSplits);
        
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
        this.cacheFilePath = new Path(conf.get(SPLITS_CACHE_DIR, DEFAULT_SPLITS_CACHE_DIR), SPLITS_CACHE_FILE);
    }
    
    public static Path getSplitsPath(Configuration conf) {
        return new Path(conf.get(SPLITS_CACHE_DIR, DEFAULT_SPLITS_CACHE_DIR), SPLITS_CACHE_FILE);
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
        TableOperations tops = null;
        initAccumuloHelper();
        try (AccumuloClient client = this.accumuloHelper.newClient()) {
            tops = client.tableOperations();
            Set<String> tableNames = getIngestTableNames();
            Map<String,Integer> splitsPerTable = new HashMap<>();
            try (PrintStream out = new PrintStream(new BufferedOutputStream(fs.create(tmpSplitsFile)))) {
                this.splits = new HashMap<>();
                // gather the splits and write to PrintStream
                for (String table : tableNames) {
                    log.info("Retrieving splits for " + table);
                    List<Text> splits = new ArrayList<>(tops.listSplits(table));
                    this.splits.put(table, splits);
                    splitsPerTable.put(table, splits.size());
                    log.info("Writing " + splits.size() + " splits.");
                    for (Text split : splits) {
                        out.println(table + this.delimiter + new String(Base64.encodeBase64(split.getBytes())));
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
        
    }
    
    private boolean exceedsMaxSplitsDeviation(Map<String,Integer> tmpSplitsPerTable) {
        Map<String,Integer> currentSplitsPerTable = getCurrentSplitsPerTable();
        if (!currentSplitsPerTable.isEmpty()) {
            Set<String> currentTables = currentSplitsPerTable.keySet();
            for (String tableName : currentTables) {
                if (currentSplitsPerTable.get(tableName)
                                * (1 - conf.getDouble(MAX_SPLIT_PERCENTAGE_DECREASE, DEFAULT_MAX_SPLIT_PERCENTAGE_DECREASE)) > tmpSplitsPerTable.get(tableName)
                                && currentSplitsPerTable.get(tableName) - tmpSplitsPerTable.get(tableName) > conf.getInt(MAX_SPLIT_DECREASE,
                                                DEFAULT_MAX_SPLIT_DECREASE)) {
                    log.warn(tableName
                                    + "Splits have decreased by greater than MAX_SPLIT_DECREASE or MAX_SPLIT_PERCENTAGE_DECREASE. Splits file will not be replaced. To force replacement, delete the current file and run generateSplitsFile.sh");
                    return true;
                }
            }
        }
        return false;
        
    }
    
    private Map<String,Integer> getCurrentSplitsPerTable() {
        Map<String,Integer> currentSplitsPerTable = new HashMap<>();
        if (null == this.splits)
            try {
                read();
            } catch (IOException ex) {
                log.warn("No splits file exists");
            }
        for (String tableName : this.splits.keySet()) {
            currentSplitsPerTable.put(tableName, this.splits.get(tableName).size());
        }
        return currentSplitsPerTable;
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
        this.splits = new HashMap<>();
        String line;
        String tableName = null;
        List<Text> splits = null;
        while ((line = in.readLine()) != null) {
            String[] parts = StringUtils.split(line, this.delimiter);
            if (tableName == null || !tableName.equals(parts[0])) {
                tableName = parts[0];
                splits = new ArrayList<>();
                this.splits.put(tableName, splits);
            }
            if (parts.length > 1) {
                splits.add(new Text(Base64.decodeBase64(parts[1].getBytes())));
            }
        }
        in.close();
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
        if (null == this.splits)
            read();
        List<Text> tableSplits = this.splits.get(table);
        if (tableSplits == null) {
            return Collections.emptyList();
        }
        return tableSplits;
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
        if (null == this.splits)
            read();
        return new HashMap<>(splits);
    }
    
}
