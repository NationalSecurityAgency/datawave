package datawave.ingest.mapreduce.job;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import datawave.ingest.data.config.ingest.AccumuloHelper;
import datawave.util.StringUtils;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataServicer;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * This class encapsulates the split points found in the accumulo.metadata table. Methods are also supplied to distribute the split points via a distributed job
 * cache. When the splits cache is updated, the current file will be compared to the new file to ensure that the MAX_SPLIT_DECREASE and
 * MAX_SPLIT_PERCENTAGE_DECREASE thresholds have not been exceeded.
 */
public class MetadataTableSplits {
    
    public static final String REFRESH_SPLITS = "datawave.ingest.refresh.splits";
    public static final String SPLITS_CACHE_DIR = "datawave.ingest.splits.cache.dir";
    public static final String MAX_SPLIT_DECREASE = "datawave.ingest.splits.max.decrease.number";
    public static final String MAX_SPLIT_PERCENTAGE_DECREASE = "datawave.ingest.splits.max.decrease.percentage";
    private static final Logger log = Logger.getLogger(MetadataTableSplits.class);
    private static final String DEFAULT_SPLITS_CACHE_DIR = "/data/splitsCache";
    private static final String SPLITS_CACHE_FILE = "all-splits.txt";
    private static final short DEFAULT_MAX_SPLIT_DECREASE = 42;
    private static final double DEFAULT_MAX_SPLIT_PERCENTAGE_DECREASE = .5;
    private static final boolean DEFAULT_REFRESH_SPLITS = true;
    private final Configuration conf;
    private AccumuloHelper cbHelper = null;
    private Path splitsPath = null;
    private Map<String,List<Text>> splits = null;
    private Map<String,Map<Text,String>> splitLocations = null;

    private Map<Text,String> getSplitsWithLocation(String table) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        SortedMap<KeyExtent,String> tabletLocations = new TreeMap<>();
        SortedMap<Text,String> tabletLocationsByEndRow = new TreeMap<>();

        MetadataServicer.forTableName(new ClientContext(this.cbHelper.getInstance(), this.cbHelper.getCredentials(), this.cbHelper.getZookeeperConfig()), table).getTabletLocations(tabletLocations);
        for (Map.Entry<KeyExtent,String> entry : tabletLocations.entrySet()){
            if(entry.getKey().getEndRow() != null){
                tabletLocationsByEndRow.put(entry.getKey().getEndRow(), entry.getValue());
            }
        }
        return tabletLocationsByEndRow;
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
    
    /**
     * 
     * @param conf
     */
    
    public MetadataTableSplits(Configuration conf) {
        Validate.notNull(conf, "Configuration object passed to MetadataTableSplits is null");
        this.conf = conf;
        this.splitsPath = getSplitsPath(conf);
    }
    
    public static Path getSplitsPath(Configuration conf) {
        return new Path(conf.get(SPLITS_CACHE_DIR, DEFAULT_SPLITS_CACHE_DIR), SPLITS_CACHE_FILE);
    }
    
    public static Path getSplitsDir(Configuration conf) {
        return new Path(conf.get(SPLITS_CACHE_DIR, DEFAULT_SPLITS_CACHE_DIR));
    }
    
    public static boolean shouldRefreshSplits(Configuration conf) {
        return (conf.getBoolean(REFRESH_SPLITS, DEFAULT_REFRESH_SPLITS));
    }
    
    /**
     * 
     * @return
     */
    public FileStatus getFileStatus() {
        FileStatus splitsStatus = null;
        try {
            splitsStatus = FileSystem.get(conf).getFileStatus(this.splitsPath);
        } catch (IOException ex) {
            String logMessage = ex instanceof FileNotFoundException ? "No splits file present" : "Could not get the FileStatus of the splits file";
            log.warn(logMessage);
        }
        return splitsStatus;
    }
    
    private void initAccumuloHelper() {
        if (cbHelper == null) {
            cbHelper = new AccumuloHelper();
            cbHelper.setup(conf);
        }
    }
    
    private Path createTempFile(FileSystem fs) throws IOException {
        // create a new temporary file
        int count = 1;
        Path tmpSplitsFile = null;
        try {
            do {
                tmpSplitsFile = new Path(this.splitsPath.getParent(), SPLITS_CACHE_FILE + "." + count);
                count++;
            } while (!fs.createNewFile(tmpSplitsFile));
        } catch (IOException ex) {
            throw new IOException("Could not create temp splits file", ex);
        }
        
        return tmpSplitsFile;
        
    }
    
    private void createCacheFile(FileSystem fs, Path tmpSplitsFile) {
        // now move the temporary file to the file cache
        try {
            fs.delete(this.splitsPath, false);
            // Note this rename will fail if the file already exists (i.e. the delete failed or somebody just replaced it)
            // but this is OK...
            if (!fs.rename(tmpSplitsFile, this.splitsPath)) {
                throw new IOException("Failed to rename temporary splits file");
            }
        } catch (Exception e) {
            log.warn("Unable to rename " + tmpSplitsFile + " to " + this.splitsPath + " probably because somebody else replaced it", e);
            try {
                fs.delete(tmpSplitsFile, false);
            } catch (Exception e2) {
                log.error("Unable to clean up " + tmpSplitsFile, e2);
            }
        }
        log.info("Updated the metadata table splits");
        
    }
    
    private Set<String> getIngestTableNames() {
        Set<String> tableNames = IngestJob.getTables(conf);
        if (tableNames.isEmpty()) {
            log.error("Missing data types or one of the following helperClass,readerClass,handlerClassNames,filterClassNames");
            throw new IllegalArgumentException("Missing data types or one of the following helperClass,readerClass,handlerClassNames,filterClassNames");
        }
        return tableNames;
    }
    
    /**
     * updates the splits file if the splits in the new file have not decreased beyond the maximum deviation allowed
     */
    public void update() {
        try {
            FileSystem fs = FileSystem.get(conf);
            initAccumuloHelper();
            Path tmpSplitsFile = createTempFile(fs);
            Map<String,Integer> tmpSplitsPerTable = writeSplits(fs, tmpSplitsFile);
            if (null == getFileStatus() || !exceedsMaxSplitsDeviation(tmpSplitsPerTable)) {
                log.info("updating splits file");
                createCacheFile(fs, tmpSplitsFile);
            } else {
                fs.delete(tmpSplitsFile, false);
            }
        } catch (IOException | AccumuloException | AccumuloSecurityException | TableNotFoundException ex) {
            log.error("Unable to update the splits file", ex);
        }
        
    }
    
    private Map<String,Integer> writeSplits(FileSystem fs, Path tmpSplitsFile) throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
                    IOException {
        TableOperations tops = null;
        initAccumuloHelper();
        try {
            tops = this.cbHelper.getConnector().tableOperations();
        } catch (AccumuloSecurityException | AccumuloException ex) {
            throw new AccumuloException("Could not get TableOperations", ex);
        }
        Set<String> tableNames = getIngestTableNames();
        Map<String,Integer> splitsPerTable = new HashMap<>();
        Map<String,Map<Text,String>> tmpSplitLocations = new HashMap<>();
        if (tops != null) {
            try (PrintStream out = new PrintStream(new BufferedOutputStream(fs.create(tmpSplitsFile)))) {
                this.splits = new HashMap<>();
                // gather the splits and write to PrintStream
                for (String table : tableNames) {
                    Map<Text,String> splitLocations = getSplitsWithLocation(table);
                    List<Text> splits = new ArrayList<>(tops.listSplits(table));
                    this.splits.put(table, splits);
                    splitsPerTable.put(table, splits.size());
                    tmpSplitLocations.put(table, splitLocations);
                    for (Text split : splits) {
                        out.println(table + "\t" + new String(Base64.encodeBase64(split.getBytes())) + "\t" + splitLocations.get(split));
                    }
                }
            } catch (IOException | AccumuloSecurityException | AccumuloException | TableNotFoundException ex) {
                log.error("Unable to write new splits file", ex);
                throw ex;
            }
        }
        return splitsPerTable;
    }
    
    private boolean exceedsMaxSplitsDeviation(Map<String,Integer> tmpSplitsPerTable) throws IOException {
        Map<String,Integer> currentSplitsPerTable = getCurrentSplitsPerTable();
        if (!currentSplitsPerTable.isEmpty()) {
            Set<String> currentTables = currentSplitsPerTable.keySet();
            for (String tableName : currentTables) {
                if (currentSplitsPerTable.get(tableName) * (1 - conf.getDouble(MAX_SPLIT_PERCENTAGE_DECREASE, DEFAULT_MAX_SPLIT_PERCENTAGE_DECREASE)) > tmpSplitsPerTable
                                .get(tableName)
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
        if (null == this.splits || null == splitLocations)
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
    
    private void read() throws IOException {
        log.info(String.format("Reading the metadata table splits (@ '%s')...", this.splitsPath.toUri().toString()));
        try (BufferedReader in = new BufferedReader(new InputStreamReader(FileSystem.get(conf).open(this.splitsPath)))) {
            readCache(in);
        } catch (IOException ex) {
            if (shouldRefreshSplits(this.conf)) {
                update();
            } else {
                throw new IOException("Could not read metadata table splits file", ex);
            }
        }
    }
    
    /**
     * Read a stream into a map of table name to split points
     * 
     * @param in
     *            an input stream containing the split points to read
     * @throws IOException
     */
    private void readCache(BufferedReader in) throws IOException {
        this.splits = new HashMap<>();
        this.splitLocations = new HashMap<>();
        String line;
        String tableName = null;
        List<Text> splits = null;
        SortedMap<Text,String> tmpSplitLocations = null;
        while ((line = in.readLine()) != null) {
            String[] parts = StringUtils.split(line, '\t');
            if (tableName == null || !tableName.equals(parts[0])) {
                tableName = parts[0];
                splits = new ArrayList<>();
                tmpSplitLocations = new TreeMap<>();
                this.splits.put(tableName, splits);
                this.splitLocations.put(tableName, tmpSplitLocations);
            }
            if (parts.length > 1) {
                splits.add(new Text(Base64.decodeBase64(parts[1].getBytes())));
            }
            if (parts.length > 2) {
                tmpSplitLocations.put(new Text(Base64.decodeBase64(parts[1].getBytes())), parts[2]);
            }
        }
        in.close();
    }
    
    /**
     * 
     * @param table
     * @return list of splits for the table
     * @throws java.io.IOException
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
     * @param maxSplits
     * @return
     * @throws IOException
     */
    public List<Text> getSplits(String table, int maxSplits) throws IOException {
        return trimSplits(getSplits(table), maxSplits);
    }
    
    /**
     * 
     * @return map of table name to list of splits for the table
     * @throws java.io.IOException
     */
    public Map<String,List<Text>> getSplits() throws IOException {
        if (null == this.splits)
            read();
        return new HashMap<>(splits);
    }

    /**
     *
     * @param table
     * @return map of splits to tablet locations for the table
     * @throws IOException
     */
    public Map<Text,String> getSplitsAndLocationByTable(String table) throws IOException {
        Map<Text,String> tableSplitsAndLocations;
        if(null == this.splitLocations)
            read();
        tableSplitsAndLocations = this.splitLocations.get(table) == null ? new HashMap<>() : this.splitLocations.get(table);
        return tableSplitsAndLocations;
    }

    /**
     * @return map of splits to table name to map of splits to table locations for the table
     * @throws java.io.IOException
     */
    public Map<String,Map<Text,String>> getSplitsAndLocation() throws IOException {
        if(null == this.splitLocations)
            read();
        return splitLocations;
    }

}
