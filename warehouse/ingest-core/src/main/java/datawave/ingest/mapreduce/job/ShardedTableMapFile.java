package datawave.ingest.mapreduce.job;

import datawave.ingest.data.config.ConfigurationHelper;
import datawave.ingest.data.config.ingest.AccumuloHelper;
import datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;
import datawave.util.StringUtils;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataServicer;
import org.apache.accumulo.core.client.impl.Credentials;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;

/**
 * Extracted from IngestJob
 */
public class ShardedTableMapFile {
    private static final String PREFIX = ShardedTableMapFile.class.getName();
    public static final String TABLE_NAMES = "job.table.names";
    public static final String SHARD_TSERVER_MAP_FILE = PREFIX + ".shardTServerMapFile";
    public static final String SPLIT_WORK_DIR = "split.work.dir";
    
    public static final String CONFIGURED_SHARDED_TABLE_NAMES = ShardedDataTypeHandler.SHARDED_TNAMES + ".configured";
    private static final Logger log = Logger.getLogger(ShardedTableMapFile.class);
    public static final String SHARDED_MAP_FILE_PATHS_RAW = "shardedMap.file.paths.raw";
    
    public static void setupFile(Configuration conf) throws IOException, URISyntaxException, AccumuloSecurityException, AccumuloException {
        Map<String,Path> map = loadMap(conf);
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
    
    public static void addToConf(Configuration conf, Map<String,Path> map) {
        for (Map.Entry<String,Path> entry : map.entrySet()) {
            log.info("Loading sharded partitioner for table '" + entry.getKey() + "' with shardedMapFile '" + entry.getValue() + "'");
            conf.set(SHARD_TSERVER_MAP_FILE + "." + entry.getKey(), entry.getValue().toString());
        }
        Set<String> var = map.keySet();
        conf.setStrings(CONFIGURED_SHARDED_TABLE_NAMES, var.toArray(new String[var.size()]));
    }
    
    private static Map<String,Path> loadMap(Configuration conf) throws IOException, URISyntaxException, AccumuloSecurityException, AccumuloException {
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
                Connector conn = accumuloHelper.getConnector();
                Credentials credentials = accumuloHelper.getCredentials();
                shardedMapFile = createShardedMapFile(log, conf, workDir, conn.getInstance(), credentials, shardedTableName);
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
     * @param instance
     *            Accumulo instance to query shard locations
     * @param credentials
     *            Accumulo access credentials for querying shard locations
     * @param shardedTableName
     *            name of the shard table--the table whose locations we are querying
     * @return the path to the sharded table map file
     * @throws IOException
     * @throws URISyntaxException
     */
    public static Path createShardedMapFile(Logger log, Configuration conf, Path workDir, Instance instance, Credentials credentials, String shardedTableName)
                    throws IOException, URISyntaxException {
        Path shardedMapFile = null;
        
        if (null != shardedTableName) {
            // Read all the metadata entries for the sharded table so that we can
            // get the mapping of shard IDs to tablet locations.
            log.info("Reading metadata entries for " + shardedTableName);
            
            SortedMap<KeyExtent,String> locations = getLocations(log, instance, credentials, shardedTableName);
            
            // Now write all of the assignments out to a file stored in HDFS
            // we're ok with putting the sharded table file in the hdfs workdir. why is that not good enough for the non sharded splits?
            shardedMapFile = new Path(workDir, shardedTableName + "_shards.lst");
            log.info("Writing shard assignments to " + shardedMapFile);
            long count = writeSplitsFile(locations, shardedMapFile, conf);
            log.info("Wrote " + count + " shard assignments to " + shardedMapFile);
        }
        
        return shardedMapFile;
    }
    
    /**
     * Continually scans the metdata table attempting to get the split locations for the shard table.
     *
     * @param log
     *            logger for reporting errors
     * @param instance
     *            Accumulo instance to query shard locations
     * @param credentials
     *            Accumulo access credentials for querying shard locations
     * @param shardedTableName
     *            name of the shard table--the table whose locations we are querying
     * @return a map of KeyExtent to the location of those key extents in accumulo
     */
    public static SortedMap<KeyExtent,String> getLocations(Logger log, Instance instance, Credentials credentials, String shardedTableName) {
        
        TreeMap<KeyExtent,String> locations = new TreeMap<>();
        while (true) {
            try {
                // re-create the locations so that we don't re-use stale metadata information.
                locations.clear();
                MetadataServicer servicer = MetadataServicer.forTableName(
                                new ClientContext(instance, credentials, AccumuloConfiguration.getDefaultConfiguration()), shardedTableName);
                servicer.getTabletLocations(locations);
                break;
            } catch (Exception e) {
                log.warn(e.getMessage() + " ... retrying ...");
                UtilWaitThread.sleep(3000);
            }
        }
        return locations;
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
    public static long writeSplitsFile(Map<KeyExtent,String> splits, Path file, Configuration conf) throws IOException {
        FileSystem fs = file.getFileSystem(conf);
        if (fs.exists(file))
            fs.delete(file, false);
        
        long count = 0;
        SequenceFile.Writer writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(file), SequenceFile.Writer.keyClass(Text.class),
                        SequenceFile.Writer.valueClass(Text.class));
        for (Map.Entry<KeyExtent,String> entry : splits.entrySet()) {
            KeyExtent extent = entry.getKey();
            if (extent.getEndRow() != null && entry.getValue() != null) {
                count++;
                Text location = new Text(entry.getValue().replaceAll("[\\.:]", "_"));
                writer.append(extent.getEndRow(), location);
            }
        }
        writer.close();
        return count;
    }
    
}
