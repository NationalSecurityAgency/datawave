package datawave.ingest.mapreduce.handler.edge;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;

import datawave.data.normalizer.DateNormalizer;
import datawave.data.type.util.NumericalEncoder;
import datawave.ingest.data.config.ConfigurationHelper;
import datawave.ingest.data.config.ingest.AccumuloHelper;
import datawave.util.StringUtils;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/*
 The edge table may contain different versions of the edge key structure. In order to be able to generate the new keys
 and correctly delete old keys we need to know the date that the new structure took effect.

 To record the switchover date an entry will be created in the Datawave Metadata table and then periodically that entry
 will be written to a cache file and put into the distributed cache during ingest.

 This class contains helper methods to create the entry in the datawave metadata table, read the entry from the table and
 write it out to a file, and read the file and return the mapping of key versions to their start date.

 */

public class EdgeKeyVersioningCache {
    
    private static final Logger log = Logger.getLogger(EdgeKeyVersioningCache.class);
    
    public static final String METADATA_TABLE_NAME = "metadata.table.name";
    public static final String KEY_VERSION_CACHE_DIR = "datawave.ingest.key.version.cache.dir";
    public static final String KEY_VERSION_DIST_CACHE_DIR = "distributed.cache.version.dir";
    
    public static final String DEFAULT_KEY_VERSION_CACHE_DIR = "/data/BulkIngest/jobCacheA/config";
    public static final String KEY_VERSION_CACHE_FILE = "edge-key-version.txt";
    private static final Text EDGE_KEY_VERSION_ROW = new Text("edge_key");
    
    private Configuration conf;
    private AccumuloHelper cbHelper = null;
    private Path versioningCache = null;
    private String metadataTableName;
    
    private Map<Integer,String> edgeKeyVersionDateChange = null;
    
    public EdgeKeyVersioningCache(Configuration conf) {
        this.conf = conf;
        this.versioningCache = new Path(this.conf.get(KEY_VERSION_CACHE_DIR, DEFAULT_KEY_VERSION_CACHE_DIR), KEY_VERSION_CACHE_FILE);
        this.metadataTableName = ConfigurationHelper.isNull(conf, METADATA_TABLE_NAME, String.class);
    }
    
    public Map<Integer,String> getEdgeKeyVersionDateChange() throws IOException {
        if (edgeKeyVersionDateChange == null) {
            readCache();
        }
        return edgeKeyVersionDateChange;
    }
    
    /**
     * Update the cache by reading the metadata table
     *
     * @param fs
     *            the hadoop filesystem containing the splits file
     * @throws AccumuloSecurityException
     *             for issues authenticating with accumulo
     * @throws AccumuloException
     *             for general issues with accumulo
     * @throws IOException
     *             for problems reading or writing to the cache
     * @throws TableNotFoundException
     *             if the table is not found
     */
    
    public void updateCache(FileSystem fs) throws AccumuloSecurityException, AccumuloException, IOException, TableNotFoundException {
        log.info("Reading the " + metadataTableName + " for edge key version ...");
        if (this.cbHelper == null) {
            this.cbHelper = new AccumuloHelper();
            this.cbHelper.setup(conf);
        }
        
        // a temporary date map. using tree map so we can print out the version/dates in order
        Map<Integer,String> versionDates = new TreeMap<>();
        try (AccumuloClient client = cbHelper.newClient()) {
            ensureTableExists(client);
            
            try (org.apache.accumulo.core.client.Scanner scanner = client.createScanner(metadataTableName, new Authorizations())) {
                scanner.setRange(new Range(EDGE_KEY_VERSION_ROW));
                
                // Read the edge key version dates from the datawave metadata table
                // If there happen to be the same version numbers but with different dates then the one with the earliest date is kept
                for (Map.Entry<Key,Value> entry : scanner) {
                    String cq = entry.getKey().getColumnQualifier().toString();
                    
                    String parts[] = StringUtils.split(cq, '/');
                    
                    Integer versionNum = NumericalEncoder.decode(parts[0]).intValue();
                    
                    // Earlier dates will sort first so only remember the first date for each version number
                    if (!versionDates.containsKey(versionNum)) {
                        versionDates.put(versionNum, parts[1]);
                    }
                }
            }
            // If Datawave Metadatatable does not have any key versions automatically populate it with one
            if (versionDates.isEmpty()) {
                /*
                 * If no key versions were found, then we're most likely initializing a new system. Therefore seeding with epoch date, which should prevent the
                 * "old" edge key from being created...that is, with EdgeKey.DATE_TYPE.OLD_EVENT (See ProtobufEdgeDataTypeHandler.writeEdges)
                 */
                Date then = new Date(0);
                log.warn("Could not find any edge key version entries in the " + metadataTableName + " table. Automatically seeding with date: " + then);
                String dateString = seedMetadataTable(client, then.getTime(), 1);
                versionDates.put(1, dateString);
            }
        }
        
        // create a new temporary file
        int count = 1;
        Path tmpVersionFile = new Path(this.versioningCache.getParent(), KEY_VERSION_CACHE_FILE + "." + count);
        while (!fs.createNewFile(tmpVersionFile)) {
            count++;
            tmpVersionFile = new Path(this.versioningCache.getParent(), KEY_VERSION_CACHE_FILE + "." + count);
        }
        
        // now attempt to write them out
        try {
            try (PrintStream out = new PrintStream(new BufferedOutputStream(fs.create(tmpVersionFile)))) {
                for (Map.Entry<Integer,String> pair : versionDates.entrySet()) {
                    out.println(pair.getKey() + "\t" + pair.getValue());
                }
            }
            
            // now move the temporary file to the file cache
            try {
                fs.delete(this.versioningCache, false);
                // Note this rename will fail if the file already exists (i.e. the delete failed or somebody just replaced it)
                // but this is OK...
                if (!fs.rename(tmpVersionFile, this.versioningCache)) {
                    throw new IOException("Failed to rename temporary splits file");
                }
            } catch (Exception e) {
                log.warn("Unable to rename " + tmpVersionFile + " to " + this.versioningCache + " probably because somebody else replaced it", e);
                try {
                    fs.delete(tmpVersionFile, false);
                } catch (Exception e2) {
                    log.error("Unable to clean up " + tmpVersionFile, e2);
                }
            }
        } catch (Exception e) {
            log.error("Unable to create new edge key version cache file", e);
        }
    }
    
    private void readCache() throws IOException {
        BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(getFileFromDistributedFileCache())));
        log.info("Got edge key version cache file from distributed cache.");
        this.edgeKeyVersionDateChange = readCache(in);
    }
    
    protected File getFileFromDistributedFileCache() {
        String cachFileDir = conf.get(KEY_VERSION_DIST_CACHE_DIR, null);
        return new File(cachFileDir, KEY_VERSION_CACHE_FILE);
    }
    
    /**
     * Read a stream into a map of version nums to start date
     *
     * @param in
     *            an input stream containing the split points to read
     * @return a map of table name to split points
     * @throws IOException
     *             for problems reading or writing to the cache
     */
    private Map<Integer,String> readCache(BufferedReader in) throws IOException {
        
        String line;
        Map<Integer,String> tmpVersions = new TreeMap<>();
        while ((line = in.readLine()) != null) {
            String parts[] = StringUtils.split(line, '\t');
            tmpVersions.put(Integer.parseInt(parts[0]), parts[1]);
        }
        in.close();
        
        return tmpVersions;
    }
    
    /**
     *
     * Key structure: edge_key version:num/yyyy-MM-ddThh:MM:ss:000Z []
     *
     * @param time
     *            the time of the entry
     * @param keyVersionNum
     *            the key version number
     * @throws AccumuloSecurityException
     *             for issues authenticating with accumulo
     * @throws AccumuloException
     *             for general issues with accumulo
     * @throws TableNotFoundException
     *             if the table was not found
     */
    public void createMetadataEntry(long time, int keyVersionNum) throws Exception {
        if (this.cbHelper == null) {
            this.cbHelper = new AccumuloHelper();
            this.cbHelper.setup(conf);
        }
        cbHelper.setup(conf);
        
        try (AccumuloClient client = cbHelper.newClient()) {
            
            ensureTableExists(client);
            
            seedMetadataTable(client, time, keyVersionNum);
        }
    }
    
    /*
     * Creates the Datawave Metadata table if it does not exist. Used for first time users who don't have any tables in their accumulo instance. This only
     * creates the table the ingest job will configure it
     */
    private void ensureTableExists(AccumuloClient client) throws AccumuloSecurityException, AccumuloException {
        TableOperations tops = client.tableOperations();
        if (!tops.exists(metadataTableName)) {
            log.info("Creating table: " + metadataTableName);
            try {
                tops.create(metadataTableName);
            } catch (TableExistsException e) {
                log.error(metadataTableName + " already exists someone got here first.");
            }
        }
    }
    
    private String seedMetadataTable(AccumuloClient client, long time, int keyVersionNum) throws TableNotFoundException, MutationsRejectedException {
        Value emptyVal = new Value();
        SimpleDateFormat dateFormat = new SimpleDateFormat(DateNormalizer.ISO_8601_FORMAT_STRING);
        String dateString = dateFormat.format(new Date(time));
        try (BatchWriter recordWriter = client.createBatchWriter(metadataTableName, new BatchWriterConfig())) {
            String normalizedVersionNum = NumericalEncoder.encode(Integer.toString(keyVersionNum));
            String rowID = "edge_key";
            String columnFamily = "version";
            String columnQualifier = normalizedVersionNum + "/" + dateString;
            
            Mutation m = new Mutation(rowID);
            
            m.put(new Text(columnFamily), new Text(columnQualifier), emptyVal);
            
            recordWriter.addMutation(m);
        }
        return dateString;
    }
    
    public void setCbHelper(AccumuloHelper cbHelper) {
        this.cbHelper = cbHelper;
    }
}
