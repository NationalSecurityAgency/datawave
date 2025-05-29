package datawave.query.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

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
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

public class NumShardMetadataHelper {

    private static final Logger log = Logger.getLogger(NumShardMetadataHelper.class);

    public static ArrayList<String> getNumShardEntries(AccumuloClient accumuloClient, String metadataTableName, Text numShards, Text numShardsCF)
                    throws AccumuloException, AccumuloSecurityException, TableNotFoundException, IOException {
        log.info("Reading the " + metadataTableName + " for multiple numshards configuration");

//        if (accumuloClient == null) {
//            throw new AccumuloException("Accumulo client is null.");
//        }
        ArrayList<String> nsEntries = new ArrayList<>();

            ensureTableExists(accumuloClient, metadataTableName);

            try (Scanner scanner = accumuloClient.createScanner(metadataTableName, new Authorizations())) {
                scanner.setRange(Range.exact(numShards, numShardsCF));

                for (Map.Entry<Key, Value> entry : scanner) {
                    nsEntries.add(entry.getKey().getColumnQualifier().toString());
                }
            }


        return nsEntries;

    }

    /**
     *     public void updateCache() throws AccumuloException, AccumuloSecurityException, TableNotFoundException, IOException {
     *         FileSystem fs = this.numShardsCachePath.getFileSystem(this.conf);
     *         String metadataTableName = ConfigurationHelper.isNull(this.conf, ShardedDataTypeHandler.METADATA_TABLE_NAME, String.class);
     *         log.info("Reading the " + metadataTableName + " for multiple numshards configuration");
     *
     *         if (this.aHelper == null) {
     *             this.aHelper = new AccumuloHelper();
     *             this.aHelper.setup(conf);
     *         }
     *
     *         ArrayList<String> nsEntries = new ArrayList<>();
     *         try (AccumuloClient client = aHelper.newClient()) {
     *             ensureTableExists(client, metadataTableName);
     *
     *             try (Scanner scanner = client.createScanner(metadataTableName, new Authorizations())) {
     *                 scanner.setRange(Range.exact(NUM_SHARDS, NUM_SHARDS_CF));
     *
     *                 for (Map.Entry<Key,Value> entry : scanner) {
     *                     nsEntries.add(entry.getKey().getColumnQualifier().toString());
     *                 }
     *             }
     *         }
     *
     *         // create a new temporary file
     *         int count = 1;
     *         Path tmpShardCacheFile = new Path(this.numShardsCachePath.getParent(), numShardsCachePath.getName() + "." + count);
     */

    private static void ensureTableExists(AccumuloClient client, String metadataTableName) throws AccumuloException, AccumuloSecurityException {
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

}
