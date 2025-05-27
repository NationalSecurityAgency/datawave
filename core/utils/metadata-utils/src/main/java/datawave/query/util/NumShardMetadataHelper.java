package datawave.query.util;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.PrintStream;
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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import datawave.ingest.data.config.ConfigurationHelper;
import datawave.ingest.data.config.ingest.AccumuloHelper;
import datawave.ingest.mapreduce.handler.shard.NumShards;
import datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;

public class NumShardMetadataHelper {

    private static final Logger log = Logger.getLogger(NumShardMetadataHelper.class);

    public static void updateCache(Path numShardsCachePath, Configuration conf, AccumuloHelper aHelper, Text numShards, Text numShardsCF,
                    int maxNumberOfRetriesCacheFile) throws AccumuloException, AccumuloSecurityException, TableNotFoundException, IOException {
        FileSystem fs = numShardsCachePath.getFileSystem(conf);
        String metadataTableName = ConfigurationHelper.isNull(conf, ShardedDataTypeHandler.METADATA_TABLE_NAME, String.class);
        log.info("Reading the " + metadataTableName + " for multiple numshards configuration");

        if (aHelper == null) {
            aHelper = new AccumuloHelper();
            aHelper.setup(conf);
        }

        ArrayList<String> nsEntries = new ArrayList<>();
        try (AccumuloClient client = aHelper.newClient()) {
            ensureTableExists(client, metadataTableName);

            try (Scanner scanner = client.createScanner(metadataTableName, new Authorizations())) {
                scanner.setRange(Range.exact(numShards, numShardsCF));

                for (Map.Entry<Key,Value> entry : scanner) {
                    nsEntries.add(entry.getKey().getColumnQualifier().toString());
                }
            }
        }

        // create a new temporary file
        int count = 1;
        Path tmpShardCacheFile = new Path(numShardsCachePath.getParent(), numShardsCachePath.getName() + "." + count);

        while (!fs.createNewFile(tmpShardCacheFile) && count < maxNumberOfRetriesCacheFile) {
            count++;
            tmpShardCacheFile = new Path(numShardsCachePath.getParent(), numShardsCachePath.getName() + "." + count);
        }

        // now attempt to write them out
        try (PrintStream out = new PrintStream(new BufferedOutputStream(fs.create(tmpShardCacheFile)))) {

            for (String nsEntry : nsEntries) {
                out.println(nsEntry);
            }
            out.close();

            boolean isCacheLoaded = false;
            int numOfTries = 0;

            while (!isCacheLoaded && numOfTries++ < maxNumberOfRetriesCacheFile) {
                // now move the temporary file to the file cache
                try {
                    fs.delete(numShardsCachePath, false);
                    // Note this rename will fail if the file already exists (i.e. the delete failed or somebody just replaced it)
                    // but this is OK...
                    if (!fs.rename(tmpShardCacheFile, numShardsCachePath)) {
                        throw new IOException("Failed to rename temporary multiple numshards cache file");
                    }

                    isCacheLoaded = true;
                } catch (Exception e) {
                    log.warn("Unable to rename " + tmpShardCacheFile + " to " + numShardsCachePath + " probably because somebody else replaced it", e);
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
