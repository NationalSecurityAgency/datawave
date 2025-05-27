package datawave.query.util;

import datawave.ingest.data.config.ConfigurationHelper;
import datawave.ingest.data.config.ingest.AccumuloHelper;
import datawave.ingest.mapreduce.handler.shard.NumShards;
import datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;
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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Map;

public class NumShardMetadataHelper {

    private static final Logger log = Logger.getLogger(NumShardMetadataHelper.class);

    public static ArrayList<String> updateCache(Path numShardsCachePath, Configuration conf, AccumuloHelper aHelper, Text numShards, Text numShardsCF, int maxNumberOfRetriesCacheFile) throws AccumuloException, AccumuloSecurityException, TableNotFoundException, IOException {
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

                for (Map.Entry<Key, Value> entry : scanner) {
                    nsEntries.add(entry.getKey().getColumnQualifier().toString());
                }
            }
        }

        return nsEntries;

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
