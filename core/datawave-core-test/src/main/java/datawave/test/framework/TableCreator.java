package datawave.test.framework;

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.iterators.IteratorUtil;

import datawave.table.constants.TableName;
import datawave.test.framework.util.MacTestUtil;

/**
 * Utility that creates and configures common tables
 */
public class TableCreator {

    private TableCreator() {
        // enforce static access
    }

    public static void createTables(AccumuloClient client) throws Exception {
        TableOperations tops = client.tableOperations();

        // TODO: make this config based
        tops.create(TableName.SHARD);
        createShardIndex(tops);
        tops.create(TableName.METADATA);
    }

    /**
     * Create the shard index table and install the uid aggregator on every scope.
     * <p>
     * This is not optional configuration.
     * {@link datawave.test.framework.writers.ShardIndexTableWriter#write(org.apache.accumulo.core.client.AccumuloClient, java.util.List, int)} writes one
     * single-uid {@code Uid.List} per event, and every event sharing a value within a shard produces an identical key. The aggregator is what merges them.
     * <p>
     * Priority 19 is deliberate: it places the aggregator <b>below</b> the default versioning iterator at 20, so aggregation happens before versioning discards
     * the duplicate keys. Raise it above 20 and the index silently degrades to one uid per shard.
     *
     * @param tops
     *            the {@link TableOperations}
     * @throws Exception
     *             if the table cannot be created or configured
     */
    public static void createShardIndex(TableOperations tops) throws Exception {
        tops.create(TableName.SHARD_INDEX);

        Map<String,String> additions = new HashMap<>();
        IteratorUtil.IteratorScope[] scopes = IteratorUtil.IteratorScope.values();
        for (IteratorUtil.IteratorScope scope : scopes) {
            String name = "table.iterator." + scope.name() + ".UIDAggregator";
            String opt = "table.iterator." + scope.name() + ".UIDAggregator.opt.*";

            additions.put(name, "19,datawave.iterators.TotalAggregatingIterator");
            additions.put(opt, "datawave.ingest.table.aggregator.GlobalIndexUidAggregator");
        }
        MacTestUtil.addPropertiesAndWait(tops, TableName.SHARD_INDEX, additions);
    }
}
