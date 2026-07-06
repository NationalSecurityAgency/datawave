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
