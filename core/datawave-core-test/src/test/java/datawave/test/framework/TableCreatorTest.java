package datawave.test.framework;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.table.constants.TableName;

class TableCreatorTest {

    private AccumuloClient client;

    @BeforeEach
    void createClient() throws Exception {
        InMemoryInstance instance = new InMemoryInstance(getClass().getSimpleName() + "-" + UUID.randomUUID());
        client = new InMemoryAccumuloClient("root", instance);
    }

    @Test
    void testCreatesEveryTableTheWritersPopulate() throws Exception {
        TableCreator.createTables(client);

        TableOperations tops = client.tableOperations();
        assertTrue(tops.exists(TableName.SHARD), "shard table should exist");
        assertTrue(tops.exists(TableName.SHARD_INDEX), "shard index should exist");
        assertTrue(tops.exists(TableName.SHARD_RINDEX), "reverse index should exist");
        assertTrue(tops.exists(TableName.METADATA), "metadata table should exist");
    }

    /**
     * Only the global indexes aggregate: the shard and metadata tables hold one entry per key and would be corrupted by a combiner that merged their values.
     */
    @Test
    void testOnlyTheGlobalIndexesCarryTheAggregator() throws Exception {
        TableCreator.createTables(client);

        assertTrue(hasUidAggregator(TableName.SHARD_INDEX), "the shard index relies on the aggregator");
        assertTrue(hasUidAggregator(TableName.SHARD_RINDEX), "the reverse index relies on the aggregator");
        assertFalse(hasUidAggregator(TableName.SHARD), "the shard table must not aggregate");
        assertFalse(hasUidAggregator(TableName.METADATA), "the metadata table must not aggregate");
    }

    private boolean hasUidAggregator(String table) throws Exception {
        Map<String,String> properties = new HashMap<>();
        for (Map.Entry<String,String> property : client.tableOperations().getProperties(table)) {
            properties.put(property.getKey(), property.getValue());
        }
        return properties.keySet().stream().anyMatch(key -> key.contains("UIDAggregator"));
    }
}
