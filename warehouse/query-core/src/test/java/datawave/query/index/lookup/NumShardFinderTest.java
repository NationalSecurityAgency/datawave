package datawave.query.index.lookup;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.ingest.mapreduce.handler.shard.NumShards;
import datawave.query.index.lookup.RangeStream.NumShardFinder;
import datawave.table.constants.TableName;

/**
 * Verifies that the num shards cache is read from the configured metadata table.
 */
public class NumShardFinderTest {

    // A namespaced name and a bare one such as TableName.METADATA are both perfectly good names for
    // this table; a deployment picks either. What matters is that the finder scans the one it was
    // configured with, so this test uses a namespaced name only because it differs from the default.
    private static final String NAMESPACE = "datawave";
    private static final String METADATA_TABLE = NAMESPACE + ".metadata";

    private AccumuloClient client;

    @BeforeEach
    public void setup() throws Exception {
        // An instance is shared by name, so an unnamed one keeps each test from seeing the last one's tables and authorizations.
        client = new InMemoryAccumuloClient("", new InMemoryInstance());
        client.namespaceOperations().create(NAMESPACE);
        client.tableOperations().create(METADATA_TABLE);

        try (BatchWriter writer = client.createBatchWriter(METADATA_TABLE, new BatchWriterConfig())) {
            Mutation m = new Mutation(NumShards.NUM_SHARDS);
            m.put(NumShards.NUM_SHARDS_CF.toString(), "19000101_3", new Value());
            writer.addMutation(m);
        }
    }

    @Test
    public void testReadsConfiguredMetadataTable() {
        NumShardFinder finder = new NumShardFinder(client, METADATA_TABLE, null, null);
        assertEquals(3, finder.getNumShards("20250519"));
    }

    /**
     * A day below every entry has no floor to fall back to, so it resolves to no shards rather than to the earliest count.
     */
    @Test
    public void testDayBeforeFirstEntryFindsNoShards() {
        NumShardFinder finder = new NumShardFinder(client, METADATA_TABLE, null, null);
        assertEquals(0, finder.getNumShards("18991231"));
    }

    /**
     * Scanning a name this deployment did not create finds no shards. The missing table is swallowed rather than raised, which is what let a name the
     * deployment never configured drop results silently instead of failing the query.
     */
    @Test
    public void testMissingMetadataTableYieldsNoShards() {
        NumShardFinder finder = new NumShardFinder(client, TableName.METADATA, null, null);
        assertEquals(0, finder.getNumShards("20250519"));
    }

    @Test
    public void testEmptyAuthorizationsStillReadEntry() throws Exception {
        client.securityOperations().changeUserAuthorizations(client.whoami(), new Authorizations());

        NumShardFinder finder = new NumShardFinder(client, METADATA_TABLE, null, null);
        assertEquals(3, finder.getNumShards("20250519"));
    }
}
