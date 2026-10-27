package datawave.query.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.accumulo.inmemory.InMemoryAccumulo;
import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.data.type.LcType;
import datawave.table.constants.TableName;

class AbstractIngestTest {

    private static final Authorizations AUTHS = new Authorizations("ALL");

    private AccumuloClient client;
    private AbstractIngest ingest;

    @BeforeEach
    void setUp() throws Exception {
        client = new InMemoryAccumuloClient("root", InMemoryAccumulo.getInstance(UUID.randomUUID().toString()));
        ingest = new AbstractIngest(client, AUTHS);
    }

    @AfterEach
    void tearDown() {
        client.close();
    }

    @Test
    void reverseOnlyFieldWritesReversedGlobalIndexWithoutForwardIndex() throws Exception {
        ingest.registerField("NAME", new LcType());
        ingest.registerColumns("NAME", List.of("ri", "e"));
        ingest.writeFV(1, "NAME", "Alice");

        try (Scanner reverse = client.createScanner(TableName.SHARD_RINDEX, AUTHS);
                        Scanner forward = client.createScanner(TableName.SHARD_INDEX, AUTHS);
                        Scanner shard = client.createScanner(TableName.SHARD, AUTHS)) {
            Map.Entry<Key,Value> entry = reverse.iterator().next();
            assertEquals("ecila", entry.getKey().getRow().toString());
            assertEquals("NAME", entry.getKey().getColumnFamily().toString());
            assertEquals(ingest.getRow() + "\0" + ingest.getDatatype(), entry.getKey().getColumnQualifier().toString());
            assertFalse(forward.iterator().hasNext());
            for (Map.Entry<Key,Value> shardEntry : shard) {
                assertFalse(shardEntry.getKey().getColumnFamily().toString().startsWith("fi\0"));
            }
        }
    }
}
