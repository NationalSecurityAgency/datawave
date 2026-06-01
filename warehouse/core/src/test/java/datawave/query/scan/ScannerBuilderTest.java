package datawave.query.scan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;

public class ScannerBuilderTest {

    private static final InMemoryInstance instance = new InMemoryInstance(ScannerBuilderTest.class.getName());

    private static AccumuloClient client;
    private static final String tableName = "shard";
    private final Authorizations auths = new Authorizations("VIZ-A");
    private final Range range = Range.exact("row");

    private static final Long ts = System.currentTimeMillis();
    private static final Key key = new Key("row", "cf", "cq", "VIZ-A", ts);
    private static final Value EMPTY_VALUE = new Value();

    @BeforeAll
    public static void beforeAll() throws Exception {
        client = new InMemoryAccumuloClient("user", instance);

        TableOperations tops = client.tableOperations();

        // create or recreate the table
        if (tops.exists(tableName)) {
            tops.delete(tableName);
        }
        tops.create(tableName);

        try (BatchWriter bw = client.createBatchWriter(tableName)) {
            Mutation m = new Mutation(key.getRow());
            m.put(key.getColumnFamily(), key.getColumnQualifier(), key.getColumnVisibilityParsed(), key.getTimestamp(), EMPTY_VALUE);
            bw.addMutation(m);
        }
    }

    @Test
    public void testClientNotSet() {
        Exception e = assertThrows(NullPointerException.class, () -> ScannerBuilder.create(null));
        String expected = "AccumuloClient must be set";
        assertEquals(expected, e.getMessage());
    }

    @Test
    public void testTableNameNotSet() {
        ScannerBuilder builder = ScannerBuilder.create(client);

        Exception e = assertThrows(NullPointerException.class, () -> buildAndScan(builder));
        String expected = "Table name must be set";
        assertEquals(expected, e.getMessage());
    }

    @Test
    public void testTableNotFound() {
        //  @formatter:off
        ScannerBuilder builder = ScannerBuilder.create(client)
                .setAuthorizations(auths)
                .setTableName("Shangri-La");
        //  @formatter:on
        Exception e = assertThrows(RuntimeException.class, () -> buildAndScan(builder));
        String expected = "ScannerBuilder could not create scanner";
        assertEquals(expected, e.getMessage());
    }

    @Test
    public void testAuthorizationsNotSet() {
        ScannerBuilder builder = ScannerBuilder.create(client);
        builder.setTableName(tableName);

        Exception e = assertThrows(NullPointerException.class, () -> buildAndScan(builder));
        String expected = "Authorizations must be set";
        assertEquals(expected, e.getMessage());
    }

    @Test
    public void testConsistencyLevelImmediate() {
        //  @formatter:off
        ScannerBuilder builder = ScannerBuilder.create(client)
                .setTableName(tableName)
                .setAuthorizations(auths)
                .setConsistencyLevel(ConsistencyLevel.IMMEDIATE);
        //  @formatter:on

        buildAndScan(builder);
        assertEquals(ConsistencyLevel.IMMEDIATE, builder.getConsistencyLevel());
    }

    @Test
    public void testConsistencyLevelEventual() {
        //  @formatter:off
        ScannerBuilder builder = ScannerBuilder.create(client)
                .setTableName(tableName)
                .setAuthorizations(auths)
                .setConsistencyLevel(ConsistencyLevel.EVENTUAL);
        //  @formatter:on

        buildAndScan(builder);
        assertEquals(ConsistencyLevel.EVENTUAL, builder.getConsistencyLevel());
    }

    @Test
    public void testConsistencyLevelImmediateViaString() {
        //  @formatter:off
        ScannerBuilder builder = ScannerBuilder.create(client)
                .setTableName(tableName)
                .setAuthorizations(auths)
                .setConsistencyLevel("IMMEDIATE");
        //  @formatter:on

        buildAndScan(builder);
        assertEquals(ConsistencyLevel.IMMEDIATE, builder.getConsistencyLevel());
    }

    @Test
    public void testConsistencyLevelEventualViaString() {
        //  @formatter:off
        ScannerBuilder builder = ScannerBuilder.create(client)
                .setTableName(tableName)
                .setAuthorizations(auths)
                .setConsistencyLevel("EVENTUAL");
        //  @formatter:on

        buildAndScan(builder);
        assertEquals(ConsistencyLevel.EVENTUAL, builder.getConsistencyLevel());
    }

    @Test
    public void testIncorrectConsistencyLevel() {
        Exception e = assertThrows(IllegalArgumentException.class, () -> ScannerBuilder.create(client).setConsistencyLevel("sometime"));
        String expected = "No enum constant org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel.sometime";
        assertEquals(expected, e.getMessage());
    }

    @Test
    public void testSetScanType() {
        //  @formatter:off
        ScannerBuilder builder = ScannerBuilder.create(client)
                .setTableName(tableName)
                .setAuthorizations(auths)
                .setScanType("executor-pool");
        //  @formatter:on

        buildAndScan(builder);
        assertEquals("executor-pool", builder.getExecutionHints().get("scan_type"));
    }

    @Test
    public void testSetScanPriority() {
        //  @formatter:off
        ScannerBuilder builder = ScannerBuilder.create(client)
                .setTableName(tableName)
                .setAuthorizations(auths)
                .setScanPriority(17);
        //  @formatter:on

        buildAndScan(builder);
        assertEquals("17", builder.getExecutionHints().get("priority"));
    }

    // set two execution hints
    @Test
    public void testSetScanTypeAndScanPriority() {
        //  @formatter:off
        ScannerBuilder builder = ScannerBuilder.create(client)
                .setTableName(tableName)
                .setAuthorizations(auths)
                .setScanType("executor-pool")
                .setScanPriority(17);
        //  @formatter:on

        buildAndScan(builder);
        assertEquals("executor-pool", builder.getExecutionHints().get("scan_type"));
        assertEquals("17", builder.getExecutionHints().get("priority"));
    }

    /**
     * Create and execute the scanner
     *
     * @param builder
     *            the {@link ScannerBuilder}
     */
    private void buildAndScan(ScannerBuilder builder) {
        try (Scanner scanner = builder.build()) {
            scanner.setRange(range);
            int count = 0;
            for (Map.Entry<Key,Value> entry : scanner) {
                count++;
                assertEquals(key, entry.getKey());
            }
            assertEquals(1, count);
        }

        assertEquals(tableName, builder.getTableName());
        assertEquals(auths, builder.getAuthorizations());
    }

}
