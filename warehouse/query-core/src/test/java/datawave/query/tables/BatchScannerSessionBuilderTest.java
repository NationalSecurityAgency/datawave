package datawave.query.tables;

import static org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.VersioningIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.core.query.configuration.QueryData;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.query.tables.async.ScannerChunk;
import datawave.webservice.query.util.QueryUncaughtExceptionHandler;

public class BatchScannerSessionBuilderTest implements BaseScannerSessionTest<BatchScannerSessionBuilder> {

    private static final InMemoryInstance instance = new InMemoryInstance(BatchScannerSessionBuilderTest.class.getName());

    private static AccumuloClient client;
    private static final String tableName = "shard";
    private final Set<Authorizations> authorizations = Set.of(new Authorizations("VIZ-A", "VIZ-B", "VIZ-C"));

    private static final Long ts = System.currentTimeMillis();
    private static final Key key = new Key("row", "cf", "cq", "VIZ-A", ts);
    private static final Value EMPTY_VALUE = new Value();

    private final Query query = new QueryImpl();

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

    @BeforeEach
    public void beforeEach() throws Exception {
        this.query.setUncaughtExceptionHandler(new QueryUncaughtExceptionHandler());
    }

    @Test
    @Override
    public void testClientNotSet() {
        Exception e = assertThrows(NullPointerException.class, () -> BatchScannerSessionBuilder.create(null));
        String expected = "AccumuloClient must be set";
        assertEquals(expected, e.getMessage());
    }

    @Test
    @Override
    public void testTableNameNotSet() {
        BatchScannerSessionBuilder builder = BatchScannerSessionBuilder.create(client);
        Exception e = assertThrows(NullPointerException.class, () -> buildAndScan(builder));
        String expected = "TableName must be set";
        assertEquals(expected, e.getMessage());
    }

    @Test
    @Override
    public void testAuthorizationsNotSet() {
        //  @formatter:off
        BatchScannerSessionBuilder builder = BatchScannerSessionBuilder.create(client)
                .setTableName(tableName);
        //  @formatter:on
        Exception e = assertThrows(NullPointerException.class, () -> buildAndScan(builder));
        String expected = "Authorizations must be set";
        assertEquals(expected, e.getMessage());
    }

    @Test
    @Override
    public void testInvalidResourceQueueSize() {
        BatchScannerSessionBuilder builder = create().setResourceQueueSize(0);
        Exception e = assertThrows(IllegalArgumentException.class, () -> buildAndScan(builder));
        String expected = "ResourceQueueSize must be greater than 0";
        assertEquals(expected, e.getMessage());
    }

    @Test
    @Override
    public void testInvalidResultQueueSize() {
        BatchScannerSessionBuilder builder = create().setResultQueueSize(0);
        Exception e = assertThrows(IllegalArgumentException.class, () -> buildAndScan(builder));
        String expected = "ResultQueueSize must be greater than 0";
        assertEquals(expected, e.getMessage());
    }

    @Test
    @Override
    public void testQueryNotSet() {
        //  @formatter:off
        BatchScannerSessionBuilder builder = BatchScannerSessionBuilder.create(client)
                .setTableName(tableName)
                .setAuthorizations(authorizations);
        //  @formatter:on
        Exception e = assertThrows(NullPointerException.class, () -> buildAndScan(builder));
        String expected = "Query must be set";
        assertEquals(expected, e.getMessage());
    }

    @Test
    @Override
    public void testTableNotFound() {
        //  @formatter:off
        BatchScannerSessionBuilder builder = BatchScannerSessionBuilder.create(client)
                .setTableName("NotFound")
                .setAuthorizations(authorizations)
                .setQuery(new QueryImpl());
        //  @formatter:on
        Exception e = assertThrows(RuntimeException.class, () -> buildAndScan(builder));
        String expected = "org.apache.accumulo.core.client.TableNotFoundException: Table NotFound (Id=NotFound) does not exist (no such table)";
        assertEquals(expected, e.getMessage());
    }

    @Test
    @Override
    public void testOptionalParameterDefaultValues() {
        //  @formatter:off
        BatchScannerSessionBuilder builder = BatchScannerSessionBuilder.create(client)
                .setTableName(tableName)
                .setAuthorizations(authorizations)
                .setQuery(query);
        //  @formatter:on
        buildAndScan(builder);

        assertEquals(tableName, builder.getTableName());
        assertEquals(authorizations, builder.getAuthorizations());
        assertEquals(query, builder.getQuery());

        assertEquals(100, builder.getResourceQueueSize());
        assertEquals(1000, builder.getResultQueueSize());

        assertNull(builder.getConsistencyLevel());
        assertTrue(builder.getExecutionHints().isEmpty());

        assertEquals(5, builder.getNumQueryThreads());
    }

    @Test
    public void testConsistencyLevelImmediate() {
        BatchScannerSessionBuilder builder = create();
        builder.setConsistencyLevel(ConsistencyLevel.IMMEDIATE);
        buildAndScan(builder);
        assertEquals(ConsistencyLevel.IMMEDIATE, builder.getConsistencyLevel());
    }

    @Test
    @Override
    public void testConsistencyLevelEventual() {
        BatchScannerSessionBuilder builder = create();
        builder.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
        buildAndScan(builder);
        assertEquals(ConsistencyLevel.EVENTUAL, builder.getConsistencyLevel());
    }

    @Test
    @Override
    public void testConsistencyLevelImmediateViaString() {
        BatchScannerSessionBuilder builder = create();
        builder.setConsistencyLevel("IMMEDIATE");
        buildAndScan(builder);
        assertEquals(ConsistencyLevel.IMMEDIATE, builder.getConsistencyLevel());
    }

    @Test
    @Override
    public void testConsistencyLevelEventualViaString() {
        BatchScannerSessionBuilder builder = create();
        builder.setConsistencyLevel("EVENTUAL");
        buildAndScan(builder);
        assertEquals(ConsistencyLevel.EVENTUAL, builder.getConsistencyLevel());
    }

    @Test
    @Override
    public void testIncorrectConsistencyLevel() {
        BatchScannerSessionBuilder builder = create();
        Exception e = assertThrows(IllegalArgumentException.class, () -> builder.setConsistencyLevel("LAZY"));
        String expected = "No enum constant org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel.LAZY";
        assertEquals(expected, e.getMessage());
    }

    @Test
    @Override
    public void testScanType() {
        BatchScannerSessionBuilder builder = create();
        builder.setScanType("executor-pool-a");
        buildAndScan(builder);
        assertEquals("executor-pool-a", builder.getExecutionHints().get("scan_type"));
    }

    @Test
    @Override
    public void testScanPriority() {
        BatchScannerSessionBuilder builder = create();
        builder.setScanPriority(10);
        buildAndScan(builder);
        assertEquals("10", builder.getExecutionHints().get("priority"));
    }

    @Test
    @Override
    public void testScanTypeAndScanPriority() {
        BatchScannerSessionBuilder builder = create();
        builder.setScanType("executor-pool-a");
        builder.setScanPriority(10);
        buildAndScan(builder);
        assertEquals("executor-pool-a", builder.getExecutionHints().get("scan_type"));
        assertEquals("10", builder.getExecutionHints().get("priority"));
    }

    @Test
    @Override
    public void testStatsEnabled() {
        BatchScannerSessionBuilder builder = create().setStatsEnabled(true);
        buildAndScan(builder);
        assertTrue(builder.isStatsEnabled());
    }

    /**
     * Create a BatchScannerSessionBuilder with the minimum required options
     *
     * @return the builder
     */
    @Override
    public BatchScannerSessionBuilder create() {
        //  @formatter:off
        return BatchScannerSessionBuilder.create(client)
                .setTableName(tableName)
                .setAuthorizations(authorizations)
                .setQuery(query);
        //  @formatter:on
    }

    /**
     * Run the scanner and assert several variables
     *
     * @param builder
     *            the builder
     */
    private void buildAndScan(BatchScannerSessionBuilder builder) {
        BatchScannerSession scanner = builder.build();
        scanner.setChunkIter(List.of(createScannerChunk()).iterator());

        assertTrue(scanner.hasNext());
        assertNotNull(scanner.next());

        assertEquals(tableName, builder.getTableName());
        assertEquals(authorizations, builder.getAuthorizations());
        assertEquals(query, builder.getQuery());
    }

    private List<ScannerChunk> createScannerChunk() {
        Key start = new Key(key.getRow());
        Key stop = start.followingKey(PartialKey.ROW);
        Range range = new Range(start, true, stop, false);

        IteratorSetting settings = new IteratorSetting(1, "VersioningIterator", VersioningIterator.class);
        QueryData queryData = new QueryData("tableName", "FOO == 'bar'", List.of(range), Collections.emptySet(), List.of(settings));

        ScannerChunk chunk = new ScannerChunk(new SessionOptions(), List.of(range), queryData);
        return List.of(chunk);
    }
}
