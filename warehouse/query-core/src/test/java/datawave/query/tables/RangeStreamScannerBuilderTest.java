package datawave.query.tables;

import static org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.webservice.query.util.QueryUncaughtExceptionHandler;

public class RangeStreamScannerBuilderTest implements BaseScannerSessionTest<RangeStreamScannerBuilder> {
    private static final InMemoryInstance instance = new InMemoryInstance(RangeStreamScannerBuilderTest.class.getName());

    private static AccumuloClient client;
    private static final String tableName = "shard";
    private final Set<Authorizations> authorizations = Set.of(new Authorizations("VIZ-A", "VIZ-B", "VIZ-C"));

    private static Range range;

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

        Key start = new Key(key.getRow());
        Key stop = start.followingKey(PartialKey.ROW);
        range = new Range(start, true, stop, false);
    }

    @Test
    @Override
    public void testClientNotSet() {
        Exception e = assertThrows(NullPointerException.class, () -> RangeStreamScannerBuilder.create(null));
        String expected = "AccumuloClient must be set";
        assertEquals(expected, e.getMessage());
    }

    @Test
    @Override
    public void testTableNameNotSet() {
        RangeStreamScannerBuilder builder = RangeStreamScannerBuilder.create(client);
        Exception e = assertThrows(NullPointerException.class, () -> buildAndScan(builder));
        String expected = "TableName must be set";
        assertEquals(expected, e.getMessage());
    }

    @Test
    @Override
    public void testAuthorizationsNotSet() {
        //  @formatter:off
        RangeStreamScannerBuilder builder = RangeStreamScannerBuilder.create(client)
                .setTableName(tableName);
        //  @formatter:on
        Exception e = assertThrows(NullPointerException.class, () -> buildAndScan(builder));
        String expected = "Authorizations must be set";
        assertEquals(expected, e.getMessage());
    }

    @Test
    @Override
    public void testInvalidResourceQueueSize() {
        RangeStreamScannerBuilder builder = create().setResourceQueueSize(0);
        Exception e = assertThrows(IllegalArgumentException.class, () -> buildAndScan(builder));
        String expected = "ResourceQueueSize must be greater than 0";
        assertEquals(expected, e.getMessage());
    }

    @Test
    @Override
    public void testInvalidResultQueueSize() {
        RangeStreamScannerBuilder builder = create().setResultQueueSize(0);
        Exception e = assertThrows(IllegalArgumentException.class, () -> buildAndScan(builder));
        String expected = "ResultQueueSize must be greater than 0";
        assertEquals(expected, e.getMessage());
    }

    @Test
    public void testResultQueueSizeSizesTheResultQueue() {
        //  @formatter:off
        RangeStreamScanner scanner = RangeStreamScannerBuilder.create(client)
                .setTableName(tableName)
                .setAuthorizations(authorizations)
                .setQuery(query)
                .setResultQueueSize(37)
                .setConfig(getConfig())
                .build();
        //  @formatter:on

        assertEquals(37, scanner.resultQueue.remainingCapacity());
    }

    @Test
    @Override
    public void testQueryNotSet() {
        //  @formatter:off
        RangeStreamScannerBuilder builder = RangeStreamScannerBuilder.create(client)
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
        RangeStreamScannerBuilder builder = RangeStreamScannerBuilder.create(client)
                .setTableName("NotFound")
                .setAuthorizations(authorizations)
                .setQuery(query)
                .setConfig(getConfig());
        //  @formatter:on
        Exception e = assertThrows(RuntimeException.class, () -> buildAndScan(builder));
        String expected = "java.util.concurrent.ExecutionException: org.apache.accumulo.core.client.TableNotFoundException: Table NotFound (Id=NotFound) does not exist (no such table)";
        assertEquals(expected, e.getMessage());
    }

    @Test
    @Override
    public void testOptionalParameterDefaultValues() {
        //  @formatter:off
        RangeStreamScannerBuilder builder = RangeStreamScannerBuilder.create(client)
                .setTableName(tableName)
                .setAuthorizations(authorizations)
                .setQuery(query)
                .setConfig(getConfig());
        //  @formatter:on
        buildAndScan(builder);

        assertEquals(tableName, builder.getTableName());
        assertEquals(authorizations, builder.getAuthorizations());
        assertEquals(query, builder.getQuery());

        assertEquals(100, builder.getResourceQueueSize());
        assertEquals(1000, builder.getResultQueueSize());

        assertNull(builder.getConsistencyLevel());
        assertTrue(builder.getExecutionHints().isEmpty());
    }

    @Test
    public void testConsistencyLevelImmediate() {
        RangeStreamScannerBuilder builder = create();
        builder.setConsistencyLevel(ConsistencyLevel.IMMEDIATE);
        buildAndScan(builder);
        assertEquals(ConsistencyLevel.IMMEDIATE, builder.getConsistencyLevel());
    }

    @Test
    @Override
    public void testConsistencyLevelEventual() {
        RangeStreamScannerBuilder builder = create();
        builder.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
        buildAndScan(builder);
        assertEquals(ConsistencyLevel.EVENTUAL, builder.getConsistencyLevel());
    }

    @Test
    @Override
    public void testConsistencyLevelImmediateViaString() {
        RangeStreamScannerBuilder builder = create();
        builder.setConsistencyLevel("IMMEDIATE");
        buildAndScan(builder);
        assertEquals(ConsistencyLevel.IMMEDIATE, builder.getConsistencyLevel());
    }

    @Test
    @Override
    public void testConsistencyLevelEventualViaString() {
        RangeStreamScannerBuilder builder = create();
        builder.setConsistencyLevel("EVENTUAL");
        buildAndScan(builder);
        assertEquals(ConsistencyLevel.EVENTUAL, builder.getConsistencyLevel());
    }

    @Test
    @Override
    public void testIncorrectConsistencyLevel() {
        RangeStreamScannerBuilder builder = create();
        Exception e = assertThrows(IllegalArgumentException.class, () -> builder.setConsistencyLevel("LAZY"));
        String expected = "No enum constant org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel.LAZY";
        assertEquals(expected, e.getMessage());
    }

    @Test
    @Override
    public void testScanType() {
        RangeStreamScannerBuilder builder = create();
        builder.setScanType("executor-pool-a");
        buildAndScan(builder);
        assertEquals("executor-pool-a", builder.getExecutionHints().get("scan_type"));
    }

    @Test
    @Override
    public void testScanPriority() {
        RangeStreamScannerBuilder builder = create();
        builder.setScanPriority(10);
        buildAndScan(builder);
        assertEquals("10", builder.getExecutionHints().get("priority"));
    }

    @Test
    @Override
    public void testScanTypeAndScanPriority() {
        RangeStreamScannerBuilder builder = create();
        builder.setScanType("executor-pool-a");
        builder.setScanPriority(10);
        buildAndScan(builder);
        assertEquals("executor-pool-a", builder.getExecutionHints().get("scan_type"));
        assertEquals("10", builder.getExecutionHints().get("priority"));
    }

    @Test
    @Override
    public void testStatsEnabled() {
        RangeStreamScannerBuilder builder = create().setStatsEnabled(true);
        buildAndScan(builder);
        assertTrue(builder.isStatsEnabled());
    }

    /**
     * Create a RangeStreamScannerBuilder with the minimum required options
     *
     * @return the builder
     */
    @Override
    public RangeStreamScannerBuilder create() {
        //  @formatter:off
        return RangeStreamScannerBuilder.create(client)
                .setTableName(tableName)
                .setAuthorizations(authorizations)
                .setQuery(query)
                .setConfig(getConfig());
        //  @formatter:on
    }

    private GenericQueryConfiguration getConfig() {
        GenericQueryConfiguration config = new GenericQueryConfiguration();
        config.setClient(client);
        config.setTableName(tableName);
        config.setAuthorizations(authorizations);
        config.setQuery(query);
        // not adding table based consistency levels or execution hints
        return config;
    }

    /**
     * Run the scanner and assert several variables
     *
     * @param builder
     *            the builder
     */
    private void buildAndScan(RangeStreamScannerBuilder builder) {
        ScannerSession scanner = builder.build();
        scanner.setRanges(List.of(range));

        assertTrue(scanner.hasNext());
        assertNotNull(scanner.next());

        assertEquals(tableName, builder.getTableName());
        assertEquals(authorizations, builder.getAuthorizations());
        assertEquals(query, builder.getQuery());
    }
}
