package datawave.query.tables;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.VersioningIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.configuration.QueryData;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.query.tables.async.ScannerChunk;
import datawave.scan.BatchScannerBuilder;
import datawave.scan.ScannerBuilder;
import datawave.table.constants.TableName;

public class ScanSessionManagerTest {

    private static final InMemoryInstance instance = new InMemoryInstance(ScanSessionManagerTest.class.getSimpleName());
    private static AccumuloClient client;

    private final String tableName = TableName.SHARD;
    private final Set<Authorizations> auths = Collections.singleton(new Authorizations("a", "b", "c"));
    private final Query query = new QueryImpl();

    private ScanSessionManagerForTests manager;

    @BeforeAll
    public static void setup() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {
        client = new InMemoryAccumuloClient("user", instance);
        client.tableOperations().create(TableName.SHARD);

        try (BatchWriter writer = client.createBatchWriter(TableName.SHARD)) {
            Mutation m = new Mutation("row");
            m.put("cf", "cq", new Value());
            m.put("cf2", "cq2", new Value());
            m.put("cf3", "cq3", new Value());
            writer.addMutation(m);
        }
    }

    @BeforeEach
    public void beforeEach() {
        manager = new ScanSessionManagerForTests();
    }

    @Test
    public void testManageScanner() {
        Scanner scanner = createScanner();
        manager.addScanner(scanner);
        advanceScanner(scanner);
        manager.close(scanner);
        assertManagerState();
    }

    @Test
    public void testManageBatchScanner() {
        BatchScanner scanner = createBatchScanner();
        manager.addScanner(scanner);
        advanceScanner(scanner);
        manager.close(scanner);
        assertManagerState();
    }

    @Test
    public void testManageScannerSession() {
        ScannerSession scanner = createScannerSession();
        manager.addScanner(scanner);
        advanceScanner(scanner);
        manager.close(scanner);
        assertFalse(scanner.isRunning());
        assertManagerState();
    }

    @Test
    public void testManageAnyFieldScanner() {
        ScannerSession scanner = createAnyFieldScanner();
        manager.addScanner(scanner);
        advanceScanner(scanner);
        manager.close(scanner);
        assertFalse(scanner.isRunning());
        assertManagerState();
    }

    @Test
    public void testManageBatchScannerSession() {
        BatchScannerSession scanner = createBatchScannerSession();
        manager.addScanner(scanner);
        advanceScanner(scanner);
        manager.close(scanner);
        assertFalse(scanner.isRunning());
        assertManagerState();
    }

    @Test
    public void testManageRangeStreamScanner() {
        ScannerSession scanner = createRangeStreamScanner();
        manager.addScanner(scanner);
        advanceScanner(scanner);

        manager.close(scanner);
        assertFalse(scanner.isRunning());
        assertManagerState();
    }

    @Test
    public void testManageMultipleScanners() {
        Scanner scanner = createScanner();
        BatchScanner batchScanner = createBatchScanner();
        ScannerSession scannerSession = createScannerSession();

        manager.addScanner(scanner);
        manager.addScanner(batchScanner);
        manager.addScanner(scannerSession);

        advanceScanner(scanner);
        advanceScanner(batchScanner);
        advanceScanner(scannerSession);

        manager.close(scanner);
        manager.close(batchScanner);
        manager.close(scannerSession);

        assertManagerState();
    }

    @Test
    public void testCloseMultipleScannersOneAtATime() {
        Scanner scanner = createScanner();
        BatchScanner batchScanner = createBatchScanner();
        ScannerSession scannerSession = createScannerSession();
        ScannerSession anyFieldScanner = createAnyFieldScanner();
        ScannerSession batchScannerSession = createBatchScannerSession();
        ScannerSession rangeStreamScanner = createRangeStreamScanner();

        manager.addScanner(scanner);
        manager.addScanner(batchScanner);
        manager.addScanner(scannerSession);
        manager.addScanner(anyFieldScanner);
        manager.addScanner(batchScannerSession);
        manager.addScanner(rangeStreamScanner);

        advanceScanner(scanner);
        advanceScanner(batchScanner);
        advanceScanner(scannerSession);
        advanceScanner(anyFieldScanner);
        advanceScanner(batchScannerSession);
        advanceScanner(rangeStreamScanner);

        manager.close(scanner);
        manager.close(batchScanner);
        manager.close(scannerSession);
        manager.close(anyFieldScanner);
        manager.close(batchScannerSession);
        manager.close(rangeStreamScanner);

        assertManagerState();
    }

    @Test
    public void testCloseMultipleScannersAllAtOnce() {
        Scanner scanner = createScanner();
        BatchScanner batchScanner = createBatchScanner();
        ScannerSession scannerSession = createScannerSession();
        ScannerSession anyFieldScanner = createAnyFieldScanner();
        ScannerSession batchScannerSession = createBatchScannerSession();
        ScannerSession rangeStreamScanner = createRangeStreamScanner();

        manager.addScanner(scanner);
        manager.addScanner(batchScanner);
        manager.addScanner(scannerSession);
        manager.addScanner(anyFieldScanner);
        manager.addScanner(batchScannerSession);
        manager.addScanner(rangeStreamScanner);

        advanceScanner(scanner);
        advanceScanner(batchScanner);
        advanceScanner(scannerSession);
        advanceScanner(anyFieldScanner);
        advanceScanner(batchScannerSession);
        advanceScanner(rangeStreamScanner);

        // close all scanners at once
        manager.close();
        assertManagerState();
    }

    private void advanceScanner(Scanner scanner) {
        scanner.setRange(new Range());
        var iter = scanner.iterator();
        assertTrue(iter.hasNext());
        assertNotNull(iter.next());
    }

    private void advanceScanner(BatchScanner scanner) {
        scanner.setRanges(Collections.singleton(new Range()));
        var iter = scanner.iterator();
        assertTrue(iter.hasNext());
        assertNotNull(iter.next());
    }

    private void advanceScanner(ScannerSession scanner) {
        scanner.setRanges(Collections.singleton(new Range()));
        assertTrue(scanner.hasNext());
        scanner.next();
    }

    private void assertManagerState() {
        assertNotNull(manager);
        assertEquals(manager.getAdded(), manager.getClosed());
    }

    private Scanner createScanner() {
        //  @formatter:off
        return ScannerBuilder.create(client)
                .setTableName(tableName)
                .setAuthorizations(auths.iterator().next())
                .build();
        //  @formatter:on
    }

    private BatchScanner createBatchScanner() {
        //  @formatter:off
        return BatchScannerBuilder.create(client)
                .setTableName(tableName)
                .setAuthorizations(auths.iterator().next())
                .build();
        //  @formatter:on
    }

    private ScannerSession createScannerSession() {
        //  @formatter:off
        return ScannerSessionBuilder.create(client)
                .setTableName(tableName)
                .setAuthorizations(auths)
                .setQuery(query)
                .build();
        //  @formatter:on
    }

    private ScannerSession createAnyFieldScanner() {
        //  @formatter:off
        return AnyFieldScannerBuilder.create(client)
                .setTableName(tableName)
                .setAuthorizations(auths)
                .setQuery(query)
                .build();
        //  @formatter:on
    }

    private BatchScannerSession createBatchScannerSession() {
        //  @formatter:off
        BatchScannerSession session = BatchScannerSessionBuilder.create(client)
                .setTableName(tableName)
                .setAuthorizations(auths)
                .setQuery(query)
                .build();
        //  @formatter:on
        session.setChunkIter(List.of(createScannerChunk()).iterator());
        return session;
    }

    private List<ScannerChunk> createScannerChunk() {
        SessionOptions options = new SessionOptions();

        Range range = new Range();

        IteratorSetting settings = new IteratorSetting(1, "VersioningIterator", VersioningIterator.class);
        QueryData queryData = new QueryData("tableName", "FOO == 'bar'", List.of(range), Collections.emptySet(), List.of(settings));

        ScannerChunk chunk = new ScannerChunk(options, List.of(range), queryData);
        return List.of(chunk);
    }

    private RangeStreamScanner createRangeStreamScanner() {
        //  @formatter:off
        return RangeStreamScannerBuilder.create(client)
                .setTableName(tableName)
                .setAuthorizations(auths)
                .setQuery(query)
                .setConfig(getConfig())
                .build();
        //  @formatter:on
    }

    private GenericQueryConfiguration getConfig() {
        GenericQueryConfiguration config = new GenericQueryConfiguration();
        config.setClient(client);
        config.setTableName(tableName);
        config.setAuthorizations(auths);
        config.setQuery(new QueryImpl());
        // not adding table based consistency levels or execution hints
        return config;
    }

    private static class ScanSessionManagerForTests extends ScanSessionManager {
        private int added = 0;
        private int closed = 0;

        public void addScanner(ScannerBase scanner) {
            added++;
            super.addScanner(scanner);
        }

        public void addScanner(ScannerSession scanner) {
            added++;
            super.addScanner(scanner);
        }

        public void close(ScannerBase scanner) {
            if (baseInstances.contains(scanner)) {
                closed++;
                super.close(scanner);
            }
        }

        public void close(ScannerSession scanner) {
            if (sessionInstances.contains(scanner)) {
                closed++;
                super.close(scanner);
            }
        }

        public int getAdded() {
            return added;
        }

        public int getClosed() {
            return closed;
        }
    }
}
