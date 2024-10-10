package datawave.query.tables;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.util.TableName;

public class ScannerManagerTest {

    private static final InMemoryInstance instance = new InMemoryInstance(ScannerManagerTest.class.getSimpleName());
    private static AccumuloClient client;

    private final String tableName = TableName.SHARD;
    private final Set<Authorizations> auths = Collections.singleton(new Authorizations("a", "b", "c"));
    private final ScannerBase.ConsistencyLevel level = ScannerBase.ConsistencyLevel.IMMEDIATE;

    private ScannerManagerForTests manager;

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
        manager = new ScannerManagerForTests();
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
        ScannerSession scanner = createBatchScannerSession();
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
        scanner.hasNext();
        scanner.next();
    }

    private void assertManagerState() {
        assertNotNull(manager);
        assertEquals(manager.getAdded(), manager.getClosed());
    }

    private Scanner createScanner() {
        return new ScannerBuilder(client).withTableName(tableName).withAuths(auths).build();
    }

    private BatchScanner createBatchScanner() {
        return new BatchScannerBuilder(client).withTableName(tableName).withAuths(auths).build();
    }

    private ScannerSession createScannerSession() {
        return new ScannerSessionBuilder(client).withWrapper(ScannerSession.class).withTableName(tableName).withAuths(auths).build();
    }

    private ScannerSession createAnyFieldScanner() {
        return new ScannerSessionBuilder(client).withWrapper(AnyFieldScanner.class).withTableName(tableName).withAuths(auths).build();
    }

    private BatchScannerSession createBatchScannerSession() {
        return new ScannerSessionBuilder(client).withWrapper(BatchScannerSession.class).withTableName(tableName).withAuths(auths).build();
    }

    private RangeStreamScanner createRangeStreamScanner() {
        return new ScannerSessionBuilder(client).withWrapper(RangeStreamScanner.class).withTableName(tableName).withAuths(auths).build();
    }

    private Query getQuery() {
        QueryImpl query = new QueryImpl();
        query.setQuery("FOO == 'bar'");
        return query;
    }

    private Map<String,String> getHints() {
        Map<String,String> hints = new HashMap<>();
        hints.put("tableName", "executor-pool");
        return hints;
    }

    private class ScannerManagerForTests extends ScannerManager {
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
