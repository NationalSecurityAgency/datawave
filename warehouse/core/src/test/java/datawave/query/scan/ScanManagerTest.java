package datawave.query.scan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
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
import datawave.util.TableName;

public class ScanManagerTest {

    private static final InMemoryInstance instance = new InMemoryInstance(ScanManagerTest.class.getName());

    private static AccumuloClient client;

    private static final String tableName = TableName.SHARD;
    private final Authorizations authorizations = new Authorizations("VIZ-A", "VIZ-B");

    private ScanManagerForTest manager;

    @BeforeAll
    public static void beforeAll() throws Exception {
        client = new InMemoryAccumuloClient("user", instance);

        client.tableOperations().create("shard");

        try (BatchWriter writer = client.createBatchWriter(tableName)) {
            Mutation m = new Mutation("row");
            m.put("cf", "cq", new Value());
            m.put("cf2", "cq2", new Value());
            m.put("cf3", "cq3", new Value());
            writer.addMutation(m);
        }
    }

    @BeforeEach
    public void beforeEach() throws Exception {
        manager = new ScanManagerForTest();
    }

    @Test
    public void testScannerLifecycle() {
        Scanner scanner = createScanner();
        manager.addScanner(scanner);
        advanceScanner(scanner);
        manager.close();
        assertManagerState(1);
    }

    @Test
    public void testManageMultipleScanners() {
        int count = 10;
        List<Scanner> scanners = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            Scanner scanner = createScanner();
            scanners.add(scanner);
            manager.addScanner(scanner);
        }

        for (Scanner scanner : scanners) {
            advanceScanner(scanner);
        }

        manager.close();
        assertManagerState(count);
    }

    private Scanner createScanner() {
        //  @formatter:off
        return ScannerBuilder.create(client)
                .setTableName(tableName)
                .setAuthorizations(authorizations)
                .build();
        //  @formatter:on
    }

    private void advanceScanner(Scanner scanner) {
        scanner.setRange(new Range());
        Iterator<Map.Entry<Key,Value>> iterator = scanner.iterator();
        assertTrue(iterator.hasNext());
        assertNotNull(iterator.next());
    }

    /**
     * Verify that close count matches create count, and that no active scanners exist
     */
    private void assertManagerState(int count) {
        assertEquals(count, manager.createCount);
        assertEquals(count, manager.closeCount);
        assertEquals(0, manager.current());
    }

    /**
     * A {@link ScanManager} with some stats
     */
    private static class ScanManagerForTest extends ScanManager {
        int createCount = 0;
        int closeCount = 0;

        public void addScanner(ScannerBase scanner) {
            createCount++;
            super.addScanner(scanner);
        }

        public void close(ScannerBase scanner) {
            closeCount++;
            super.close(scanner);
        }

        public int current() {
            return baseInstances.size();
        }
    }
}
