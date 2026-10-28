package datawave.next.scanner;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.jupiter.api.Test;

import datawave.accumulo.inmemory.InMemoryAccumulo;
import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.core.query.configuration.QueryData;

/**
 * Verifies that a retrieval scan is built through {@link DocumentRangeScan#createScanner()}, which requires an execution hint and a consistency level.
 * <p>
 * The document scheduler depends on its scans being routed to a dedicated tablet server executor pool, so the hint is a hard requirement and a missing one is a
 * configuration error. The document scan path built its scanner inline and so skipped both the requirement and the routing.
 */
public class DocumentRangeScanHintTest {

    private static final String TABLE = "shard";
    private static final String POOL = "shardTablePool";

    /**
     * The default retrieval path must build its scanner through {@code createScanner}, which is where the hint and consistency level are applied. Building one
     * inline instead silently drops both.
     */
    @Test
    public void testDocumentScanPathBuildsItsScannerThroughCreateScanner() throws Exception {
        DocumentScannerConfig config = createConfig();
        config.setUseQueryIterator(false);

        AtomicBoolean createScannerCalled = new AtomicBoolean(false);

        DocumentRangeScan scan = new DocumentRangeScan(keyWithContext(), config) {
            @Override
            protected Scanner createScanner() {
                createScannerCalled.set(true);
                throw new ScannerBuiltSignal();
            }
        };
        scan.setContext("hint-test");

        RuntimeException thrown = assertThrows(RuntimeException.class, scan::run);
        assertTrue(createScannerCalled.get(), "the document scan path must build its scanner through createScanner");
        assertTrue(hasCause(thrown, ScannerBuiltSignal.class), "expected the scan to fail via the stubbed scanner");
    }

    @Test
    public void testFullyConfiguredScanBuildsAScanner() throws Exception {
        DocumentRangeScan scan = new DocumentRangeScan(keyWithContext(), createConfig());

        assertNotNull(scan.createScanner(), "a fully configured scan must build a scanner");
    }

    /**
     * The hint is a hard requirement, so a scan that cannot be routed must fail rather than run unrouted.
     */
    @Test
    public void testMissingScanHintTableIsRejected() throws Exception {
        DocumentScannerConfig config = createConfig();
        config.setRetrievalScanHintTable(null);

        DocumentRangeScan scan = new DocumentRangeScan(keyWithContext(), config);
        assertThrows(NullPointerException.class, scan::createScanner);
    }

    @Test
    public void testMissingConsistencyLevelIsRejected() throws Exception {
        DocumentScannerConfig config = createConfig();
        config.setRetrievalConsistencyLevel(null);

        DocumentRangeScan scan = new DocumentRangeScan(keyWithContext(), config);
        assertThrows(NullPointerException.class, scan::createScanner);
    }

    @Test
    public void testMissingRetrievalExecutorPoolIsRejected() throws Exception {
        DocumentScannerConfig config = createConfig();
        config.setRetrievalExecutorPool(null);

        DocumentRangeScan scan = new DocumentRangeScan(keyWithContext(), config);
        assertThrows(NullPointerException.class, scan::createScanner);
    }

    /**
     * The hint is configured for one table, so applying it to another would route the scan to the wrong pool.
     */
    @Test
    public void testHintForADifferentTableIsRejected() throws Exception {
        DocumentScannerConfig config = createConfig();
        config.setRetrievalScanHintTable("a-different-table");

        DocumentRangeScan scan = new DocumentRangeScan(keyWithContext(), config);
        assertThrows(IllegalArgumentException.class, scan::createScanner);
    }

    private KeyWithContext keyWithContext() {
        QueryData queryData = new QueryData();
        queryData.setTableName(TABLE);
        queryData.setSettings(List.of(new IteratorSetting(100, "query", "datawave.query.iterator.QueryIterator")));

        return new KeyWithContext(new Key("row", "datatype\0uid"), queryData, false);
    }

    private DocumentScannerConfig createConfig() throws Exception {
        AccumuloClient client = new InMemoryAccumuloClient("root", new InMemoryAccumulo());
        client.tableOperations().create(TABLE);

        DocumentScannerConfig config = new DocumentScannerConfig();
        config.setClient(client);
        config.setAuthorizations(Set.of(new Authorizations("A", "B")));
        config.setResults(new LinkedBlockingQueue<>());
        config.setRetrievalExecutorPool(Executors.newFixedThreadPool(1));
        config.setRetrievalScanHintTable(TABLE);
        config.setRetrievalScanHintPool(POOL);
        config.setRetrievalConsistencyLevel("IMMEDIATE");
        return config;
    }

    /**
     * Marker used to abort the scan as soon as the scanner would have been built.
     */
    private static class ScannerBuiltSignal extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }

    /**
     * The scan wraps failures more than once on the way out, so the chain has to be walked.
     *
     * @param throwable
     *            the thrown exception
     * @param type
     *            the cause being looked for
     * @return true if the cause appears anywhere in the chain
     */
    private boolean hasCause(Throwable throwable, Class<? extends Throwable> type) {
        for (Throwable current = throwable; current != null; current = current.getCause()) {
            if (type.isInstance(current)) {
                return true;
            }
        }
        return false;
    }
}
