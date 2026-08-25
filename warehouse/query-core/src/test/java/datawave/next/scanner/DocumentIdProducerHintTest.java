package datawave.next.scanner;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.jupiter.api.Test;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.core.query.configuration.QueryData;
import datawave.query.iterator.QueryOptions;

/**
 * Verifies that a field index scan is governed by the search execution hint.
 * <p>
 * The producer validated the table it was about to scan against the retrieval hint while applying the search pool, so the search hint's table was never checked
 * against anything. The shipped configuration gives both hints the same value, which hid the mismatch.
 */
public class DocumentIdProducerHintTest {

    private static final String SHARD = "shard";
    private static final String SEARCH_POOL = "searchPool";

    @Test
    public void testFullyConfiguredProducerBuildsAScanner() throws Exception {
        DocumentIdProducer producer = producerFor(createConfig());

        assertNotNull(producer.createScanner(), "a fully configured search scan must build a scanner");
    }

    /**
     * The search scan must be validated against the search hint. When the two hints name different tables, validating against the retrieval hint rejects a
     * correctly configured scan.
     */
    @Test
    public void testSearchHintIsUsedRatherThanTheRetrievalHint() throws Exception {
        DocumentScannerConfig config = createConfig();
        config.setSearchScanHintTable(SHARD);
        config.setRetrievalScanHintTable("a-different-table");

        DocumentIdProducer producer = producerFor(config);

        assertDoesNotThrow(producer::createScanner, "the search scan must be governed by the search hint");
    }

    /**
     * A retrieval hint that happens to match must not stand in for a missing search hint.
     */
    @Test
    public void testMissingSearchHintTableIsRejected() throws Exception {
        DocumentScannerConfig config = createConfig();
        config.setSearchScanHintTable(null);
        config.setRetrievalScanHintTable(SHARD);

        DocumentIdProducer producer = producerFor(config);

        assertThrows(NullPointerException.class, producer::createScanner);
    }

    @Test
    public void testMissingSearchConsistencyLevelIsRejected() throws Exception {
        DocumentScannerConfig config = createConfig();
        config.setSearchConsistencyLevel(null);

        DocumentIdProducer producer = producerFor(config);

        assertThrows(NullPointerException.class, producer::createScanner);
    }

    @Test
    public void testMissingSearchExecutorPoolIsRejected() throws Exception {
        DocumentScannerConfig config = createConfig();
        config.setSearchExecutorPool(null);

        DocumentIdProducer producer = producerFor(config);

        assertThrows(NullPointerException.class, producer::createScanner);
    }

    /**
     * The search hint is configured for one table, so scanning another would route the scan to the wrong pool.
     */
    @Test
    public void testSearchHintForADifferentTableIsRejected() throws Exception {
        DocumentScannerConfig config = createConfig();
        config.setSearchScanHintTable("a-different-table");

        DocumentIdProducer producer = producerFor(config);

        assertThrows(IllegalArgumentException.class, producer::createScanner);
    }

    private DocumentIdProducer producerFor(DocumentScannerConfig config) {
        return new DocumentIdProducer(config, queryData(), Range.exact("20240101_0"));
    }

    private QueryData queryData() {
        IteratorSetting setting = new IteratorSetting(100, "query", "datawave.query.iterator.QueryIterator");
        setting.addOption(QueryOptions.START_TIME, "0");
        setting.addOption(QueryOptions.END_TIME, String.valueOf(Long.MAX_VALUE));
        setting.addOption(QueryOptions.INDEXED_FIELDS, "FIELD_A");

        QueryData queryData = new QueryData();
        queryData.setTableName(SHARD);
        queryData.setQuery("FIELD_A == 'value'");
        queryData.setSettings(List.of(setting));
        return queryData;
    }

    private DocumentScannerConfig createConfig() throws Exception {
        AccumuloClient client = new InMemoryAccumuloClient("root", new InMemoryInstance());
        client.tableOperations().create(SHARD);

        DocumentScannerConfig config = new DocumentScannerConfig();
        config.setClient(client);
        config.setAuthorizations(Set.of(new Authorizations("A", "B")));
        config.setCandidateQueue(new LinkedBlockingQueue<>());
        config.setResults(new LinkedBlockingQueue<>());
        config.setSearchExecutorPool(Executors.newFixedThreadPool(1));
        config.setSearchScanHintTable(SHARD);
        config.setSearchScanHintPool(SEARCH_POOL);
        config.setSearchConsistencyLevel("IMMEDIATE");
        return config;
    }
}
