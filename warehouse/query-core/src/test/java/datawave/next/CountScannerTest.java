package datawave.next;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.accumulo.core.data.Value;
import org.junit.jupiter.api.Test;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;

import datawave.core.iterators.ResultCountingIterator;
import datawave.core.query.configuration.Result;
import datawave.next.scanner.DocumentScannerConfig;

/**
 * Verifies that a count query which matched nothing still reports a count.
 * <p>
 * The count is taken from the last result seen, so an empty result set leaves nothing to take a key or a column visibility from.
 */
public class CountScannerTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Test
    public void testEmptyResultSetReportsZero() {
        DocumentScannerConfig config = createConfig();
        CountScanner scanner = new CountScanner(config, Collections.emptyIterator());

        assertTimeoutPreemptively(TIMEOUT, () -> {
            scanner.start();
            assertTrue(scanner.hasNext(), "a count query always reports a count");

            Result result = assertDoesNotThrow(scanner::next, "an empty result set must not fail the count");
            assertNotNull(result);
            assertNotNull(result.getKey(), "a key is required even when nothing matched");
            assertEquals(0L, readCount((Value) result.getValue()), "an empty result set counts zero");
        });
    }

    /**
     * Once the count has been emitted the scanner is finished.
     */
    @Test
    public void testCountIsOnlyEmittedOnce() {
        DocumentScannerConfig config = createConfig();
        CountScanner scanner = new CountScanner(config, Collections.emptyIterator());

        assertTimeoutPreemptively(TIMEOUT, () -> {
            scanner.start();
            assertTrue(scanner.hasNext());
            assertNotNull(scanner.next());
            assertEquals(false, scanner.hasNext(), "the count is only reported once");
        });
    }

    private long readCount(Value value) {
        Kryo kryo = new Kryo();
        try (Input input = new Input(value.get())) {
            ResultCountingIterator.ResultCountTuple tuple = kryo.readObject(input, ResultCountingIterator.ResultCountTuple.class);
            return tuple.getCount();
        }
    }

    private DocumentScannerConfig createConfig() {
        DocumentScannerConfig config = new DocumentScannerConfig();
        config.setQueryId("count-test");
        config.setCandidateQueue(new LinkedBlockingQueue<>());
        config.setResults(new LinkedBlockingQueue<>());
        config.setSearchThreads(1);
        config.setRetrievalThreads(1);
        config.setMaxSearchTasks(4);
        config.setMaxRetrievalTasks(4);
        config.setCandidateQueuePollTimeMillis(5L);
        config.setResultQueuePollTimeMillis(5L);
        return config;
    }
}
