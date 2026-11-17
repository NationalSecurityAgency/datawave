package datawave.query.tables;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
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

/**
 * Pins thread-safety defects in {@link BatchScannerSession}'s bookkeeping and executor management.
 * <p>
 * {@link BatchScannerSession#canRun(ScannerChunk)} dereferences a {@code serverMap} entry without a null check, and
 * {@link BatchScannerSession#updateThreadService(ExecutorService)} null-checks its parameter while shutting down the field, so a null argument silently leaks
 * the running executor.
 * <p>
 * These tests assert the broken behavior on purpose so that a fix is forced to update them.
 */
public class BatchScannerSessionThreadingTest {

    private static final InMemoryInstance instance = new InMemoryInstance(BatchScannerSessionThreadingTest.class.getName());

    private static final String tableName = "shard";

    private static AccumuloClient client;

    private final Set<Authorizations> authorizations = Set.of(new Authorizations("VIZ-A", "VIZ-B", "VIZ-C"));
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
    }

    @BeforeEach
    public void beforeEach() {
        this.query.setUncaughtExceptionHandler(new QueryUncaughtExceptionHandler());
    }

    /**
     * With no work left to hand out, the backoff check reads a server that was never registered in {@code serverMap} and dereferences the resulting null.
     */
    @Test
    public void testCanRunThrowsForAnUnregisteredLocation() {
        BatchScannerSession session = create().build();

        assertThrows(NullPointerException.class, () -> session.canRun(chunk()));

        session.close();
    }

    /**
     * Passing a null executor null-checks the argument rather than the field, so the running executor is neither shut down nor replaced with anything usable,
     * and the session is left in a state where it cannot be closed.
     */
    @Test
    public void testUpdateThreadServiceWithNullLeaksTheRunningExecutor() {
        BatchScannerSession session = create().build();

        ExecutorService existing = session.service;
        assertFalse(existing.isShutdown());

        session.updateThreadService(null);

        // the executor is left running with no reference held by the session
        assertFalse(existing.isShutdown(), "the previous executor was shut down");
        assertNull(session.service, "the session still holds an executor");

        // and the session can no longer be closed, because its close path dereferences the executor it just dropped
        assertThrows(NullPointerException.class, session::close);

        existing.shutdownNow();
    }

    /**
     * Control for {@link #testUpdateThreadServiceWithNullLeaksTheRunningExecutor()}: a non-null executor does replace and shut down the previous one.
     */
    @Test
    public void testUpdateThreadServiceWithAnExecutorShutsDownThePreviousOne() {
        BatchScannerSession session = create().build();

        ExecutorService existing = session.service;
        ExecutorService replacement = Executors.newSingleThreadExecutor();

        session.updateThreadService(replacement);

        assertTrue(existing.isShutdown());
        assertSame(replacement, session.service);

        session.close();
    }

    /**
     * Create a builder with the minimum required options.
     *
     * @return the builder
     */
    private BatchScannerSessionBuilder create() {
        //  @formatter:off
        return BatchScannerSessionBuilder.create(client)
                .setTableName(tableName)
                .setAuthorizations(authorizations)
                .setQuery(query);
        //  @formatter:on
    }

    /**
     * Create a scanner chunk whose location was never registered with the session.
     *
     * @return the chunk
     */
    private ScannerChunk chunk() {
        Key start = new Key("row");
        Key stop = start.followingKey(PartialKey.ROW);
        Range range = new Range(start, true, stop, false);

        IteratorSetting settings = new IteratorSetting(1, "VersioningIterator", VersioningIterator.class);
        QueryData queryData = new QueryData(tableName, "FOO == 'bar'", List.of(range), Collections.emptySet(), List.of(settings));

        return new ScannerChunk(new SessionOptions(), List.of(range), queryData);
    }
}
