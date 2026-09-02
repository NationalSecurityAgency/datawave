package datawave.query.tables;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

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

import com.google.common.util.concurrent.Service.State;

import datawave.accumulo.inmemory.InMemoryAccumulo;
import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.webservice.query.util.QueryUncaughtExceptionHandler;

/**
 * Pins how {@link ScannerSession} propagates failures through the {@link QueryUncaughtExceptionHandler} it takes from the {@link Query}.
 * <p>
 * Two defects are covered. First, the handler is owned by the {@link Query}, so every session built for one query shares a single write-once object that is
 * never cleared: one session's failure permanently poisons its siblings. Second, a failing session records its failure in two independent places, the handler
 * and the Guava {@link com.google.common.util.concurrent.Service} failure state, and {@link ScannerSession#hasNext()} can surface either one depending on which
 * the consumer thread observes first. That is why {@code ScannerSessionBuilderTest#testTableNotFound}, which asserts an exact exception message, is sensitive
 * to Guava's internal state-transition timing.
 * <p>
 * These tests assert the broken behavior on purpose so that a fix is forced to update them.
 */
public class ScannerSessionUncaughtExceptionHandlerTest {

    private static final InMemoryAccumulo instance = new InMemoryAccumulo(ScannerSessionUncaughtExceptionHandlerTest.class.getName());

    private static final String tableName = "shard";
    private static final Long ts = System.currentTimeMillis();
    private static final Key key = new Key("row", "cf", "cq", "VIZ-A", ts);
    private static final Value EMPTY_VALUE = new Value();
    private static final long TIMEOUT_MILLIS = 30_000L;

    private static AccumuloClient client;
    private static Range range;

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

        try (BatchWriter bw = client.createBatchWriter(tableName)) {
            Mutation m = new Mutation(key.getRow());
            m.put(key.getColumnFamily(), key.getColumnQualifier(), key.getColumnVisibilityParsed(), key.getTimestamp(), EMPTY_VALUE);
            bw.addMutation(m);
        }
    }

    @BeforeEach
    public void beforeEach() {
        this.query.setUncaughtExceptionHandler(new QueryUncaughtExceptionHandler());

        Key start = new Key(key.getRow());
        Key stop = start.followingKey(PartialKey.ROW);
        range = new Range(start, true, stop, false);
    }

    /**
     * Every session built from the same {@link Query} shares that query's handler instance rather than owning one.
     */
    @Test
    public void testSessionsBuiltFromTheSameQueryShareOneHandler() {
        ScannerSession first = create().build();
        ScannerSession second = create().build();

        assertSame(query.getUncaughtExceptionHandler(), first.uncaughtExceptionHandler);
        assertSame(first.uncaughtExceptionHandler, second.uncaughtExceptionHandler);

        first.close();
        second.close();
    }

    /**
     * Control for {@link #testUnrelatedFailurePoisonsAHealthySession()}: with a clean handler the session reads its data without incident.
     */
    @Test
    public void testHealthySessionReturnsResults() {
        ScannerSession session = create().build();
        session.setRanges(List.of(range));

        assertTrue(session.hasNext());
        assertNotNull(session.next());

        session.close();
    }

    /**
     * A failure recorded by any other holder of the query's handler is rethrown by a session that never failed, because {@link ScannerSession#hasNext()} checks
     * the shared handler unconditionally and the handler is never cleared.
     */
    @Test
    public void testUnrelatedFailurePoisonsAHealthySession() {
        ScannerSession session = create().build();
        session.setRanges(List.of(range));

        // stands in for a sibling session, or any other component, failing against the same Query
        RuntimeException unrelated = new RuntimeException("failure from an unrelated scanner session");
        query.getUncaughtExceptionHandler().uncaughtException(Thread.currentThread(), unrelated);

        RuntimeException e = assertThrows(RuntimeException.class, session::hasNext);
        assertSame(unrelated, e.getCause());

        session.close();
    }

    /**
     * {@link AnyFieldScanner} opts out of the shared handler, which is the counter-example the shared-handler design forces on any session whose failures
     * should not fail the whole query. This test documents the intended behavior and should keep passing after a fix.
     */
    @Test
    public void testAnyFieldScannerDoesNotShareTheQueryHandler() {
        ScannerSession session = create().build();
        AnyFieldScanner anyField = new AnyFieldScanner(session);

        assertNotSame(query.getUncaughtExceptionHandler(), anyField.uncaughtExceptionHandler);

        session.close();
        anyField.close();
    }

    /**
     * A single failure is recorded in two independent channels: the shared handler and the Guava service's own failure state. Whichever the consumer thread
     * observes first decides the type and message of the exception {@link ScannerSession#hasNext()} throws, which is what makes an exact-message assertion on
     * this path dependent on Guava's timing.
     */
    @Test
    public void testFailureIsRecordedInTwoCompetingChannels() {
        //  @formatter:off
        ScannerSession session = ScannerSessionBuilder.create(client)
                .setTableName("NotFound")
                .setAuthorizations(authorizations)
                .setQuery(query)
                .build();
        //  @formatter:on
        session.setRanges(List.of(range));

        // both surfacing paths throw a RuntimeException, so this assertion is stable regardless of which one wins
        assertThrows(RuntimeException.class, session::hasNext);

        awaitFailed(session);

        QueryUncaughtExceptionHandler handler = query.getUncaughtExceptionHandler();
        Throwable handlerFailure = handler.hasUncaughtException() ? handler.getUncaughtException().getLeft() : null;
        Throwable serviceFailure = session.failureCause();

        assertNotNull(handlerFailure, "the handler channel did not record the failure");
        assertNotNull(serviceFailure, "the service channel did not record the failure");

        // the message asserted by ScannerSessionBuilderTest#testTableNotFound is produced only by the handler channel
        String expected = "org.apache.accumulo.core.client.TableNotFoundException: Table NotFound (Id=NotFound) does not exist (no such table)";
        assertEquals(expected, new RuntimeException(handlerFailure).getMessage());

        // the service channel would surface the same failure as an IllegalStateException with an entirely different message
        assertNotSame(handlerFailure, serviceFailure);

        session.close();
    }

    /**
     * Create a builder with the minimum required options.
     *
     * @return the builder
     */
    private ScannerSessionBuilder create() {
        //  @formatter:off
        return ScannerSessionBuilder.create(client)
                .setTableName(tableName)
                .setAuthorizations(authorizations)
                .setQuery(query);
        //  @formatter:on
    }

    /**
     * Wait for a session's service to reach the failed state.
     *
     * @param session
     *            the session to wait on
     */
    private void awaitFailed(ScannerSession session) {
        long deadline = System.currentTimeMillis() + TIMEOUT_MILLIS;
        while (session.state() != State.FAILED) {
            if (System.currentTimeMillis() > deadline) {
                fail("timed out waiting for the session to fail, state was " + session.state());
            }
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("interrupted while waiting for the session to fail");
            }
        }
    }
}
