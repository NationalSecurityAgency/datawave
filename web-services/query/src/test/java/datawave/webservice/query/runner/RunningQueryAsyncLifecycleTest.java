package datawave.webservice.query.runner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.google.common.util.concurrent.Uninterruptibles;

import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.query.cache.ResultsPage;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.QueryLogic;
import datawave.microservice.query.QueryImpl;
import datawave.microservice.querymetric.QueryMetricFactoryImpl;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.util.DnProperties;

/**
 * Exercises the real producer and request wait loops used by the asynchronous RunningQuery default introduced in PR #3637.
 *
 * Latches establish the ordering between prefetch, reset, and forced page returns. The Future.get deadlines are shorter than the ten-second page timeout, so
 * normal page expiration cannot make a blocked request appear to have responded to a force notification or freed an executor worker.
 */
@Timeout(15)
public class RunningQueryAsyncLifecycleTest {
    private final List<RunningQuery> queries = new ArrayList<>();
    private final List<ExecutorService> executors = new ArrayList<>();

    @BeforeEach
    public void setUp() {
        System.setProperty(DnProperties.NPE_OU_PROPERTY, "iamnotaperson");
    }

    @AfterEach
    public void tearDown() throws Exception {
        for (RunningQuery query : queries) {
            query.cancel();
        }
        for (ExecutorService executor : executors) {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(2, TimeUnit.SECONDS), "Query worker did not terminate");
        }
    }

    /**
     * After returning a page, the original producer could occupy a worker while blocked on its full read-ahead queue. With one shared worker, that idle query
     * prevented a second query's producer from starting. The worker must become available without losing the first query's buffered rows.
     */
    @Test
    public void idlePagedQueryDoesNotStarveAnotherQuery() throws Exception {
        ExecutorService executor = executor();
        CountDownLatch prefetched = new CountDownLatch(1);
        Iterator<Object> rows = Arrays.<Object> asList("a", "b", "c", "d", "e").iterator();
        AtomicInteger nextCalls = new AtomicInteger();
        Iterator<Object> iterator = new Iterator<>() {
            @Override
            public boolean hasNext() {
                return rows.hasNext();
            }

            @Override
            public Object next() {
                Object row = rows.next();
                if (nextCalls.incrementAndGet() == 3) {
                    // Reading the third row establishes that a later row was enqueued before submitting the competing request.
                    prefetched.countDown();
                }
                return row;
            }
        };
        RunningQuery first = query(executor, iterator, 1);
        RunningQuery second = query(executor, Collections.<Object> singletonList("fast-result").iterator(), 1);

        assertEquals(Collections.singletonList("a"), first.next().getResults());
        assertTrue(prefetched.await(2, TimeUnit.SECONDS), "First query did not prefetch");
        // Run the request on a separate executor: only its producer should compete for the shared query worker.
        Future<ResultsPage> page = executor().submit(second::next);
        assertEquals(Collections.singletonList("fast-result"), page.get(2, TimeUnit.SECONDS).getResults());

        // Returning the worker must preserve prefetched rows for later requests.
        List<Object> remaining = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            remaining.addAll(first.next().getResults());
        }
        assertEquals(Arrays.asList("b", "c", "d", "e"), remaining);
        assertEquals(ResultsPage.Status.NONE, first.next().getStatus());
    }

    /**
     * Reset reused the RunningQuery and reset its page number, but retained prefetched rows, iterator counters, and the number of results already returned. A
     * fresh execution must start with new-a, page one, and enough result budget to return both new rows.
     */
    @Test
    public void resetDiscardsPrefetchedRowsAndRestartsResultBudget() throws Exception {
        ExecutorService executor = executor();
        CountDownLatch prefetched = new CountDownLatch(1);
        AtomicInteger nextCalls = new AtomicInteger();
        Iterator<Object> rows = new TransformIterator<>(Arrays.<Object> asList("old-a", "old-b", "old-c", "old-d").iterator(), value -> {
            if (nextCalls.incrementAndGet() == 3) {
                // Wait until old-b has been enqueued; otherwise canceling before prefetch could hide the retained-row bug.
                prefetched.countDown();
            }
            return value;
        });
        RunningQuery query = query(executor, rows, 1);
        query.getSettings().setMaxResultsOverridden(true);
        query.getSettings().setMaxResultsOverride(2L);

        assertEquals(Collections.singletonList("old-a"), query.next().getResults());
        assertTrue(prefetched.await(2, TimeUnit.SECONDS), "Old execution did not prefetch");
        query.closeConnection(mock(AccumuloConnectionFactory.class));
        // A marker on the same single-worker executor proves the old producer exited before reinitialization.
        // This case isolates retained state; late producer completion is covered separately below.
        executor.submit(() -> {}).get(2, TimeUnit.SECONDS);
        when(query.getLogic().getTransformIterator(query.getSettings()))
                        .thenReturn(new TransformIterator<>(Arrays.<Object> asList("new-a", "new-b").iterator(), value -> value));
        query.setClient(mock(AccumuloClient.class));

        assertEquals(Collections.singletonList("new-a"), query.next().getResults());
        assertEquals(1, query.getLastPageNumber());
        assertEquals(Collections.singletonList("new-b"), query.next().getResults());
        assertEquals(ResultsPage.Status.NONE, query.next().getStatus());
    }

    /**
     * Canceling a Future does not prove its iterator call has stopped. Keep the old next() call alive across reset, then let it return a row or throw. Neither
     * its output nor its completion may affect the newly installed iterator's results or producer state.
     *
     * @param lateFailure
     *            whether the old iterator throws instead of returning its pending row
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void resetIgnoresLateResultsAndFailuresFromPreviousProducer(boolean lateFailure) throws Exception {
        ExecutorService executor = executor();
        CountDownLatch producerBlocked = new CountDownLatch(1);
        CountDownLatch releaseProducer = new CountDownLatch(1);
        AtomicInteger count = new AtomicInteger();
        Iterator<Object> rows = new Iterator<>() {
            @Override
            public boolean hasNext() {
                return count.get() < 2;
            }

            @Override
            public Object next() {
                if (count.getAndIncrement() == 0) {
                    return "old-a";
                }
                producerBlocked.countDown();
                // Model iterator work that does not stop immediately when closeConnection interrupts its Future.
                assertTrue(Uninterruptibles.awaitUninterruptibly(releaseProducer, 5, TimeUnit.SECONDS), "Old producer was not released");
                if (lateFailure) {
                    throw new IllegalStateException("Failure from the previous execution");
                }
                return "old-b";
            }
        };
        RunningQuery query = query(executor, rows, 1);
        try {
            assertEquals(Collections.singletonList("old-a"), query.next().getResults());
            assertTrue(producerBlocked.await(2, TimeUnit.SECONDS), "Old producer did not block");
            query.closeConnection(mock(AccumuloConnectionFactory.class));
            when(query.getLogic().getTransformIterator(query.getSettings()))
                            .thenReturn(new TransformIterator<>(Collections.<Object> singletonList("new-a").iterator(), value -> value));
            query.setClient(mock(AccumuloClient.class));
            // Release the old invocation only after the replacement execution has been initialized.
            releaseProducer.countDown();
            assertEquals(Collections.singletonList("new-a"), query.next().getResults());
            assertEquals(ResultsPage.Status.NONE, query.next().getStatus());
        } finally {
            releaseProducer.countDown();
        }
    }

    /**
     * A force notification could be sent after collecting a row but before the request entered hasNext()'s monitor wait. The old wait predicate ignored
     * forcedReturn, so the request started waiting after the notification and could not reach the flag check inside the results loop.
     */
    @Test
    public void forcedReturnBeforeHasNextWaitReturnsCollectedRows() throws Exception {
        CountDownLatch producerBlocked = new CountDownLatch(1);
        CountDownLatch releaseProducer = new CountDownLatch(1);
        RunningQuery query = query(executor(), blockingIterator(false, producerBlocked, releaseProducer), 10);
        query.setActiveCall(true);
        CountDownLatch callerPaused = new CountDownLatch(1);
        CountDownLatch releaseCaller = new CountDownLatch(1);
        AtomicBoolean paused = new AtomicBoolean();
        QueryImpl settings = (QueryImpl) query.getSettings();
        // RunningQuery checks the handler after incrementing its page count and before the next loop iteration.
        // Pause at that existing call to place the force notification before the request starts waiting.
        doAnswer(invocation -> {
            if (query.getCurrentPageCount() == 1 && paused.compareAndSet(false, true)) {
                callerPaused.countDown();
                assertTrue(releaseCaller.await(2, TimeUnit.SECONDS), "Caller was not released");
            }
            return null;
        }).when(settings).getUncaughtExceptionHandler();
        Future<ResultsPage> page = executor().submit(query::next);
        try {
            assertTrue(producerBlocked.await(2, TimeUnit.SECONDS), "Producer did not block");
            assertTrue(callerPaused.await(2, TimeUnit.SECONDS), "Caller did not pause after its first row");
            // No request monitor is waiting yet: the flag, not a remembered notification, must prevent the subsequent wait.
            query.attemptForcedPageReturn();
            releaseCaller.countDown();
            ResultsPage results = page.get(1, TimeUnit.SECONDS);
            assertEquals(Collections.singletonList("first"), results.getResults());
            assertEquals(ResultsPage.Status.PARTIAL, results.getStatus());
            assertFalse(query.isFinished(), "A forced return must preserve the pending result");
        } finally {
            releaseCaller.countDown();
            releaseProducer.countDown();
        }
    }

    /**
     * Covers the already-waiting counterpart to the missed-notification case. Forcing either wait must return a partial page with the collected row, without
     * treating the pending second row as end-of-query or consuming its iterator counters.
     *
     * @param blockInNext
     *            false blocks the request in hasNext(); true advertises another row but blocks its production, leaving the request in getNext()
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void forcedReturnDuringWaitPreservesPendingRow(boolean blockInNext) throws Exception {
        CountDownLatch producerBlocked = new CountDownLatch(1);
        CountDownLatch releaseProducer = new CountDownLatch(1);
        RunningQuery query = query(executor(), blockingIterator(blockInNext, producerBlocked, releaseProducer), 10);
        query.setActiveCall(true);
        Future<ResultsPage> page = executor().submit(query::next);
        try {
            assertTrue(producerBlocked.await(2, TimeUnit.SECONDS), "Producer did not block");
            awaitResultWait(query, blockInNext ? "getNext" : "hasNext");
            query.attemptForcedPageReturn();
            ResultsPage results = page.get(1, TimeUnit.SECONDS);
            assertEquals(Collections.singletonList("first"), results.getResults());
            assertEquals(ResultsPage.Status.PARTIAL, results.getStatus());
            assertFalse(query.isFinished());
            releaseProducer.countDown();
            assertEquals(Collections.singletonList("second"), query.next().getResults());
        } finally {
            releaseProducer.countDown();
        }
    }

    private void awaitResultWait(RunningQuery query, String method) {
        // The producer latch only establishes that the iterator is blocked. Check the request's actual monitor wait
        // before forcing, so these cases cannot accidentally exercise the before-wait scenario instead.
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (System.nanoTime() < deadline) {
            Thread thread = query.getCurrentThread();
            if (thread != null && thread.getState() == Thread.State.TIMED_WAITING) {
                for (StackTraceElement frame : thread.getStackTrace()) {
                    if (frame.getClassName().equals(RunningQuery.class.getName()) && frame.getMethodName().equals(method)) {
                        return;
                    }
                }
            }
            Thread.yield();
        }
        throw new AssertionError("Request did not enter " + method + " wait");
    }

    private Iterator<Object> blockingIterator(boolean blockInNext, CountDownLatch blocked, CountDownLatch release) {
        return new Iterator<>() {
            private int count;

            @Override
            public boolean hasNext() {
                if (count == 1 && !blockInNext) {
                    awaitRelease();
                }
                return count < 2;
            }

            @Override
            public Object next() {
                if (count == 1 && blockInNext) {
                    awaitRelease();
                }
                return count++ == 0 ? "first" : "second";
            }

            private void awaitRelease() {
                blocked.countDown();
                try {
                    assertTrue(release.await(5, TimeUnit.SECONDS), "Producer was not released");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
            }
        };
    }

    private ExecutorService executor() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executors.add(executor);
        return executor;
    }

    @SuppressWarnings("unchecked")
    private RunningQuery query(ExecutorService executor, Iterator<Object> rows, int pageSize) throws Exception {
        QueryImpl settings = spy(new QueryImpl());
        settings.setId(UUID.randomUUID());
        settings.setQueryLogicName("test");
        settings.setQuery("FOO == 'bar'");
        settings.setQueryName("async lifecycle test");
        settings.setUserDN("CN=user, OU=People");
        settings.setQueryAuthorizations("");
        settings.setBeginDate(new Date());
        settings.setEndDate(new Date());
        settings.setExpirationDate(new Date(System.currentTimeMillis() + 60000));
        settings.setPagesize(pageSize);
        QueryLogic<Object> logic = mock(QueryLogic.class);
        when(logic.getMaxPageSize()).thenReturn(100);
        when(logic.getMaxWork()).thenReturn(-1L);
        when(logic.getResultLimit(settings)).thenReturn(100L);
        when(logic.getMaxResults()).thenReturn(100L);
        when(logic.initialize(any(), any(), any())).thenReturn(mock(GenericQueryConfiguration.class));
        when(logic.getTransformIterator(settings)).thenReturn(new TransformIterator<>(rows, value -> value));
        DatawaveUser user = new DatawaveUser(SubjectIssuerDNPair.of("CN=user, OU=People", "CN=issuer"), DatawaveUser.UserType.USER, null, null, null, 0L);
        RunningQuery query = new RunningQuery(mock(AccumuloClient.class), AccumuloConnectionFactory.Priority.NORMAL, logic, settings, "",
                        new DatawavePrincipal(Collections.singletonList(user)), new RunningQuery.RunningQueryTiming() {
                            @Override
                            public boolean shouldReturnPartialResults(int size, int maximum, long elapsed) {
                                // Disable the ordinary partial-page heuristic so only the explicit force can return these small pages promptly.
                                return false;
                            }

                            @Override
                            public int getMaxLongRunningTimeoutRetries() {
                                return 10;
                            }

                            @Override
                            public long getPageShortCircuitTimeoutMs() {
                                return 10000;
                            }
                        }, new QueryMetricFactoryImpl());
        query.setExecutor(executor);
        queries.add(query);
        return query;
    }
}
