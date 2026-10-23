package datawave.webservice.query.runner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.common.connection.AccumuloConnectionFactory.Priority;
import datawave.core.query.cache.ResultsPage;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.QueryLogic;
import datawave.microservice.query.QueryImpl;
import datawave.microservice.querymetric.QueryMetricFactoryImpl;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.runner.RunningQuery.RunningQueryTiming;

@Timeout(15)
class RunningQueryAsyncTest {
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final List<CountDownLatch> gates = new ArrayList<>();
    private final List<RunningQuery> queries = new ArrayList<>();
    private QueryImpl settings;
    private QueryLogic<?> logic;
    private AccumuloClient client;
    private AccumuloConnectionFactory connections;

    @BeforeEach
    void setUp() throws Exception {
        settings = new QueryImpl();
        settings.setId(UUID.randomUUID());
        settings.setPagesize(10);
        logic = mock(QueryLogic.class);
        client = mock(AccumuloClient.class);
        connections = mock(AccumuloConnectionFactory.class);
        when(logic.initialize(any(), any(), any())).thenReturn(mock(GenericQueryConfiguration.class));
        when(logic.getMaxResults()).thenReturn(-1L);
        when(logic.getResultLimit(any())).thenReturn(-1L);
        when(logic.getMaxWork()).thenReturn(-1L);
    }

    @AfterEach
    void cleanUp() throws Exception {
        gates.forEach(CountDownLatch::countDown);
        for (RunningQuery query : queries) {
            query.cancel();
            query.closeConnection(connections);
        }
        executor.shutdownNow();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }

    private RunningQuery query(TransformIterator<?,?> iterator, RunningQueryTiming timing) throws Exception {
        when(logic.getTransformIterator(settings)).thenReturn(iterator);
        RunningQuery query = new RunningQuery(client, Priority.NORMAL, logic, settings, null, null, timing, new QueryMetricFactoryImpl());
        query.setExecutor(executor);
        query.setUseResultsThread(true);
        queries.add(query);
        return query;
    }

    private CountDownLatch gate() {
        CountDownLatch gate = new CountDownLatch(1);
        gates.add(gate);
        return gate;
    }

    private static void await(CountDownLatch latch) {
        try {
            assertTrue(latch.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private RunningQueryTiming timing(long timeoutMillis) {
        return new RunningQueryTiming() {
            @Override
            public boolean shouldReturnPartialResults(int count, int maximum, long elapsed) {
                return false;
            }

            @Override
            public int getMaxLongRunningTimeoutRetries() {
                return 3;
            }

            @Override
            public long getPageShortCircuitTimeoutMs() {
                return timeoutMillis;
            }
        };
    }

    private TransformIterator<Object,Object> twoRows(CountDownLatch secondRead, CountDownLatch release) {
        return new TransformIterator<Object,Object>() {
            private int count;

            @Override
            public boolean hasNext() {
                if (count == 1) {
                    secondRead.countDown();
                    await(release);
                }
                return count < 2;
            }

            @Override
            public Object next() {
                return ++count;
            }
        };
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void interruptNeverBecomesExhaustion(boolean blockInNext) throws Exception {
        CountDownLatch entered = gate();
        CountDownLatch release = gate();
        RunningQuery query = query(new TransformIterator<Object,Object>() {
            @Override
            public boolean hasNext() {
                if (!blockInNext) {
                    entered.countDown();
                    await(release);
                }
                return true;
            }

            @Override
            public Object next() {
                if (blockInNext) {
                    entered.countDown();
                    await(release);
                }
                return "unconsumed";
            }
        }, null);
        AtomicReference<Thread> consumer = new AtomicReference<>();
        AtomicBoolean interrupted = new AtomicBoolean();
        Future<?> result = executor.submit(() -> {
            consumer.set(Thread.currentThread());
            try {
                return query.next();
            } finally {
                interrupted.set(Thread.currentThread().isInterrupted());
            }
        });
        await(entered);
        consumer.get().interrupt();
        ExecutionException failure = assertThrows(ExecutionException.class, () -> result.get(2, TimeUnit.SECONDS));
        assertTrue(failure.getCause() instanceof QueryException);
        assertTrue(interrupted.get());
        assertFalse(query.isFinished());
        assertThrows(QueryException.class, query::next);
    }

    @Test
    void forcedPageIsPartialAndRemainingRowsArePreserved() throws Exception {
        CountDownLatch entered = gate();
        CountDownLatch release = gate();
        RunningQuery query = query(twoRows(entered, release), timing(60000));
        query.setActiveCall(true);
        Future<ResultsPage> result = executor.submit(query::next);
        await(entered);
        // The producer can enter its second read just before the consumer records the first result.
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (query.getCurrentPageCount() == 0 && System.nanoTime() < end) {
            Thread.yield();
        }
        assertEquals(1, query.getCurrentPageCount());
        query.attemptForcedPageReturn();
        ResultsPage first = result.get(2, TimeUnit.SECONDS);
        assertEquals(Collections.singletonList(1), first.getResults());
        assertEquals(ResultsPage.Status.PARTIAL, first.getStatus());
        assertFalse(query.isFinished());
        query.setActiveCall(false);
        release.countDown();
        assertEquals(Collections.singletonList(2), query.next().getResults());
        assertTrue(query.isFinished());
    }

    @Test
    void forcedRequestBeforeNextWaitIsNotLost() throws Exception {
        CountDownLatch releaseProducer = gate();
        CountDownLatch beforeWait = gate();
        CountDownLatch releaseConsumer = gate();
        AtomicBoolean gated = new AtomicBoolean();
        settings = spy(settings);
        RunningQuery query = query(twoRows(gate(), releaseProducer), timing(60000));
        doAnswer(call -> {
            if (query.getCurrentPageCount() == 1 && gated.compareAndSet(false, true)) {
                beforeWait.countDown();
                await(releaseConsumer);
            }
            return null;
        }).when(settings).getUncaughtExceptionHandler();
        query.setActiveCall(true);
        Future<ResultsPage> result = executor.submit(query::next);
        await(beforeWait);
        query.attemptForcedPageReturn();
        releaseConsumer.countDown();
        ResultsPage page = result.get(2, TimeUnit.SECONDS);
        assertEquals(Collections.singletonList(1), page.getResults());
        assertEquals(ResultsPage.Status.PARTIAL, page.getStatus());
    }

    @Test
    void timeoutWithRowsReturnsPartialPage() throws Exception {
        CountDownLatch release = gate();
        RunningQuery query = query(twoRows(gate(), release), timing(2000));
        ResultsPage page = query.next();
        assertEquals(Collections.singletonList(1), page.getResults());
        assertEquals(ResultsPage.Status.PARTIAL, page.getStatus());
        release.countDown();
        assertEquals(Collections.singletonList(2), query.next().getResults());
    }

    @Test
    void resetDiscardsOldBufferedRowsAndFuture() throws Exception {
        settings.setPagesize(1);
        RunningQuery query = query(new TransformIterator<>(Arrays.asList("old", "unread").iterator(), value -> value), null);
        assertEquals(Collections.singletonList("old"), query.next().getResults());
        query.closeConnection(connections);
        when(logic.getTransformIterator(settings)).thenReturn(new TransformIterator<>(Collections.singletonList("new").iterator(), value -> value));
        query.setClient(client);
        assertEquals(Collections.singletonList("new"), query.next().getResults());
        assertEquals(ResultsPage.Status.NONE, query.next().getStatus());
    }

    @Test
    void closeUnblocksReaderBeforeReturningClientAndIsIdempotent() throws Exception {
        CountDownLatch entered = gate();
        CountDownLatch release = gate();
        AtomicBoolean exited = new AtomicBoolean();
        RunningQuery query = query(new TransformIterator<Object,Object>() {
            @Override
            public boolean hasNext() {
                entered.countDown();
                while (release.getCount() != 0) {
                    try {
                        release.await();
                    } catch (InterruptedException ignored) {
                        // Requires logic.close() to release its resource.
                    }
                }
                exited.set(true);
                return false;
            }
        }, null);
        doAnswer(call -> {
            release.countDown();
            return null;
        }).when(logic).close();
        doAnswer(call -> {
            assertTrue(exited.get(), "client cannot be reused while its reader is still active");
            return null;
        }).when(connections).returnClient(client);
        Future<?> result = executor.submit(query::next);
        await(entered);
        query.closeConnection(connections);
        assertThrows(ExecutionException.class, () -> result.get(2, TimeUnit.SECONDS));
        query.closeConnection(connections);
        verify(logic, times(1)).close();
        verify(connections, times(1)).returnClient(client);
    }

    @Test
    void closeRetainsClientIfReaderCannotStop() throws Exception {
        CountDownLatch entered = gate();
        CountDownLatch release = gate();
        RunningQuery query = query(new TransformIterator<Object,Object>() {
            @Override
            public boolean hasNext() {
                entered.countDown();
                while (release.getCount() != 0) {
                    try {
                        release.await();
                    } catch (InterruptedException ignored) {
                        // Simulate a scanner that cannot be released even by logic.close().
                    }
                }
                return false;
            }
        }, null);
        Future<?> result = executor.submit(query::next);
        await(entered);
        assertThrows(QueryException.class, () -> query.closeConnection(connections));
        assertThrows(ExecutionException.class, () -> result.get(2, TimeUnit.SECONDS));
        assertSame(client, query.getClient());
        verify(connections, never()).returnClient(client);
        assertThrows(IllegalStateException.class, () -> query.setClient(client));
        release.countDown();
        query.closeConnection(connections);
        verify(connections).returnClient(client);
    }

    @Test
    void concurrentNextIsRejectedWithoutFailingActiveCall() throws Exception {
        CountDownLatch entered = gate();
        CountDownLatch release = gate();
        RunningQuery query = query(twoRows(entered, release), null);
        Future<ResultsPage> first = executor.submit(query::next);
        await(entered);
        assertThrows(QueryException.class, query::next);
        release.countDown();
        assertEquals(Arrays.asList(1, 2), first.get(2, TimeUnit.SECONDS).getResults());
    }

    @Test
    void successfulPagesDoNotResetExecutionWideRetryBudget() throws Exception {
        settings.setPagesize(1);
        when(logic.isLongRunningQuery()).thenReturn(true);
        EventBase marker = mock(EventBase.class);
        when(marker.isIntermediateResult()).thenReturn(true);
        RunningQuery query = query(new TransformIterator<>(Arrays.asList(marker, "one", marker, "two", marker).iterator(), value -> value), timing(60000));
        assertEquals(ResultsPage.Status.PARTIAL, query.next().getStatus());
        assertEquals(Collections.singletonList("one"), query.next().getResults());
        assertEquals(ResultsPage.Status.PARTIAL, query.next().getStatus());
        assertEquals(Collections.singletonList("two"), query.next().getResults());
        assertThrows(QueryException.class, query::next);
        assertThrows(QueryException.class, query::next);
    }

    @Test
    void zeroResultLimitNeverAdvancesIterator() throws Exception {
        when(logic.getResultLimit(any())).thenReturn(0L);
        AtomicInteger reads = new AtomicInteger();
        RunningQuery query = query(new TransformIterator<Object,Object>() {
            @Override
            public boolean hasNext() {
                reads.incrementAndGet();
                return true;
            }
        }, null);
        assertEquals(ResultsPage.Status.NONE, query.next().getStatus());
        assertEquals(0, reads.get());
        assertTrue(query.isFinished());
    }
}
