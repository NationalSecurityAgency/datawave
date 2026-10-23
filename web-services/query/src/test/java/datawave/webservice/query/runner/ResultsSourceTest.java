package datawave.webservice.query.runner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.collections4.iterators.TransformIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.runner.ResultsSource.Outcome;
import datawave.webservice.query.runner.ResultsSource.PageRequest;

@Timeout(10)
class ResultsSourceTest {
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final List<ResultsSource> sources = new ArrayList<>();
    private final List<CountDownLatch> gates = new ArrayList<>();

    @AfterEach
    void cleanUp() throws Exception {
        gates.forEach(CountDownLatch::countDown);
        sources.forEach(ResultsSource::cancel);
        executor.shutdownNow();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }

    private ResultsSource source(TransformIterator<?,?> iterator, boolean async) {
        ResultsSource source = async ? new AsyncResultsSource(iterator, executor) : new DirectResultsSource(iterator);
        sources.add(source);
        return source;
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

    private TransformIterator<String,String> rows(String... values) {
        return new TransformIterator<>(Arrays.asList(values).iterator(), value -> value);
    }

    private TransformIterator<Object,Object> blocked(CountDownLatch entered, CountDownLatch release) {
        return new TransformIterator<Object,Object>() {
            @Override
            public boolean hasNext() {
                entered.countDown();
                await(release);
                return true;
            }

            @Override
            public Object next() {
                return "row";
            }
        };
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void deliversAllRowsBeforeExhaustion(boolean async) throws Exception {
        ResultsSource source = source(rows("one", "two", "three"), async);
        PageRequest page = source.beginPage(5000);
        assertEquals("one", source.poll(page).result);
        assertEquals("two", source.poll(page).result);
        assertEquals("three", source.poll(page).result);
        assertEquals(Outcome.EXHAUSTED, source.poll(page).outcome);
        assertEquals(Outcome.EXHAUSTED, source.poll(page).outcome);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void forcedRequestBeforePollIsNotLost(boolean async) throws Exception {
        ResultsSource source = source(rows("row"), async);
        PageRequest page = source.beginPage(5000);
        source.requestPageReturn(page);
        assertEquals(Outcome.FORCED, source.poll(page).outcome);
        source.endPage(page);
        PageRequest next = source.beginPage(5000);
        assertEquals("row", source.poll(next).result);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void staleRequestCannotForceAnotherPage(boolean async) throws Exception {
        ResultsSource source = source(rows("row"), async);
        PageRequest old = source.beginPage(5000);
        source.endPage(old);
        PageRequest current = source.beginPage(5000);
        source.requestPageReturn(old);
        assertEquals("row", source.poll(current).result);
    }

    @Test
    void forceWakesWaitingConsumer() throws Exception {
        CountDownLatch entered = gate();
        CountDownLatch release = gate();
        ResultsSource source = source(blocked(entered, release), true);
        PageRequest page = source.beginPage(5000);
        Future<ResultsSource.Attempt> result = executor.submit(() -> source.poll(page));
        await(entered);
        source.requestPageReturn(page);
        assertEquals(Outcome.FORCED, result.get(2, TimeUnit.SECONDS).outcome);
    }

    @Test
    void notificationWithoutResultIsNotTimeout() throws Exception {
        CountDownLatch entered = gate();
        CountDownLatch release = gate();
        ResultsSource source = source(blocked(entered, release), true);
        PageRequest page = source.beginPage(5000);
        AtomicReference<Thread> consumer = new AtomicReference<>();
        Future<ResultsSource.Attempt> result = executor.submit(() -> {
            consumer.set(Thread.currentThread());
            return source.poll(page);
        });
        await(entered);
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (consumer.get().getState() != Thread.State.TIMED_WAITING && System.nanoTime() < end) {
            Thread.yield();
        }
        assertEquals(Thread.State.TIMED_WAITING, consumer.get().getState());
        synchronized (source.monitor) {
            source.monitor.notifyAll();
        }
        assertThrows(TimeoutException.class, () -> result.get(30, TimeUnit.MILLISECONDS));
        release.countDown();
        assertEquals("row", result.get(2, TimeUnit.SECONDS).result);
    }

    @Test
    void deadlineDoesNotDiscardPendingResult() throws Exception {
        CountDownLatch entered = gate();
        CountDownLatch release = gate();
        ResultsSource source = source(blocked(entered, release), true);
        PageRequest first = source.beginPage(20);
        assertEquals(Outcome.DEADLINE, source.poll(first).outcome);
        await(entered);
        source.endPage(first);
        release.countDown();
        PageRequest second = source.beginPage(5000);
        assertEquals("row", source.poll(second).result);
    }

    @Test
    void cancellationWakesConsumerAndInterruptsWorker() throws Exception {
        CountDownLatch entered = gate();
        CountDownLatch release = gate();
        ResultsSource source = source(blocked(entered, release), true);
        PageRequest page = source.beginPage(Long.MAX_VALUE);
        Future<ResultsSource.Attempt> result = executor.submit(() -> source.poll(page));
        await(entered);
        source.cancel();
        ExecutionException failure = assertThrows(ExecutionException.class, () -> result.get(2, TimeUnit.SECONDS));
        assertTrue(failure.getCause() instanceof QueryException);
        assertTrue(source.awaitStopped(2000));
    }

    @Test
    void cancelledFutureIsNotProofThatWorkerExited() throws Exception {
        CountDownLatch entered = gate();
        CountDownLatch release = gate();
        ResultsSource source = source(new TransformIterator<Object,Object>() {
            @Override
            public boolean hasNext() {
                entered.countDown();
                boolean done = false;
                while (!done) {
                    try {
                        done = release.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException ignored) {
                        // Model an iterator that only unblocks when logic.close() releases its resource.
                    }
                }
                return false;
            }
        }, true);
        PageRequest page = source.beginPage(5000);
        Future<?> result = executor.submit(() -> source.poll(page));
        await(entered);
        source.cancel();
        assertThrows(ExecutionException.class, () -> result.get(2, TimeUnit.SECONDS));
        assertFalse(source.awaitStopped(20));
        release.countDown();
        assertTrue(source.awaitStopped(2000));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void failureBeforeConsumerWaitIsAlwaysReported(boolean failInNext) throws Exception {
        IllegalStateException expected = new IllegalStateException("iterator failure");
        ResultsSource source = new AsyncResultsSource(new TransformIterator<Object,Object>() {
            @Override
            public boolean hasNext() {
                if (!failInNext) {
                    throw expected;
                }
                return true;
            }

            @Override
            public Object next() {
                throw expected;
            }
        }, new ImmediateExecutor());
        sources.add(source);
        PageRequest page = source.beginPage(5000);
        assertSame(expected, assertThrows(IllegalStateException.class, () -> source.poll(page)));
        assertSame(expected, assertThrows(IllegalStateException.class, () -> source.poll(page)));
    }

    @Test
    void producerErrorIsReported() {
        AssertionError expected = new AssertionError("producer error");
        ResultsSource source = source(new TransformIterator<Object,Object>() {
            @Override
            public boolean hasNext() {
                throw expected;
            }
        }, true);
        PageRequest page = source.beginPage(5000);
        assertSame(expected, assertThrows(AssertionError.class, () -> source.poll(page)));
        assertTrue(source.awaitStopped(2000));
    }

    @Test
    void cancellationBeforeTaskStartsPreventsIteratorAccess() throws Exception {
        CountDownLatch queued = gate();
        List<Runnable> tasks = new ArrayList<>();
        ExecutorService holdingExecutor = new ImmediateExecutor() {
            @Override
            public void execute(Runnable command) {
                tasks.add(command);
                queued.countDown();
            }
        };
        AtomicInteger reads = new AtomicInteger();
        ResultsSource source = new AsyncResultsSource(new TransformIterator<Object,Object>() {
            @Override
            public boolean hasNext() {
                reads.incrementAndGet();
                return false;
            }
        }, holdingExecutor);
        sources.add(source);
        PageRequest page = source.beginPage(5000);
        Future<?> result = executor.submit(() -> source.poll(page));
        await(queued);
        source.cancel();
        assertTrue(source.awaitStopped(20));
        tasks.get(0).run();
        assertThrows(ExecutionException.class, () -> result.get(2, TimeUnit.SECONDS));
        assertEquals(0, reads.get());
    }

    private static class ImmediateExecutor extends AbstractExecutorService {
        @Override
        public void execute(Runnable command) {
            command.run();
        }

        @Override
        public void shutdown() {}

        @Override
        public List<Runnable> shutdownNow() {
            return Collections.emptyList();
        }

        @Override
        public boolean isShutdown() {
            return false;
        }

        @Override
        public boolean isTerminated() {
            return false;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return false;
        }
    }
}
