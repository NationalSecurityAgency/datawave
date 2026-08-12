package datawave.webservice.query.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Tests for {@link QueryUncaughtExceptionHandler}.
 */
class QueryUncaughtExceptionHandlerTest {

    private QueryUncaughtExceptionHandler handler;

    @BeforeEach
    void setUp() {
        handler = new QueryUncaughtExceptionHandler();
    }

    /**
     * Contains tests for scenarios involving a single thread.
     */
    @Nested
    @DisplayName("Basic single-threaded behavior")
    class BasicBehavior {

        /**
         * Verify the initial state of a {@link QueryUncaughtExceptionHandler}.
         */
        @Test
        void initialStateIsEmpty() {
            assertNull(handler.getThrowable());
            assertNull(handler.getThread());
            assertTrue(handler.getMessages().isEmpty());
        }

        /**
         * Verify we capture the first exception.
         */
        @Test
        void firstExceptionIsCaptured() {
            Thread thread = new Thread("t1");
            RuntimeException exception = new RuntimeException("boom");

            handler.uncaughtException(thread, exception);

            assertSame(exception, handler.getThrowable());
            assertSame(thread, handler.getThread());
        }

        /**
         * Verify exceptions after the first are ignored.
         */
        @Test
        void secondExceptionIsIgnored() {
            Thread thread1 = new Thread("t1");
            Thread thread2 = new Thread("t2");
            RuntimeException exception1 = new RuntimeException("first");
            RuntimeException exception2 = new RuntimeException("second");

            handler.uncaughtException(thread1, exception1);
            handler.uncaughtException(thread2, exception2);

            assertSame(exception1, handler.getThrowable());
            assertSame(thread1, handler.getThread());
        }

        /**
         * Verify a null throwable is ignored.
         */
        @Test
        void nullThrowableIsIgnored() {
            handler.uncaughtException(new Thread("t1"), null);
            assertNull(handler.getThrowable());
            assertNull(handler.getThread());

            Thread thread = new Thread("t2");
            RuntimeException exception = new RuntimeException("first");
            handler.uncaughtException(thread, exception);

            assertSame(exception, handler.getThrowable());
            assertSame(thread, handler.getThread());
        }

        /**
         * Verify a null throwable does not overwrite a captured throwable.
         */
        @Test
        void nullThrowableAfterRealExceptionDoesNotOverwrite() {
            Thread thread = new Thread("t1");
            RuntimeException exception = new RuntimeException("first");

            handler.uncaughtException(thread, exception);
            handler.uncaughtException(new Thread("t2"), null);

            assertSame(exception, handler.getThrowable());
            assertSame(thread, handler.getThread());
        }

        /**
         * Verify that passing in a null thread with a non-null throwable is allowed.
         */
        @Test
        void nullThreadWithRealThrowableIsStillCaptured() {
            RuntimeException ex = new RuntimeException("boom");

            handler.uncaughtException(null, ex);

            assertSame(ex, handler.getThrowable());
            assertNull(handler.getThread());
        }

        /**
         * Verify messages are captured in order.
         */
        @Test
        void messagesAreAddedInOrder() {
            handler.addMessage("m1");
            handler.addMessage("m2");
            handler.addMessage("m3");

            assertEquals(List.of("m1", "m2", "m3"), handler.getMessages());
        }

        /**
         * Verify the returned message list cannot be modified.
         */
        @Test
        void getMessagesReturnsListCopy() {
            handler.addMessage("m1");
            List<String> messages = handler.getMessages();

            assertThrows(UnsupportedOperationException.class, () -> messages.add("m2"));
            assertThrows(UnsupportedOperationException.class, () -> messages.remove(0));
            assertThrows(UnsupportedOperationException.class, messages::clear);
        }

        /**
         * Verify the message list does not reflect a live view.
         */
        @Test
        void getMessagesReturnsSnapshotNotLiveView() {
            handler.addMessage("m1");
            List<String> snapshot = handler.getMessages();

            handler.addMessage("m2");

            assertEquals(1, snapshot.size(), "Snapshot should not reflect later additions");
            assertEquals(2, handler.getMessages().size());
        }
    }

    /**
     * Contains tests for concurrent scenarios.
     */
    @Nested
    @DisplayName("Thread-safety / concurrency behavior")
    class ConcurrencyBehavior {

        /**
         * Verify that even under high concurrency, the captured thread and exception are always originated from the same call to
         * {@link QueryUncaughtExceptionHandler#uncaughtException(Thread, Throwable)}, and we always retain just one exception.
         */
        @Test
        @Timeout(30)
        void onlyOneExceptionRetainedUnderHighConcurrency() throws InterruptedException {
            // Create the thread pool and latches.
            int threadCount = 200;
            ExecutorService pool = Executors.newFixedThreadPool(threadCount);
            CountDownLatch readyLatch = new CountDownLatch(threadCount);
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(threadCount);

            List<RuntimeException> thrownExceptions = Collections.synchronizedList(new ArrayList<>());
            List<Thread> fakeThreads = Collections.synchronizedList(new ArrayList<>());

            try {
                // Submit a bunch of tasks that will attempt to pass a thread and exception to the handler.
                for (int i = 0; i < threadCount; i++) {
                    int idx = i;
                    pool.submit(() -> {
                        Thread fakeThread = new Thread("fake-" + idx);
                        RuntimeException ex = new RuntimeException("exception-" + idx);
                        thrownExceptions.add(ex);
                        fakeThreads.add(fakeThread);
                        readyLatch.countDown();
                        try {
                            startLatch.await();
                            handler.uncaughtException(fakeThread, ex);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } finally {
                            doneLatch.countDown();
                        }
                    });
                }

                // Wait for all threads to finish.
                assertTrue(readyLatch.await(10, TimeUnit.SECONDS), "Threads failed to reach start line");
                startLatch.countDown();
                assertTrue(doneLatch.await(15, TimeUnit.SECONDS), "Threads failed to complete in time");

                Throwable captured = handler.getThrowable();
                Thread capturedThread = handler.getThread();

                // Verify we captured an exception.
                assertNotNull(captured, "An exception should have been captured");
                assertNotNull(capturedThread, "A thread should have been captured");
                assertTrue(thrownExceptions.contains(captured), "Captured exception must be one of the thrown exceptions");

                // Verify the exception and thread came from the same call.
                int capturedIdx = thrownExceptions.indexOf(captured);
                assertSame(fakeThreads.get(capturedIdx), capturedThread, "Captured throwable and thread must originate from the same uncaughtException call");
            } finally {
                pool.shutdownNow();
            }
        }

        /**
         * Verify that even under high concurrency, the captured thread and exception are always originated from the same call. This tests runs trials with
         * fresh handler instances to verify that the check-then-CAS pattern in {@link QueryUncaughtExceptionHandler#uncaughtException(Thread, Throwable)} never
         * races into an inconsistent or lost state.
         */
        @Test
        @Timeout(30)
        void repeatedTrialsNeverProduceAMismatchedOrMissingPair() throws InterruptedException {
            // Run 20 trials.
            for (int trial = 0; trial < 20; trial++) {
                // Create the thread pool and latches.
                handler = new QueryUncaughtExceptionHandler();
                int threadCount = 50;
                ExecutorService pool = Executors.newFixedThreadPool(threadCount);
                CountDownLatch startLatch = new CountDownLatch(1);
                CountDownLatch doneLatch = new CountDownLatch(threadCount);

                try {
                    // Submit a bunch of tasks that will attempt to pass a thread and exception to the handler.
                    for (int i = 0; i < threadCount; i++) {
                        int idx = i;
                        pool.submit(() -> {
                            try {
                                startLatch.await();
                                handler.uncaughtException(new Thread("trial-thread-" + idx), new RuntimeException("trial-ex-" + idx));
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            } finally {
                                doneLatch.countDown();
                            }
                        });
                    }

                    // Wait for the tasks to finish.
                    startLatch.countDown();
                    assertTrue(doneLatch.await(10, TimeUnit.SECONDS));

                    // Verify we captured an exception.
                    assertNotNull(handler.getThrowable(), "Trial " + trial + ": throwable missing");
                    assertNotNull(handler.getThread(), "Trial " + trial + ": thread missing");

                    String threadName = handler.getThread().getName();
                    String exMessage = handler.getThrowable().getMessage();
                    String threadSuffix = threadName.substring(threadName.lastIndexOf('-') + 1);
                    String exSuffix = exMessage.substring(exMessage.lastIndexOf('-') + 1);

                    // Verify the exception and thread came from the same call.
                    assertEquals(threadSuffix, exSuffix, "Trial " + trial + ": captured thread/throwable pair do not correspond to the same call");
                } finally {
                    pool.shutdownNow();
                }
            }
        }

        /**
         * Verify we capture all messages supplied to {@link QueryUncaughtExceptionHandler#addMessage(String)}.
         */
        @Test
        @Timeout(20)
        void allMessagesAreCapturedUnderConcurrency() throws InterruptedException {
            // Create the thread pool and latches.
            int threadCount = 100;
            int messagesPerThread = 50;
            ExecutorService pool = Executors.newFixedThreadPool(20);
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(threadCount);

            try {
                // Submit a bunch of tasks that will record messages.
                for (int i = 0; i < threadCount; i++) {
                    int idx = i;
                    pool.submit(() -> {
                        try {
                            startLatch.await();
                            for (int j = 0; j < messagesPerThread; j++) {
                                handler.addMessage("thread-" + idx + "-msg-" + j);
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } finally {
                            doneLatch.countDown();
                        }
                    });
                }

                // Wait for the tasks to finish.
                startLatch.countDown();
                assertTrue(doneLatch.await(15, TimeUnit.SECONDS));

                // Verify that we have the correct number of messages.
                List<String> messages = handler.getMessages();
                assertEquals(threadCount * messagesPerThread, messages.size(), "No messages should be lost under concurrent addition");

                // Verify we did not accidentally duplicate a message.
                long distinctCount = messages.stream().distinct().count();
                assertEquals(threadCount * messagesPerThread, distinctCount, "No message should be duplicated under concurrent addition");
            } finally {
                pool.shutdownNow();
            }
        }

        /**
         * Verify that read/writes of messages under high concurrency do not result in any exceptions.
         */
        @Test
        @Timeout(20)
        void concurrentReadsAndWritesDoNotThrowOrCorruptState() throws InterruptedException {
            // Create the thread pool and latches.
            int writerThreads = 20;
            int readerThreads = 20;
            ExecutorService pool = Executors.newFixedThreadPool(writerThreads + readerThreads);
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(writerThreads + readerThreads);
            AtomicInteger errors = new AtomicInteger();

            try {
                // Submit a bunch of tasks to write messages.
                for (int i = 0; i < writerThreads; i++) {
                    int idx = i;
                    pool.submit(() -> {
                        try {
                            startLatch.await();
                            handler.uncaughtException(new Thread("writer-" + idx), new RuntimeException("ex-" + idx));
                            for (int j = 0; j < 20; j++) {
                                handler.addMessage("writer-" + idx + "-" + j);
                            }
                        } catch (Throwable t) {
                            // Count any exceptions thrown.
                            errors.incrementAndGet();
                        } finally {
                            doneLatch.countDown();
                        }
                    });
                }
                // Submit a bunch of tasks to read messages.
                for (int i = 0; i < readerThreads; i++) {
                    pool.submit(() -> {
                        try {
                            startLatch.await();
                            for (int j = 0; j < 50; j++) {
                                handler.getThrowable();
                                handler.getThread();
                                List<String> m = handler.getMessages();
                                if (m == null) {
                                    errors.incrementAndGet();
                                }
                            }
                        } catch (Throwable t) {
                            // Count any exceptions thrown.
                            errors.incrementAndGet();
                        } finally {
                            doneLatch.countDown();
                        }
                    });
                }

                // Wait for the tasks to finish.
                startLatch.countDown();
                assertTrue(doneLatch.await(15, TimeUnit.SECONDS));

                // Verify no exceptions were seen.
                assertEquals(0, errors.get(), "No exceptions should occur during concurrent reads/writes");
            } finally {
                pool.shutdownNow();
            }
        }
    }

    /**
     * Contains test to verify that call order of {@link QueryUncaughtExceptionHandler#uncaughtException(Thread, Throwable)} is respected.
     */
    @Nested
    @DisplayName("Ordering guarantee: lowest sequence number always wins")
    class OrderingGuaranteeBehavior {

        @SuppressWarnings("unchecked")
        private AtomicReference<ImmutableTriple<Long,Throwable,Thread>> atomicRefFor(QueryUncaughtExceptionHandler h) throws Exception {
            Field field = QueryUncaughtExceptionHandler.class.getDeclaredField("atomicRef");
            field.setAccessible(true);
            return (AtomicReference<ImmutableTriple<Long,Throwable,Thread>>) field.get(h);
        }

        private AtomicLong sequenceGeneratorFor(QueryUncaughtExceptionHandler h) throws Exception {
            Field field = QueryUncaughtExceptionHandler.class.getDeclaredField("sequenceGenerator");
            field.setAccessible(true);
            return (AtomicLong) field.get(h);
        }

        /**
         * Verify that a call with a lower sequence number will overwrite a already-stored higher sequenced value.
         */
        @Test
        void earlierSequenceOverwritesAlreadyStoredLaterSequence() throws Exception {
            AtomicReference<ImmutableTriple<Long,Throwable,Thread>> atomicRef = atomicRefFor(handler);
            AtomicLong sequenceGenerator = sequenceGeneratorFor(handler);

            // Simulate a "late" caller (sequence 5) whose write already landed first in wall-clock time.
            Throwable lateThrowable = new RuntimeException("late-already-stored");
            Thread lateThread = new Thread("late-thread");
            atomicRef.set(ImmutableTriple.of(5L, lateThrowable, lateThread));

            // The generator hasn't dispensed any sequence numbers on this instance yet, so the next real call receives sequence 0, simulating a earlier call.
            assertEquals(0L, sequenceGenerator.get(), "test setup assumption: generator must be untouched");

            Throwable earlyThrowable = new RuntimeException("true-earliest-call");
            Thread earlyThread = new Thread("early-thread");
            handler.uncaughtException(earlyThread, earlyThrowable);

            // Verify that the handler contains the exception and thread from the 'earlier' call.
            assertSame(earlyThrowable, handler.getThrowable(),
                            "A call assigned a lower sequence number must overwrite an already-stored higher-sequence value");
            assertSame(earlyThread, handler.getThread());
        }

        /**
         * Verify that a call with a higher sequence number never overwrites an already-stored lower-sequence value.
         */
        @Test
        void laterSequenceNeverOverwritesAlreadyStoredEarlierSequence() throws Exception {
            AtomicReference<ImmutableTriple<Long,Throwable,Thread>> atomicRef = atomicRefFor(handler);
            AtomicLong sequenceGenerator = sequenceGeneratorFor(handler);

            // Simulate the true first call (sequence 0) already having been stored.
            Throwable earlyThrowable = new RuntimeException("true-earliest-call");
            Thread earlyThread = new Thread("early-thread");
            atomicRef.set(ImmutableTriple.of(0L, earlyThrowable, earlyThread));

            // Advance the generator so the next real call receives a higher sequence number, simulating a caller that genuinely happened later.
            sequenceGenerator.set(999L);

            handler.uncaughtException(new Thread("late-thread"), new RuntimeException("late-loser"));

            // Verify that the handler contains the exception and thread from the 'earlier' call.
            assertSame(earlyThrowable, handler.getThrowable(),
                            "A call assigned a higher sequence number must never overwrite an already-stored lower-sequence value");
            assertSame(earlyThread, handler.getThread());
        }

        /**
         * Verify that repeated out-of-order arrivals always converge on the lowest sequence number.
         */
        @Test
        void repeatedOutOfOrderArrivalsConvergeOnLowestSequence() throws Exception {
            AtomicReference<ImmutableTriple<Long,Throwable,Thread>> atomicRef = atomicRefFor(handler);
            AtomicLong sequenceGenerator = sequenceGeneratorFor(handler);

            // Drive the real merge logic (via the real sequence generator + real uncaughtException call) with a deliberately scrambled arrival order:
            // 3, 7, 1, 9, 0, 5.
            // Regardless of arrival order, only sequence 0's payload should survive.
            long[] scrambledSequences = {3, 7, 1, 9, 0, 5};
            Throwable winningThrowable = null;
            Thread winningThread = null;

            for (long seq : scrambledSequences) {
                sequenceGenerator.set(seq);
                Throwable ex = new RuntimeException("candidate-" + seq);
                Thread t = new Thread("thread-" + seq);
                if (seq == 0) {
                    winningThrowable = ex;
                    winningThread = t;
                }
                handler.uncaughtException(t, ex);
            }

            // Verify the handler has the throwable and thread from sequence 0.
            assertSame(winningThrowable, handler.getThrowable(), "Only the candidate with sequence 0 should survive regardless of arrival order");
            assertSame(winningThread, handler.getThread());
            assertEquals(0L, atomicRef.get().getLeft().longValue());
        }
    }
}
