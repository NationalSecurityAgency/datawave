package datawave.concurrent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ThreadUtils}.
 */
class ThreadUtilsTest {

    private ThreadPoolExecutor executor;

    @AfterEach
    void tearDown() {
        if (executor != null && !executor.isShutdown()) {
            executor.shutdownNow();
        }
    }

    /**
     * Tests for {@link ThreadUtils#shutdownAndWait(ThreadPoolExecutor, long, TimeUnit)}.
     */
    @Nested
    class ShutdownAndWaitTests {

        /**
         * Verify that {@link ThreadUtils#shutdownAndWait(ThreadPoolExecutor, long, TimeUnit)} returns true when all tasks complete before the timeout.
         */
        @Test
        void testWhenAllTasksCompleteBeforeTimeout() {
            executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
            // Submit a task that will take 50 ms to complete.
            executor.submit(() -> {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            });

            boolean result = ThreadUtils.shutdownAndWait(executor, 2, TimeUnit.SECONDS);

            // Verify the executor shutdown, and all tasks completed.
            assertThat(result).isTrue();
            assertThat(executor.isShutdown()).isTrue();
            assertThat(executor.isTerminated()).isTrue();
        }

        /**
         * Verify that {@link ThreadUtils#shutdownAndWait(ThreadPoolExecutor, long, TimeUnit)} returns false when tasks do not complete before the timeout.
         */
        @Test
        void testWhenTasksDoNotCompleteBeforeTimeout() {
            executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
            // Submit a task that will take 2 second to complete.
            executor.submit(() -> {
                try {
                    Thread.sleep(TimeUnit.SECONDS.toMillis(2));
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            });

            // Attempt to shut down the executor.
            boolean result = ThreadUtils.shutdownAndWait(executor, 1, TimeUnit.SECONDS);

            // Verify the executor shutdown, but not all tasks completed, and the executor is not yet terminated.
            assertThat(executor.isShutdown()).isTrue();
            assertThat(result).isFalse();
            assertThat(executor.isTerminated()).isFalse();
        }

        /**
         * Verify that {@link ThreadUtils#shutdownAndWait(ThreadPoolExecutor, long, TimeUnit)} returns false when the thread is interrupted.
         */
        @Test
        void testInterrupted() throws InterruptedException {
            executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
            CountDownLatch taskStarted = new CountDownLatch(1);

            // Submit a task that will take 5 seconds.
            executor.submit(() -> {
                taskStarted.countDown();
                try {
                    Thread.sleep(TimeUnit.SECONDS.toMillis(5));
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            });
            taskStarted.await();

            // Create a separate worker thread that will attempt to shut down the executor.
            AtomicBoolean result = new AtomicBoolean(true);
            AtomicBoolean interruptedFlagAfterCall = new AtomicBoolean(false);
            Thread worker = new Thread(() -> {
                boolean r = ThreadUtils.shutdownAndWait(executor, 10, TimeUnit.SECONDS);
                result.set(r);
                interruptedFlagAfterCall.set(Thread.currentThread().isInterrupted());
            });

            // Start the worker thread and given it some time to enter awaitTermination.
            worker.start();
            Thread.sleep(200);

            // Interrupt the worker thread and wait for it to finish.
            worker.interrupt();
            worker.join(2000);

            // Verify that false was returned as a result of the thread interruption.
            assertThat(worker.isAlive()).isFalse();
            assertThat(result.get()).isFalse();
            assertThat(interruptedFlagAfterCall.get()).isTrue();
            assertThat(executor.isShutdown()).isTrue();
        }
    }

    @Nested
    class WaitForThreadsTests {

        /**
         * Verify that {@link ThreadUtils#waitForThreads(Consumer, ThreadPoolExecutor, String, int, long, long)} waits for all tasks to complete, and does not
         * have an issue with a null log delegate.
         */
        @Test
        void testNullLogDelegateDoesNotThrowException() {
            int poolSize = 2;
            int totalTasks = 5;
            executor = new ThreadPoolExecutor(poolSize, poolSize, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());

            for (int i = 0; i < totalTasks; i++) {
                executor.submit(() -> {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                    }
                });
            }

            long start = System.currentTimeMillis();
            long elapsed = ThreadUtils.waitForThreads(null, executor, "test", poolSize, totalTasks, start);

            assertThat(elapsed).isGreaterThanOrEqualTo(0);
            assertThat(executor.getCompletedTaskCount()).isEqualTo(totalTasks);
            assertThat(executor.getQueue()).isEmpty();
            assertThat(executor.getActiveCount()).isEqualTo(0);
        }

        /**
         * Verify that {@link ThreadUtils#waitForThreads(Consumer, ThreadPoolExecutor, String, int, long, long)} supplies a message to the log delegate.
         */
        @Test
        void testLogDelegateIsProvidedMessages() {
            @SuppressWarnings("unchecked")
            Consumer<String> logDelegate = mock(Consumer.class);
            int poolSize = 1;
            int totalTasks = 2;
            executor = new ThreadPoolExecutor(poolSize, poolSize, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());

            for (int i = 0; i < totalTasks; i++) {
                executor.submit(() -> {
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                    }
                });
            }

            long start = System.currentTimeMillis();
            long elapsed = ThreadUtils.waitForThreads(logDelegate, executor, "unitTest", poolSize, totalTasks, start);

            assertThat(elapsed).isGreaterThanOrEqualTo(0);
            assertThat(executor.getCompletedTaskCount()).isEqualTo(totalTasks);

            // We should have one initial progress message (first loop iteration) plus one "Finished Waiting" message.
            verify(logDelegate, atLeastOnce()).accept(anyString());
        }

        /**
         * Verify that {@link ThreadUtils#waitForThreads(Consumer, ThreadPoolExecutor, String, int, long, long)} still supplies a final message to the log
         * delegate even if no work is submitted to the executor.
         */
        @Test
        void testNoWorkSubmittedStillResultsInFinalMessageToLogDelegate() {
            @SuppressWarnings("unchecked")
            Consumer<String> logDelegate = mock(Consumer.class);
            int poolSize = 1;
            executor = new ThreadPoolExecutor(poolSize, poolSize, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());

            long start = System.currentTimeMillis();
            ThreadUtils.waitForThreads(logDelegate, executor, "empty", poolSize, 0, start);

            // The loop body never runs (nothing queued/active/incomplete), but the final message is always sent.
            verify(logDelegate, atLeastOnce()).accept(anyString());
        }

        /**
         * Verify that {@link ThreadUtils#waitForThreads(Consumer, ThreadPoolExecutor, String, int, long, long)} returns a
         */
        @Test
        void testElapsedTimeIsReturned() {
            int poolSize = 1;
            int totalTasks = 1;
            executor = new ThreadPoolExecutor(poolSize, poolSize, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());

            // Simulate work that 'started' 500 ms ago.
            long start = System.currentTimeMillis() - 500;
            // Submit a thread that will sleep for 50 ms.
            executor.submit(() -> {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            });

            long elapsed = ThreadUtils.waitForThreads(null, executor, "elapsedTest", poolSize, totalTasks, start);

            // The elapsed time should be between 500-600 ms.
            assertThat(elapsed).isGreaterThanOrEqualTo(500);
        }
    }

    /**
     * Tests for {@link ThreadUtils#blockUntil(long, TimeUnit, long, TimeUnit, BooleanSupplier)}
     */
    @DisplayName("Method blockUntil()")
    @Nested
    class BlockUntilTests {

        private final BooleanSupplier never = () -> false;
        private final BooleanSupplier always = () -> true;

        @DisplayName("Throws an exception for invalid argument of")
        @Nested
        class InvalidArguments {

            @DisplayName("A negative timeout")
            @Test
            void negativeTimeoutThrowsException() {
                assertThatThrownBy(() -> ThreadUtils.blockUntil(-1, TimeUnit.MILLISECONDS, 100, TimeUnit.MILLISECONDS, () -> true))
                                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("timeout must be 0 or greater");
            }

            @DisplayName("A null timeout unit")
            @Test
            void nullTimeoutUnitThrowsException() {
                assertThatThrownBy(() -> ThreadUtils.blockUntil(60_000, null, -1, TimeUnit.MILLISECONDS, () -> true)).isInstanceOf(NullPointerException.class)
                                .hasMessageContaining("timeout unit cannot be null");
            }

            @DisplayName("A negative poll interval")
            @Test
            void negativePollIntervalThrowsException() {
                assertThatThrownBy(() -> ThreadUtils.blockUntil(60_000, TimeUnit.MILLISECONDS, -1, TimeUnit.MILLISECONDS, () -> true))
                                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("pollInterval must be 0 or greater");
            }

            @DisplayName("A negative poll interval unit")
            @Test
            void nullPollIntervalUnitThrowsException() {
                assertThatThrownBy(() -> ThreadUtils.blockUntil(60_000, TimeUnit.MILLISECONDS, 100, null, () -> true)).isInstanceOf(NullPointerException.class)
                                .hasMessageContaining("pollIntervalUnit cannot be null");
            }

            @DisplayName("A null condition")
            @Test
            void nullConditionThrowsCondition() {
                assertThatThrownBy(() -> ThreadUtils.blockUntil(60_000, TimeUnit.MILLISECONDS, 100, TimeUnit.MILLISECONDS, null))
                                .isInstanceOf(NullPointerException.class).hasMessageContaining("condition cannot be null");
            }
        }

        @DisplayName("Returns true")
        @Nested
        class TimelyPaths {

            @DisplayName("Immediately given an already true condition")
            @Test
            void alreadyTrueCondition() throws InterruptedException {
                AtomicInteger calls = new AtomicInteger();
                long start = System.nanoTime();
                boolean result = ThreadUtils.blockUntil(10, TimeUnit.SECONDS, 1, TimeUnit.SECONDS, () -> {
                    calls.incrementAndGet();
                    return true;
                });
                long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                assertTrue(result);
                assertEquals(1, calls.get());
                assertTrue(elapsedMs < 500, "should not sleep, took " + elapsedMs + "ms");
            }

            @DisplayName("Immediately given an already true condition and a timeout of zero")
            @Test
            void alreadyTrueConditionWithZeroTimeout() throws InterruptedException {
                assertTrue(ThreadUtils.blockUntil(0, TimeUnit.MILLISECONDS, 0, TimeUnit.MILLISECONDS, always));
            }

            @DisplayName("Given non-true condition that becomes true within timeout")
            @Test
            void eventuallyTrueConditionReturnsTrue() throws InterruptedException {
                AtomicInteger calls = new AtomicInteger();
                boolean result = ThreadUtils.blockUntil(5, TimeUnit.SECONDS, 10, TimeUnit.MILLISECONDS, () -> calls.incrementAndGet() >= 4);
                assertTrue(result);
                assertEquals(4, calls.get());
            }

            @DisplayName("Given non-true condition that becomes true after a delay within the timeout")
            @Test
            void eventuallyTrueConditionWithDelay() throws InterruptedException {
                long becomesTrueAt = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(150);
                long start = System.nanoTime();
                boolean result = ThreadUtils.blockUntil(5, TimeUnit.SECONDS, 20, TimeUnit.MILLISECONDS, () -> System.nanoTime() - becomesTrueAt >= 0);
                long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                assertTrue(result);
                assertTrue(elapsedMs >= 140, "returned too early: " + elapsedMs + "ms");
                assertTrue(elapsedMs < 1000, "returned too late: " + elapsedMs + "ms");
            }

            @DisplayName("Given non-true condition that becomes true within huge timeout")
            @Test
            void eventuallyTrueConditionWithHugeTimeout() {
                AtomicInteger calls = new AtomicInteger();
                assertTimeoutPreemptively(Duration.ofSeconds(5), () -> assertTrue(
                                ThreadUtils.blockUntil(Long.MAX_VALUE, TimeUnit.DAYS, 5, TimeUnit.MILLISECONDS, () -> calls.incrementAndGet() >= 3)));
            }
        }

        @DisplayName("Returns false")
        @Nested
        class TimeoutPaths {

            @DisplayName("Given a false condition that remains false with a timeout of 0")
            @Test
            void alwaysFalseConditionWithTimeoutOfZero() throws InterruptedException {
                AtomicInteger calls = new AtomicInteger();
                boolean result = ThreadUtils.blockUntil(0, TimeUnit.MILLISECONDS, 10, TimeUnit.MILLISECONDS, () -> {
                    calls.incrementAndGet();
                    return false;
                });
                assertFalse(result);
                assertTrue(calls.get() >= 1);
            }

            @DisplayName("Given a condition that does not become true within a non-zero timeout")
            @Test
            void alwaysFalseConditionWithNonZeroTimeout() throws InterruptedException {
                long start = System.nanoTime();
                boolean result = ThreadUtils.blockUntil(200, TimeUnit.MILLISECONDS, 20, TimeUnit.MILLISECONDS, never);
                long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                assertFalse(result);
                assertTrue(elapsedMs >= 199, "returned before timeout: " + elapsedMs + "ms");
                assertTrue(elapsedMs < 1500, "overshot the timeout: " + elapsedMs + "ms");
            }
        }

        @DisplayName("Polling during wait")
        @Nested
        class PollingTests {

            @DisplayName("Does not exceed the timeout deadline")
            @Test
            void pollIntervalLongerThanTimeout() throws InterruptedException {
                long start = System.nanoTime();
                boolean result = ThreadUtils.blockUntil(100, TimeUnit.MILLISECONDS, 10, TimeUnit.SECONDS, never);
                long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                assertFalse(result);
                assertTrue(elapsedMs >= 99, "returned before timeout: " + elapsedMs + "ms");
                assertTrue(elapsedMs < 2000, "slept a full poll interval: " + elapsedMs + "ms");
            }

            @DisplayName("Is polled roughly at the requested interval")
            @Test
            void conditionIsPolledRoughlyAtTheRequestedInterval() throws InterruptedException {
                AtomicInteger calls = new AtomicInteger();
                ThreadUtils.blockUntil(300, TimeUnit.MILLISECONDS, 50, TimeUnit.MILLISECONDS, () -> {
                    calls.incrementAndGet();
                    return false;
                });
                // Allow for some variation during scheduling.
                assertTrue(calls.get() >= 3 && calls.get() <= 10, "unexpected poll count: " + calls.get());
            }

            @DisplayName("")
            @Test
            void conditionIsCheckedOnceMoreAtTheDeadlineBeforeGivingUp() throws InterruptedException {
                // Becomes true right at the timeout boundary; with a poll interval larger than the timeout the final check after the clamped sleep must still
                // observe it.
                long becomesTrueAt = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(100);
                boolean result = ThreadUtils.blockUntil(150, TimeUnit.MILLISECONDS, 10, TimeUnit.SECONDS, () -> System.nanoTime() - becomesTrueAt >= 0);
                assertTrue(result);
            }
        }

        @DisplayName("Does not busy-spin")
        @Nested
        class BusySpinTests {

            @DisplayName("Given a poll interval of 0")
            @Test
            void zeroPollInterval() throws InterruptedException {
                AtomicInteger calls = new AtomicInteger();
                ThreadUtils.blockUntil(50, TimeUnit.MILLISECONDS, 0, TimeUnit.MILLISECONDS, () -> {
                    calls.incrementAndGet();
                    return false;
                });
                // Clamped to a 1ms minimum, so roughly 50 evaluations at most; a hot spin would be thousands.
                assertTrue(calls.get() <= 100, "looks like a hot spin: " + calls.get() + " evaluations");
            }

            @DisplayName("Given a sub-ms poll interval")
            @Test
            void subMillisecondPollInterval() throws InterruptedException {
                AtomicInteger calls = new AtomicInteger();
                ThreadUtils.blockUntil(50, TimeUnit.MILLISECONDS, 500, TimeUnit.MICROSECONDS, () -> {
                    calls.incrementAndGet();
                    return false;
                });
                assertTrue(calls.get() <= 100, "looks like a hot spin: " + calls.get() + " evaluations");
            }
        }

        @DisplayName("Throws an exception when")
        @Nested
        class ExceptionalPaths {

            @DisplayName("The condition throws an exception")
            @Test
            void conditionThrowsException() {
                IllegalStateException exception = assertThrows(IllegalStateException.class,
                                () -> ThreadUtils.blockUntil(1, TimeUnit.SECONDS, 10, TimeUnit.MILLISECONDS, () -> {
                                    throw new IllegalStateException("boom");
                                }));
                assertEquals("boom", exception.getMessage());
            }

            @DisplayName("The thread is interrupted")
            @Test
            void interruptedThread() {
                Thread.currentThread().interrupt();
                try {
                    assertThrows(InterruptedException.class, () -> ThreadUtils.blockUntil(10, TimeUnit.SECONDS, 10, TimeUnit.MILLISECONDS, never));
                } finally {
                    Thread.interrupted();
                }
            }

            @DisplayName("The thread is interrupted while the thread is sleeping")
            @Test
            void interruptWhileSleepingIsPropagated() throws Exception {
                // 0 = running, 1 = interrupted, 2 = returned
                AtomicInteger outcome = new AtomicInteger();
                Thread thread = new Thread(() -> {
                    try {
                        ThreadUtils.blockUntil(30, TimeUnit.SECONDS, 1, TimeUnit.SECONDS, never);
                        outcome.set(2);
                    } catch (InterruptedException e) {
                        outcome.set(1);
                    }
                });
                thread.start();
                Thread.sleep(100);
                thread.interrupt();
                thread.join(5000);
                assertFalse(thread.isAlive());
                assertEquals(1, outcome.get());
            }
        }
    }
}
