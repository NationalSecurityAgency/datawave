package datawave.concurrent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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

        @DisplayName("Throws an exception given a negative timeout")
        @Test
        void testNegativeTimeout() {
            assertThatThrownBy(() -> ThreadUtils.blockUntil(-1, TimeUnit.MILLISECONDS, 100, TimeUnit.MILLISECONDS, () -> true))
                            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("timeout must be 0 or greater");
        }

        @DisplayName("Throws an exception given a null timeout unit")
        @Test
        void testNullTimeoutUnit() {
            assertThatThrownBy(() -> ThreadUtils.blockUntil(60_000, null, -1, TimeUnit.MILLISECONDS, () -> true)).isInstanceOf(NullPointerException.class)
                            .hasMessageContaining("timeout unit cannot be null");
        }

        @DisplayName("Throws an exception given a negative poll interval")
        @Test
        void testNegativePollInterval() {
            assertThatThrownBy(() -> ThreadUtils.blockUntil(60_000, TimeUnit.MILLISECONDS, -1, TimeUnit.MILLISECONDS, () -> true))
                            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("pollInterval must be 0 or greater");
        }

        @DisplayName("Throws an exception given a negative poll interval unit")
        @Test
        void testNullPollIntervalUnit() {
            assertThatThrownBy(() -> ThreadUtils.blockUntil(60_000, TimeUnit.MILLISECONDS, 100, null, () -> true)).isInstanceOf(NullPointerException.class)
                            .hasMessageContaining("pollIntervalUnit cannot be null");
        }

        @DisplayName("Throws an exception given a null condition")
        @Test
        void testNullCondition() {
            assertThatThrownBy(() -> ThreadUtils.blockUntil(60_000, TimeUnit.MILLISECONDS, 100, TimeUnit.MILLISECONDS, null))
                            .isInstanceOf(NullPointerException.class).hasMessageContaining("condition cannot be null");
        }

        @DisplayName("Returns false when the timeout is exceeded")
        @Test
        void testTimeoutExceeded() throws InterruptedException {
            long startTime = System.currentTimeMillis();

            // Verify that blockUntil returns false.
            assertThat(ThreadUtils.blockUntil(1000, TimeUnit.MILLISECONDS, 100, TimeUnit.MILLISECONDS, () -> false)).isFalse();

            // Assert that the thread was blocked for at least 1000 ms.
            assertThat(System.currentTimeMillis() - startTime).isGreaterThanOrEqualTo(1000L);
        }

        @DisplayName("Returns true when the condition evaluates to true within the timeout")
        @Test
        void testTimeoutNotExceeded() throws InterruptedException {
            AtomicBoolean condition = new AtomicBoolean(false);
            long startTime = System.currentTimeMillis();

            // Set this condition to true 1 second in the future.
            CompletableFuture.runAsync(() -> condition.set(true), CompletableFuture.delayedExecutor(1000, TimeUnit.MILLISECONDS));

            // Verify that blockUntil returns true after the condition is set to true within the timeout of 3 seconds.
            assertThat(ThreadUtils.blockUntil(3000, TimeUnit.MILLISECONDS, 100, TimeUnit.MILLISECONDS, condition::get)).isTrue();

            // Assert that the thread was blocked for at least 1000 ms, and no more than 3000 ms.
            assertThat(System.currentTimeMillis() - startTime).isBetween(1000L, 3000L);
        }

        @DisplayName("Will cap the timeout at Long.MAX_VALUE given a timeout that will overflow")
        @Test
        void testMaxTimeout() throws InterruptedException {
            AtomicBoolean condition = new AtomicBoolean(false);
            long startTime = System.currentTimeMillis();

            // Set this condition to true 1 second in the future.
            CompletableFuture.runAsync(() -> condition.set(true), CompletableFuture.delayedExecutor(1000, TimeUnit.MILLISECONDS));

            // Verify that blockUntil returns true after the condition is set to true within the timeout of 3 seconds.
            assertThat(ThreadUtils.blockUntil(Long.MAX_VALUE, TimeUnit.MILLISECONDS, 100, TimeUnit.MILLISECONDS, condition::get)).isTrue();

            // Assert that the thread was blocked for at least 1000 ms, and no more than 3000 ms.
            assertThat(System.currentTimeMillis() - startTime).isBetween(1000L, 3000L);
        }
    }

    /**
     * Tests for {@link ThreadUtils#getDeadline(long, TimeUnit)}.
     */
    @DisplayName("Method getDeadline()")
    @Nested
    class GetDeadlineTests {

        @DisplayName("throws an NPE when given a null timeout unit")
        @Test
        void nullTimeoutUnit() {
            assertThatThrownBy(() -> ThreadUtils.getDeadline(1, null)).isInstanceOf(NullPointerException.class);
        }

        @DisplayName("returns the current system nano time given a negative timeout")
        @Test
        void negativeTimeout() {
            long beforeCall = System.nanoTime();
            long deadline = ThreadUtils.getDeadline(-5, TimeUnit.MINUTES);
            long afterCall = System.nanoTime();
            assertThat(deadline).isGreaterThanOrEqualTo(beforeCall);
            assertThat(deadline).isLessThanOrEqualTo(afterCall);
        }

        @DisplayName("returns the current system nano time given a timeout of 0")
        @Test
        void zeroTimeout() {
            long beforeCall = System.nanoTime();
            long deadline = ThreadUtils.getDeadline(0, TimeUnit.MINUTES);
            long afterCall = System.nanoTime();
            assertThat(deadline).isGreaterThanOrEqualTo(beforeCall);
            assertThat(deadline).isLessThanOrEqualTo(afterCall);
        }

        @DisplayName("returns Long.MAX_VALUE when given timeout that would overflow")
        @Test
        void maxValueTimeout() {
            assertThat(ThreadUtils.getDeadline(Long.MAX_VALUE, TimeUnit.MINUTES)).isEqualTo(Long.MAX_VALUE);
        }

        @DisplayName("returns a deadline based on system nano time")
        @Test
        void nonOverflowTimeout() {
            long delta = TimeUnit.MINUTES.toNanos(5);
            long beforeCall = System.nanoTime();
            long deadline = ThreadUtils.getDeadline(5, TimeUnit.MINUTES);
            long afterCall = System.nanoTime();
            // Ensure the deadline was based on the current nano time.
            assertThat(deadline - delta).isGreaterThanOrEqualTo(beforeCall);
            assertThat(deadline - delta).isLessThanOrEqualTo(afterCall);
        }
    }

    @DisplayName("Method convertOrCap()")
    @Nested
    class ConvertOrCapTests {

        @DisplayName("Returns the converted value")
        @Test
        void nonOverflowConversion() {
            assertThat(ThreadUtils.convertOrCap(5, TimeUnit.MINUTES::toMillis)).isEqualTo(TimeUnit.MINUTES.toMillis(5));
        }

        @DisplayName("Returns Long.MAX_VALUE when the converted value is 0")
        @Test
        void convertedValueOfZero() {
            assertThat(ThreadUtils.convertOrCap(0, TimeUnit.MINUTES::toMillis)).isEqualTo(Long.MAX_VALUE);
        }

        @DisplayName("Returns Long.MAX_VALUE when the converted value is negative")
        @Test
        void convertedValueOfNegative() {
            assertThat(ThreadUtils.convertOrCap(-1, TimeUnit.MINUTES::toMillis)).isEqualTo(Long.MAX_VALUE);
        }

        @DisplayName("Returns Long.MAX_VALUE when the value overflowed")
        @Test
        void overflowValue() {
            assertThat(ThreadUtils.convertOrCap(Long.MAX_VALUE, TimeUnit.DAYS::toNanos)).isEqualTo(Long.MAX_VALUE);
        }
    }
}
