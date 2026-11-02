package datawave.concurrent;

import static java.util.Objects.requireNonNull;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities for working with threads and thread pools.
 */
public final class ThreadUtils {

    private static final Logger log = LoggerFactory.getLogger(ThreadUtils.class);

    /**
     * Shuts down the executor and waits for threads still in progress to finish within the specified time before continuing.
     *
     * @param executor
     *            the executor
     * @param timeout
     *            the time to wait
     * @param timeoutUnit
     *            the timeout unit
     * @return true if all tasks completed within the timeout period, or false if the tasks did not finish completing or if the thread was interrupted
     */
    public static boolean shutdownAndWait(ThreadPoolExecutor executor, long timeout, TimeUnit timeoutUnit) {
        requireNonNull(executor, "executor cannot be null");
        executor.shutdown();
        try {
            return executor.awaitTermination(timeout, timeoutUnit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Closed thread pool but not all threads completed successfully.");
            return false;
        }
    }

    /**
     * Waits for all active threads in the given thread pool to complete, with the option to provide a log delegate for accepting strings containing status
     * updates.
     *
     * @param logDelegate
     *            a wrapper delegate that will be supplied in-progress messages every 10 seconds while waiting for tasks to complete, and a final conclusion
     *            message after all tasks complete
     * @param executor
     *            the thread executor
     * @param type
     *            the type
     * @param poolSize
     *            the pool size
     * @param totalTasks
     *            the work time units
     * @param start
     *            the start time
     * @return time taken to complete all tasks
     */
    public static long waitForThreads(Consumer<String> logDelegate, ThreadPoolExecutor executor, String type, int poolSize, long totalTasks, long start) {
        long currentTime = System.currentTimeMillis();
        int activeTasks = executor.getActiveCount();
        int queuedTasks = executor.getQueue().size();
        long completedTasks = executor.getCompletedTaskCount();

        // Use an initial value of 0 to always trigger at least an initial status message if any tasks are still running.
        long lastMessaged = 0;
        while ((queuedTasks > 0 || activeTasks > 0 || completedTasks < totalTasks) && !executor.isTerminated()) {
            // Supply another status message to the log delegate if it has been at least 10 seconds since sending the last one.
            if (logDelegate != null && (lastMessaged < (System.currentTimeMillis() - 10_000L))) {
                logDelegate.accept(type + " running, T: " + activeTasks + "/" + poolSize + ", Completed: " + completedTasks + "/" + totalTasks + ", Remaining: "
                                + queuedTasks + ", " + (currentTime - start) + " ms elapsed");
                // Update the time we last supplied a message to the log delegate.
                lastMessaged = System.currentTimeMillis();
            }

            currentTime = System.currentTimeMillis();
            activeTasks = executor.getActiveCount();
            queuedTasks = executor.getQueue().size();
            completedTasks = executor.getCompletedTaskCount();
        }

        // Once all active threads have been completed, submit a completion message to the log delegate.
        if (logDelegate != null) {
            logDelegate.accept("Finished Waiting for " + type + " running, T: " + activeTasks + "/" + poolSize + ", Completed: " + completedTasks + "/"
                            + totalTasks + ", Remaining: " + queuedTasks + ", " + (currentTime - start) + " ms elapsed");
        }

        // Return the time it took for this method to complete.
        return (System.currentTimeMillis() - start);
    }

    /**
     * The shortest time {@link #blockUntil} will sleep between condition checks. Poll intervals below this (including 0) are raised to it, so the method never
     * busy-spins.
     */
    private static final long MIN_POLL_INTERVAL_NANOS = TimeUnit.MILLISECONDS.toNanos(1);

    /**
     * Blocks the execution of the current thread until the given condition evaluates to true, or until the timeout has been exceeded.
     * <p>
     * The condition is always evaluated at least once, even if the timeout is 0. Poll intervals shorter than 1 millisecond (including 0) are treated as 1
     * millisecond to avoid busy-waiting. A timeout too large to represent in nanoseconds is capped at {@link Long#MAX_VALUE} nanoseconds (roughly 292 years),
     * which in practice means "wait indefinitely".
     *
     * @param timeout
     *            the timeout to wait (0 or greater)
     * @param timeoutUnit
     *            the timeout unit
     * @param pollInterval
     *            the poll interval (0 or greater)
     * @param pollIntervalUnit
     *            the poll interval unit
     * @param condition
     *            the condition
     * @return true if the condition evaluated to true within the timeout, or false otherwise
     * @throws InterruptedException
     *             if the thread is interrupted
     * @throws IllegalArgumentException
     *             if timeout or pollInterval are less than 0
     * @throws NullPointerException
     *             if timeoutUnit, pollIntervalUnit, or condition are null
     */
    public static boolean blockUntil(long timeout, TimeUnit timeoutUnit, long pollInterval, TimeUnit pollIntervalUnit, BooleanSupplier condition)
                    throws InterruptedException {
        if (timeout < 0) {
            throw new IllegalArgumentException("timeout must be 0 or greater");
        }
        requireNonNull(timeoutUnit, "timeout unit cannot be null");
        if (pollInterval < 0) {
            throw new IllegalArgumentException("pollInterval must be 0 or greater");
        }
        requireNonNull(pollIntervalUnit, "pollIntervalUnit cannot be null");
        requireNonNull(condition, "condition cannot be null");

        // TimeUnit.toNanos saturates at Long.MAX_VALUE rather than overflowing.
        long deadline = System.nanoTime() + timeoutUnit.toNanos(timeout);

        // Require a minimum poll interval of MIN_POLL_INTERVAL_NANOS to avoid busy-waiting.
        long pollIntervalNanos = Math.max(pollIntervalUnit.toNanos(pollInterval), MIN_POLL_INTERVAL_NANOS);

        // If the condition does not return true yet, sleep for another interval.
        while (!condition.getAsBoolean()) {
            // System.nanoTime() has an arbitrary origin (it may be negative). The deadline must be compared by difference, and not by the comparison of the
            // raw values.
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                return false;
            }
            // Sleep for either the poll interval or the remaining time until the timeout, whichever one is shorter.
            TimeUnit.NANOSECONDS.sleep(Math.min(pollIntervalNanos, remaining));
        }
        return true;
    }

    private ThreadUtils() {
        throw new UnsupportedOperationException();
    }
}
