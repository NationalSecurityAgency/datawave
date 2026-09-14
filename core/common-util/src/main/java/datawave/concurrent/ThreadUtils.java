package datawave.concurrent;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

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
        Preconditions.checkNotNull(executor, "executor cannot be null");
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
     * Blocks the execution of the current thread until the given condition evaluates to true, or until the timeout has been exceeded.
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
     *             if timeoutMs or pollIntervalMs are less than 0
     * @throws NullPointerException
     *             if timeoutUnit, pollIntervalUnit, or condition are null
     */
    public static boolean blockUntil(long timeout, TimeUnit timeoutUnit, long pollInterval, TimeUnit pollIntervalUnit, BooleanSupplier condition)
                    throws InterruptedException {
        Preconditions.checkArgument(timeout >= 0, "timeout must be 0 or greater");
        Preconditions.checkNotNull(timeoutUnit, "timeout unit cannot be null");
        Preconditions.checkArgument(pollInterval >= 0, "pollInterval must be 0 or greater");
        Preconditions.checkNotNull(pollIntervalUnit, "pollIntervalUnit cannot be null");
        Preconditions.checkNotNull(condition, "condition cannot be null");

        long deadline = getDeadline(timeout, timeoutUnit);
        long pollIntervalNanos = convertOrCap(pollInterval, pollIntervalUnit::toNanos);

        // If the condition does not return true yet, sleep for another interval.
        while (!(condition.getAsBoolean())) {
            long currentTime = System.nanoTime();
            if (currentTime > deadline) {
                return false;
            }
            // Sleep for either the poll interval or the remaining time until the timeout, whichever one is shorter.
            long interval = Math.min(pollIntervalNanos, deadline - currentTime);
            // noinspection BusyWait
            Thread.sleep(TimeUnit.NANOSECONDS.toMillis(interval));
        }
        return true;
    }

    /**
     * Calculates the deadline of the given timeout based on a start time of the current system nano time. This method will return:
     * <ul>
     * <li>The current system nano time if the timeout is less than 1.</li>
     * <li>{@link Long#MAX_VALUE} if the calculated deadline would overflow into a negative number.</li>
     * <li>The deadline in nanos if there is no chance of overflow.</li>
     * </ul>
     *
     * @param timeout
     *            the timeout
     * @param timeoutUnit
     *            the timeout unit
     * @return the deadline in nanos
     * @throws NullPointerException
     *             if timeoutUnit is null
     */
    public static long getDeadline(long timeout, TimeUnit timeoutUnit) {
        if (timeout <= 0) {
            return System.nanoTime();
        }

        long timeoutNanos = convertOrCap(timeout, timeoutUnit::toNanos);
        // If timeoutNanos overflowed into a negative number, cap the deadline at Long.MAX_VALUE.
        if (timeoutNanos <= 0) {
            return Long.MAX_VALUE;
        } else {
            long now = System.nanoTime();
            // If adding now + timeoutNanos would result in an overflow, cap the deadline at Long.MAX_VALUE.
            if (Long.MAX_VALUE - timeoutNanos < now) {
                return Long.MAX_VALUE;
            } else {
                // Otherwise return the calculated deadline.
                return now + timeoutNanos;
            }
        }
    }

    /**
     * Returns the value of the given time converted by the given function, e.g. {@code TimeUnit::toMillis}. If the converted value is less than one, it is
     * assumed that the resulting long overflowed, and {@link Long#MAX_VALUE} will be returned.
     *
     * @param time
     *            the time
     * @param function
     *            the function to covert the time to a different unit
     * @return the conversion
     */
    public static long convertOrCap(long time, Function<Long,Long> function) {
        long conversion = function.apply(time);
        return conversion <= 0 ? Long.MAX_VALUE : conversion;
    }

    private ThreadUtils() {
        throw new UnsupportedOperationException();
    }
}
