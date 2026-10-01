/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package datawave.util.concurrent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * Utilities for properly handling InterruptedException according to CWE-391.
 * <p>
 * This class addresses SonarQube/CWE-391: "Interrupted exceptions caught, not re-thrown"
 * Reference: <a href="https://cwe.mitre.org/data/definitions/391">CWE-391</a>
 * </p>
 * <p>
 * For DataWave Issue #2321: Fix Major issues in SonarQube
 * </p>
 * 
 * <h2>The Problem</h2>
 * <p>
 * When a thread is interrupted, it's a signal that the thread should stop what it's doing.
 * Swallowing InterruptedException breaks this mechanism and can cause:
 * <ul>
 *   <li>Threads that won't terminate during shutdown</li>
 *   <li>Resource leaks</li>
 *   <li>Unresponsive applications</li>
 *   <li>Security issues (threads continuing when they should stop)</li>
 * </ul>
 * </p>
 * 
 * <h2>Migration Examples</h2>
 * 
 * <h3>Before (WRONG - CWE-391 Violation):</h3>
 * <pre>{@code
 * try {
 *     Thread.sleep(1000);
 * } catch (InterruptedException e) {
 *     log.error("Interrupted", e);  // Silent catch - BAD!
 * }
 * }</pre>
 * 
 * <h3>After (Option 1 - Using this utility):</h3>
 * <pre>{@code
 * InterruptibleTask.sleepMillis(1000);
 * // TaskInterruptedException thrown if interrupted, interrupt flag preserved
 * }</pre>
 * 
 * <h3>After (Option 2 - Manual fix pattern):</h3>
 * <pre>{@code
 * try {
 *     Thread.sleep(1000);
 * } catch (InterruptedException e) {
 *     Thread.currentThread().interrupt();  // CRITICAL: Restore interrupt flag
 *     throw new RuntimeException("Operation interrupted", e);
 * }
 * }</pre>
 * 
 * <h3>For methods that must handle interruption gracefully:</h3>
 * <pre>{@code
 * String result = InterruptibleTask.runInterruptible(() -> {
 *     Thread.sleep(100);
 *     return fetchData();
 * }, true);  // suppress = true, returns null on interrupt
 * }</pre>
 */
public final class InterruptibleTask {
    
    private static final Logger log = LoggerFactory.getLogger(InterruptibleTask.class);
    
    private InterruptibleTask() {
        // Utility class - prevent instantiation
    }
    
    /**
     * Exception thrown when a task is interrupted.
     * <p>
     * This is an unchecked exception that wraps InterruptedException while
     * ensuring the interrupt status is preserved on the current thread.
     * </p>
     */
    public static class TaskInterruptedException extends RuntimeException {
        private static final long serialVersionUID = 1L;
        
        /**
         * Create with message and cause.
         *
         * @param message the detail message
         * @param cause the InterruptedException
         */
        public TaskInterruptedException(String message, InterruptedException cause) {
            super(message, cause);
        }
        
        /**
         * Create with default message.
         *
         * @param cause the InterruptedException
         */
        public TaskInterruptedException(InterruptedException cause) {
            super("Task was interrupted", cause);
        }
        
        /**
         * Get the original InterruptedException.
         *
         * @return the cause as InterruptedException
         */
        @Override
        public InterruptedException getCause() {
            return (InterruptedException) super.getCause();
        }
    }
    
    /**
     * Functional interface for tasks that may be interrupted and return a value.
     *
     * @param <T> the return type
     */
    @FunctionalInterface
    public interface InterruptibleCallable<T> {
        /**
         * Execute the task.
         *
         * @return the result
         * @throws InterruptedException if interrupted
         */
        T call() throws InterruptedException;
    }
    
    /**
     * Functional interface for void tasks that may be interrupted.
     */
    @FunctionalInterface
    public interface InterruptibleRunnable {
        /**
         * Execute the task.
         *
         * @throws InterruptedException if interrupted
         */
        void run() throws InterruptedException;
    }
    
    // ==================== Core Execution Methods ====================
    
    /**
     * Execute an interruptible task with proper interrupt handling.
     * <p>
     * If the task is interrupted:
     * <ol>
     *   <li>The interrupt status is restored on the current thread</li>
     *   <li>A TaskInterruptedException is thrown</li>
     * </ol>
     * </p>
     *
     * @param task the task to execute
     * @param <T> the return type
     * @return the result of the task
     * @throws TaskInterruptedException if the task is interrupted
     */
    public static <T> T run(InterruptibleCallable<T> task) {
        try {
            return task.call();
        } catch (InterruptedException e) {
            // CRITICAL: Restore interrupt status before throwing
            Thread.currentThread().interrupt();
            log.debug("Task interrupted, interrupt status restored");
            throw new TaskInterruptedException(e);
        }
    }
    
    /**
     * Execute an interruptible void task with proper interrupt handling.
     *
     * @param task the task to execute
     * @throws TaskInterruptedException if the task is interrupted
     */
    public static void run(InterruptibleRunnable task) {
        try {
            task.run();
        } catch (InterruptedException e) {
            // CRITICAL: Restore interrupt status before throwing
            Thread.currentThread().interrupt();
            log.debug("Task interrupted, interrupt status restored");
            throw new TaskInterruptedException(e);
        }
    }
    
    /**
     * Execute a callable that may throw InterruptedException,
     * with the option to suppress the exception and return a default value.
     * <p>
     * <strong>Important:</strong> Even when suppressing, the interrupt status is restored.
     * </p>
     *
     * @param task the task
     * @param suppressException if true, returns null on interrupt instead of throwing
     * @param <T> the return type
     * @return the result or null if interrupted and suppressed
     */
    public static <T> T run(InterruptibleCallable<T> task, boolean suppressException) {
        if (!suppressException) {
            return run(task);
        }
        
        try {
            return task.call();
        } catch (InterruptedException e) {
            // CRITICAL: Always restore interrupt status, even when suppressing
            Thread.currentThread().interrupt();
            log.debug("Task interrupted (suppressed), interrupt status restored");
            return null;
        }
    }
    
    /**
     * Execute with default value on interruption.
     *
     * @param task the task
     * @param defaultValue value to return if interrupted
     * @param <T> the return type
     * @return the result or defaultValue if interrupted
     */
    public static <T> T runWithDefault(InterruptibleCallable<T> task, T defaultValue) {
        try {
            return task.call();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.debug("Task interrupted, returning default value");
            return defaultValue;
        }
    }
    
    // ==================== Sleep Methods ====================
    
    /**
     * Sleep for the specified duration with proper interrupt handling.
     *
     * @param duration the sleep duration
     * @param unit the time unit
     * @throws TaskInterruptedException if interrupted during sleep
     */
    public static void sleep(long duration, TimeUnit unit) {
        run(() -> unit.sleep(duration));
    }
    
    /**
     * Sleep for the specified milliseconds with proper interrupt handling.
     *
     * @param millis milliseconds to sleep
     * @throws TaskInterruptedException if interrupted during sleep
     */
    public static void sleepMillis(long millis) {
        run(() -> Thread.sleep(millis));
    }
    
    /**
     * Sleep for the specified seconds with proper interrupt handling.
     *
     * @param seconds seconds to sleep
     * @throws TaskInterruptedException if interrupted during sleep
     */
    public static void sleepSeconds(long seconds) {
        sleep(seconds, TimeUnit.SECONDS);
    }
    
    /**
     * Sleep without throwing exception, just return whether completed.
     *
     * @param duration the sleep duration
     * @param unit the time unit
     * @return true if sleep completed, false if interrupted
     */
    public static boolean sleepQuietly(long duration, TimeUnit unit) {
        try {
            unit.sleep(duration);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
    
    // ==================== Wait/Notify Methods ====================
    
    /**
     * Wait on an object with proper interrupt handling.
     *
     * @param lock the object to wait on (must hold its monitor)
     * @throws TaskInterruptedException if interrupted during wait
     */
    public static void waitOn(Object lock) {
        run(() -> {
            synchronized (lock) {
                lock.wait();
            }
        });
    }
    
    /**
     * Wait on an object with timeout and proper interrupt handling.
     *
     * @param lock the object to wait on (must hold its monitor)
     * @param timeout maximum time to wait in milliseconds
     * @throws TaskInterruptedException if interrupted during wait
     */
    public static void waitOn(Object lock, long timeout) {
        run(() -> {
            synchronized (lock) {
                lock.wait(timeout);
            }
        });
    }
    
    /**
     * Wait on an object with timeout, returning whether notified.
     *
     * @param lock the object to wait on (must hold its monitor)
     * @param timeout maximum time to wait
     * @param unit the time unit
     * @return true if notified, false if timeout or interrupted
     */
    public static boolean waitQuietly(Object lock, long timeout, TimeUnit unit) {
        try {
            synchronized (lock) {
                lock.wait(unit.toMillis(timeout));
                return true;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
    
    // ==================== Thread Join Methods ====================
    
    /**
     * Join a thread with proper interrupt handling.
     *
     * @param thread the thread to join
     * @throws TaskInterruptedException if interrupted during join
     */
    public static void join(Thread thread) {
        run((InterruptibleRunnable) thread::join);
    }
    
    /**
     * Join a thread with timeout and proper interrupt handling.
     *
     * @param thread the thread to join
     * @param timeout maximum time to wait in milliseconds
     * @throws TaskInterruptedException if interrupted during join
     */
    public static void join(Thread thread, long timeout) {
        run(() -> thread.join(timeout));
    }
    
    /**
     * Join a thread with timeout and time unit.
     *
     * @param thread the thread to join
     * @param timeout maximum time to wait
     * @param unit the time unit
     * @throws TaskInterruptedException if interrupted during join
     */
    public static void join(Thread thread, long timeout, TimeUnit unit) {
        run(() -> thread.join(unit.toMillis(timeout)));
    }
    
    /**
     * Join without throwing exception, return whether completed.
     *
     * @param thread the thread to join
     * @param timeout maximum time to wait
     * @param unit the time unit
     * @return true if joined, false if timeout or interrupted
     */
    public static boolean joinQuietly(Thread thread, long timeout, TimeUnit unit) {
        try {
            thread.join(unit.toMillis(timeout));
            return !thread.isAlive();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
    
    // ==================== Lock Methods ====================
    
    /**
     * Acquire a lock interruptibly with proper handling.
     *
     * @param lock the lock to acquire
     * @throws TaskInterruptedException if interrupted while waiting
     */
    public static void lockInterruptibly(Lock lock) {
        run(lock::lockInterruptibly);
    }
    
    /**
     * Try to acquire a lock with timeout.
     *
     * @param lock the lock to acquire
     * @param timeout maximum time to wait
     * @param unit the time unit
     * @return true if acquired, false if timeout
     * @throws TaskInterruptedException if interrupted while waiting
     */
    public static boolean tryLock(Lock lock, long timeout, TimeUnit unit) {
        return run(() -> lock.tryLock(timeout, unit));
    }
    
    /**
     * Await on a condition with proper interrupt handling.
     *
     * @param condition the condition to await
     * @throws TaskInterruptedException if interrupted while waiting
     */
    public static void await(Condition condition) {
        run((InterruptibleRunnable) condition::await);
    }
    
    /**
     * Await on a condition with timeout.
     *
     * @param condition the condition to await
     * @param timeout maximum time to wait
     * @param unit the time unit
     * @return true if signaled, false if timeout
     * @throws TaskInterruptedException if interrupted while waiting
     */
    public static boolean await(Condition condition, long timeout, TimeUnit unit) {
        return run(() -> condition.await(timeout, unit));
    }
    
    // ==================== Concurrent Utility Methods ====================
    
    /**
     * Await on a CountDownLatch with proper interrupt handling.
     *
     * @param latch the latch to await
     * @throws TaskInterruptedException if interrupted while waiting
     */
    public static void await(CountDownLatch latch) {
        run((InterruptibleRunnable) latch::await);
    }
    
    /**
     * Await on a CountDownLatch with timeout.
     *
     * @param latch the latch to await
     * @param timeout maximum time to wait
     * @param unit the time unit
     * @return true if counted down to zero, false if timeout
     * @throws TaskInterruptedException if interrupted while waiting
     */
    public static boolean await(CountDownLatch latch, long timeout, TimeUnit unit) {
        return run(() -> latch.await(timeout, unit));
    }
    
    /**
     * Take from a blocking queue with proper interrupt handling.
     *
     * @param queue the queue to take from
     * @param <T> the element type
     * @return the element taken
     * @throws TaskInterruptedException if interrupted while waiting
     */
    public static <T> T take(BlockingQueue<T> queue) {
        return run(queue::take);
    }
    
    /**
     * Poll from a blocking queue with timeout.
     *
     * @param queue the queue to poll from
     * @param timeout maximum time to wait
     * @param unit the time unit
     * @param <T> the element type
     * @return the element, or null if timeout
     * @throws TaskInterruptedException if interrupted while waiting
     */
    public static <T> T poll(BlockingQueue<T> queue, long timeout, TimeUnit unit) {
        return run(() -> queue.poll(timeout, unit));
    }
    
    /**
     * Put to a blocking queue with proper interrupt handling.
     *
     * @param queue the queue to put to
     * @param element the element to put
     * @param <T> the element type
     * @throws TaskInterruptedException if interrupted while waiting
     */
    public static <T> void put(BlockingQueue<T> queue, T element) {
        run(() -> queue.put(element));
    }
    
    /**
     * Get from a Future with proper interrupt handling.
     *
     * @param future the future to get from
     * @param <T> the result type
     * @return the result
     * @throws TaskInterruptedException if interrupted while waiting
     * @throws RuntimeException wrapping ExecutionException if computation failed
     */
    public static <T> T get(Future<T> future) {
        return run(() -> {
            try {
                return future.get();
            } catch (java.util.concurrent.ExecutionException e) {
                throw new RuntimeException("Future computation failed", e.getCause());
            }
        });
    }
    
    /**
     * Get from a Future with timeout.
     *
     * @param future the future to get from
     * @param timeout maximum time to wait
     * @param unit the time unit
     * @param <T> the result type
     * @return the result
     * @throws TaskInterruptedException if interrupted while waiting
     * @throws RuntimeException wrapping ExecutionException or TimeoutException
     */
    public static <T> T get(Future<T> future, long timeout, TimeUnit unit) {
        return run(() -> {
            try {
                return future.get(timeout, unit);
            } catch (java.util.concurrent.ExecutionException e) {
                throw new RuntimeException("Future computation failed", e.getCause());
            } catch (java.util.concurrent.TimeoutException e) {
                throw new RuntimeException("Future timed out", e);
            }
        });
    }
}
