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

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Supplier;

/**
 * Utilities for proper synchronization patterns.
 * <p>
 * This class addresses SonarQube issue: "Method parameter used for synchronization"
 * (also known as: "Synchronization on method parameters")
 * </p>
 * <p>
 * For DataWave Issue #2321: Fix Major issues in SonarQube
 * </p>
 * 
 * <h2>The Problem</h2>
 * <p>
 * Synchronizing on method parameters is dangerous because:
 * <ul>
 *   <li>Different callers may pass different objects</li>
 *   <li>This defeats the purpose of synchronization</li>
 *   <li>Can lead to race conditions and data corruption</li>
 * </ul>
 * </p>
 * 
 * <h2>Migration Examples</h2>
 * 
 * <h3>Before (WRONG - SonarQube violation):</h3>
 * <pre>{@code
 * public void updateCache(Object lock, String key, String value) {
 *     synchronized (lock) {  // BAD: synchronizing on parameter!
 *         cache.put(key, value);
 *     }
 * }
 * }</pre>
 * 
 * <h3>After (Option 1 - Using SynchronizedExecutor):</h3>
 * <pre>{@code
 * public class CacheManager {
 *     private final SynchronizedExecutor cacheSync = new SynchronizedExecutor();
 *     
 *     public void updateCache(String key, String value) {
 *         cacheSync.execute(() -> cache.put(key, value));
 *     }
 * }
 * }</pre>
 * 
 * <h3>After (Option 2 - Using private lock object):</h3>
 * <pre>{@code
 * public class CacheManager {
 *     private final Object cacheLock = new Object();  // Dedicated lock
 *     
 *     public void updateCache(String key, String value) {
 *         synchronized (cacheLock) {  // GOOD: class-level lock
 *             cache.put(key, value);
 *         }
 *     }
 * }
 * }</pre>
 * 
 * <h3>For Read-Heavy Workloads:</h3>
 * <pre>{@code
 * private final ReadWriteLockExecutor rwLock = new ReadWriteLockExecutor();
 * 
 * public String getValue(String key) {
 *     return rwLock.read(() -> cache.get(key));  // Multiple readers allowed
 * }
 * 
 * public void setValue(String key, String value) {
 *     rwLock.write(() -> cache.put(key, value));  // Exclusive access
 * }
 * }</pre>
 */
public final class SynchronizationUtils {
    
    private SynchronizationUtils() {
        // Utility class - prevent instantiation
    }
    
    /**
     * Provides synchronized execution with a dedicated internal lock object.
     * <p>
     * Use one instance per resource that needs protection.
     * The lock object is internal and cannot be accessed by callers,
     * ensuring consistent synchronization.
     * </p>
     */
    public static class SynchronizedExecutor {
        private final Object lock = new Object();
        
        /**
         * Execute a runnable within a synchronized block.
         *
         * @param action the action to execute
         */
        public void execute(Runnable action) {
            synchronized (lock) {
                action.run();
            }
        }
        
        /**
         * Execute a supplier within a synchronized block and return result.
         *
         * @param supplier the supplier to execute
         * @param <T> the return type
         * @return the result
         */
        public <T> T execute(Supplier<T> supplier) {
            synchronized (lock) {
                return supplier.get();
            }
        }
        
        /**
         * Execute a callable within a synchronized block.
         *
         * @param callable the callable to execute
         * @param <T> the return type
         * @return the result
         * @throws Exception if the callable throws
         */
        public <T> T call(Callable<T> callable) throws Exception {
            synchronized (lock) {
                return callable.call();
            }
        }
        
        /**
         * Get the underlying lock object.
         * <p>
         * <strong>Warning:</strong> Only use this if you need to synchronize
         * multiple operations atomically across method calls. Prefer using
         * execute() methods when possible.
         * </p>
         *
         * @return the lock object
         */
        public Object getLock() {
            return lock;
        }
    }
    
    /**
     * Provides a reentrant lock wrapper for finer control.
     * <p>
     * Use this when you need:
     * <ul>
     *   <li>Try-lock patterns (non-blocking lock attempts)</li>
     *   <li>Timed lock waits</li>
     *   <li>Interruptible lock acquisition</li>
     *   <li>Condition variables</li>
     * </ul>
     * </p>
     */
    public static class ReentrantLockExecutor {
        private final ReentrantLock lock;
        
        /**
         * Create with a non-fair lock.
         */
        public ReentrantLockExecutor() {
            this.lock = new ReentrantLock();
        }
        
        /**
         * Create with specified fairness.
         *
         * @param fair true for fair lock (FIFO ordering)
         */
        public ReentrantLockExecutor(boolean fair) {
            this.lock = new ReentrantLock(fair);
        }
        
        /**
         * Execute an action with the lock held.
         *
         * @param action the action to execute
         */
        public void execute(Runnable action) {
            lock.lock();
            try {
                action.run();
            } finally {
                lock.unlock();
            }
        }
        
        /**
         * Execute a supplier with the lock held.
         *
         * @param supplier the supplier to execute
         * @param <T> the return type
         * @return the result
         */
        public <T> T execute(Supplier<T> supplier) {
            lock.lock();
            try {
                return supplier.get();
            } finally {
                lock.unlock();
            }
        }
        
        /**
         * Try to execute an action if lock is immediately available.
         *
         * @param action the action to execute
         * @return true if executed, false if lock not available
         */
        public boolean tryExecute(Runnable action) {
            if (lock.tryLock()) {
                try {
                    action.run();
                    return true;
                } finally {
                    lock.unlock();
                }
            }
            return false;
        }
        
        /**
         * Try to execute an action with timeout.
         *
         * @param action the action to execute
         * @param timeout the maximum time to wait
         * @param unit the time unit
         * @return true if executed, false if timeout
         * @throws InterruptedException if interrupted while waiting
         */
        public boolean tryExecute(Runnable action, long timeout, TimeUnit unit)
                throws InterruptedException {
            if (lock.tryLock(timeout, unit)) {
                try {
                    action.run();
                    return true;
                } finally {
                    lock.unlock();
                }
            }
            return false;
        }
        
        /**
         * Execute with interruptible lock acquisition.
         *
         * @param action the action to execute
         * @throws InterruptedException if interrupted while waiting
         */
        public void executeInterruptibly(Runnable action) throws InterruptedException {
            lock.lockInterruptibly();
            try {
                action.run();
            } finally {
                lock.unlock();
            }
        }
        
        /**
         * Check if current thread holds the lock.
         *
         * @return true if held by current thread
         */
        public boolean isHeldByCurrentThread() {
            return lock.isHeldByCurrentThread();
        }
        
        /**
         * Get the number of holds by current thread.
         *
         * @return the hold count
         */
        public int getHoldCount() {
            return lock.getHoldCount();
        }
        
        /**
         * Check if any thread is waiting for this lock.
         *
         * @return true if threads are waiting
         */
        public boolean hasQueuedThreads() {
            return lock.hasQueuedThreads();
        }
        
        /**
         * Get the underlying Lock for advanced use cases.
         *
         * @return the lock
         */
        public Lock getLock() {
            return lock;
        }
    }
    
    /**
     * Provides read-write lock support for read-heavy workloads.
     * <p>
     * Multiple readers can access simultaneously, but writers have exclusive access.
     * Use this when reads significantly outnumber writes.
     * </p>
     */
    public static class ReadWriteLockExecutor {
        private final ReentrantReadWriteLock lock;
        
        /**
         * Create with non-fair lock.
         */
        public ReadWriteLockExecutor() {
            this.lock = new ReentrantReadWriteLock();
        }
        
        /**
         * Create with specified fairness.
         *
         * @param fair true for fair lock
         */
        public ReadWriteLockExecutor(boolean fair) {
            this.lock = new ReentrantReadWriteLock(fair);
        }
        
        /**
         * Execute a read operation. Multiple readers can run concurrently.
         *
         * @param supplier the read operation
         * @param <T> the return type
         * @return the result
         */
        public <T> T read(Supplier<T> supplier) {
            lock.readLock().lock();
            try {
                return supplier.get();
            } finally {
                lock.readLock().unlock();
            }
        }
        
        /**
         * Execute a void read operation.
         *
         * @param action the read operation
         */
        public void read(Runnable action) {
            lock.readLock().lock();
            try {
                action.run();
            } finally {
                lock.readLock().unlock();
            }
        }
        
        /**
         * Execute a write operation. Exclusive access is guaranteed.
         *
         * @param action the write operation
         */
        public void write(Runnable action) {
            lock.writeLock().lock();
            try {
                action.run();
            } finally {
                lock.writeLock().unlock();
            }
        }
        
        /**
         * Execute a write operation and return result.
         *
         * @param supplier the write operation
         * @param <T> the return type
         * @return the result
         */
        public <T> T write(Supplier<T> supplier) {
            lock.writeLock().lock();
            try {
                return supplier.get();
            } finally {
                lock.writeLock().unlock();
            }
        }
        
        /**
         * Try to execute a read operation if available.
         *
         * @param supplier the read operation
         * @param defaultValue value to return if lock not available
         * @param <T> the return type
         * @return the result or default
         */
        public <T> T tryRead(Supplier<T> supplier, T defaultValue) {
            if (lock.readLock().tryLock()) {
                try {
                    return supplier.get();
                } finally {
                    lock.readLock().unlock();
                }
            }
            return defaultValue;
        }
        
        /**
         * Try to execute a write operation if available.
         *
         * @param action the write operation
         * @return true if executed
         */
        public boolean tryWrite(Runnable action) {
            if (lock.writeLock().tryLock()) {
                try {
                    action.run();
                    return true;
                } finally {
                    lock.writeLock().unlock();
                }
            }
            return false;
        }
        
        /**
         * Get the number of read locks held.
         *
         * @return the read lock count
         */
        public int getReadLockCount() {
            return lock.getReadLockCount();
        }
        
        /**
         * Check if write lock is held.
         *
         * @return true if write locked
         */
        public boolean isWriteLocked() {
            return lock.isWriteLocked();
        }
        
        /**
         * Get the underlying ReadWriteLock for advanced use.
         *
         * @return the read-write lock
         */
        public ReadWriteLock getLock() {
            return lock;
        }
    }
    
    /**
     * Provides optimistic read lock support using StampedLock.
     * <p>
     * Use this for highest read performance when:
     * <ul>
     *   <li>Reads vastly outnumber writes</li>
     *   <li>Read operations are short</li>
     *   <li>You can retry reads if a write intervenes</li>
     * </ul>
     * </p>
     * <p>
     * <strong>Warning:</strong> StampedLock is NOT reentrant!
     * </p>
     */
    public static class OptimisticLockExecutor {
        private final StampedLock lock = new StampedLock();
        
        /**
         * Execute a read with optimistic locking.
         * <p>
         * First tries an optimistic read (no locking). If a write occurred
         * during the read, automatically retries with a pessimistic read lock.
         * </p>
         *
         * @param supplier the read operation
         * @param <T> the return type
         * @return the result
         */
        public <T> T optimisticRead(Supplier<T> supplier) {
            // First try optimistic read
            long stamp = lock.tryOptimisticRead();
            T result = supplier.get();
            
            // If valid, we're done
            if (lock.validate(stamp)) {
                return result;
            }
            
            // Otherwise, fall back to pessimistic read
            stamp = lock.readLock();
            try {
                return supplier.get();
            } finally {
                lock.unlockRead(stamp);
            }
        }
        
        /**
         * Execute a pessimistic read operation.
         *
         * @param supplier the read operation
         * @param <T> the return type
         * @return the result
         */
        public <T> T read(Supplier<T> supplier) {
            long stamp = lock.readLock();
            try {
                return supplier.get();
            } finally {
                lock.unlockRead(stamp);
            }
        }
        
        /**
         * Execute a write operation.
         *
         * @param action the write operation
         */
        public void write(Runnable action) {
            long stamp = lock.writeLock();
            try {
                action.run();
            } finally {
                lock.unlockWrite(stamp);
            }
        }
        
        /**
         * Execute a write operation and return result.
         *
         * @param supplier the write operation
         * @param <T> the return type
         * @return the result
         */
        public <T> T write(Supplier<T> supplier) {
            long stamp = lock.writeLock();
            try {
                return supplier.get();
            } finally {
                lock.unlockWrite(stamp);
            }
        }
    }
    
    // ==================== Factory Methods ====================
    
    /**
     * Create a new synchronized executor with a dedicated lock.
     *
     * @return a new SynchronizedExecutor
     */
    public static SynchronizedExecutor newSynchronizedExecutor() {
        return new SynchronizedExecutor();
    }
    
    /**
     * Create a new reentrant lock executor.
     *
     * @return a new ReentrantLockExecutor
     */
    public static ReentrantLockExecutor newReentrantLockExecutor() {
        return new ReentrantLockExecutor();
    }
    
    /**
     * Create a new fair reentrant lock executor.
     *
     * @return a new fair ReentrantLockExecutor
     */
    public static ReentrantLockExecutor newFairReentrantLockExecutor() {
        return new ReentrantLockExecutor(true);
    }
    
    /**
     * Create a new read-write lock executor.
     *
     * @return a new ReadWriteLockExecutor
     */
    public static ReadWriteLockExecutor newReadWriteLockExecutor() {
        return new ReadWriteLockExecutor();
    }
    
    /**
     * Create a new fair read-write lock executor.
     *
     * @return a new fair ReadWriteLockExecutor
     */
    public static ReadWriteLockExecutor newFairReadWriteLockExecutor() {
        return new ReadWriteLockExecutor(true);
    }
    
    /**
     * Create a new optimistic lock executor.
     *
     * @return a new OptimisticLockExecutor
     */
    public static OptimisticLockExecutor newOptimisticLockExecutor() {
        return new OptimisticLockExecutor();
    }
}
