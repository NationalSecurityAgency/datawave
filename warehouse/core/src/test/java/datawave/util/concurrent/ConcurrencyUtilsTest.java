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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.DisplayName;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for ThreadSafeCounter, InterruptibleTask, and SynchronizationUtils.
 * 
 * These tests verify:
 * - Thread safety under concurrent load
 * - Correct interrupt handling
 * - Proper synchronization behavior
 */
@DisplayName("Concurrency Utilities Tests")
class ConcurrencyUtilsTest {
    
    // ==================== ThreadSafeCounter.PreciseCounter Tests ====================
    
    @Test
    @DisplayName("PreciseCounter: Basic operations work correctly")
    void testPreciseCounterBasicOperations() {
        ThreadSafeCounter.PreciseCounter counter = new ThreadSafeCounter.PreciseCounter();
        
        assertEquals(0, counter.get());
        assertEquals(1, counter.increment());
        assertEquals(2, counter.increment());
        assertEquals(12, counter.add(10));
        assertEquals(11, counter.decrement());
        
        counter.reset();
        assertEquals(0, counter.get());
        
        counter.set(100);
        assertEquals(100, counter.get());
        
        assertEquals(100, counter.getAndSet(50));
        assertEquals(50, counter.get());
    }
    
    @Test
    @DisplayName("PreciseCounter: Compare and set works atomically")
    void testPreciseCounterCompareAndSet() {
        ThreadSafeCounter.PreciseCounter counter = new ThreadSafeCounter.PreciseCounter(100);
        
        assertTrue(counter.compareAndSet(100, 200));
        assertEquals(200, counter.get());
        
        assertFalse(counter.compareAndSet(100, 300)); // Should fail, not 100
        assertEquals(200, counter.get());
    }
    
    @Test
    @Timeout(30)
    @DisplayName("PreciseCounter: Thread-safe under high concurrency")
    void testPreciseCounterConcurrency() throws Exception {
        ThreadSafeCounter.PreciseCounter counter = new ThreadSafeCounter.PreciseCounter();
        int threadCount = 100;
        int incrementsPerThread = 10000;
        
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        
        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < incrementsPerThread; j++) {
                        counter.increment();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }
        
        startLatch.countDown();
        assertTrue(doneLatch.await(30, TimeUnit.SECONDS));
        executor.shutdown();
        
        assertEquals(threadCount * incrementsPerThread, counter.get(),
            "No increments should be lost due to race conditions");
    }
    
    @RepeatedTest(5)
    @Timeout(10)
    @DisplayName("PreciseCounter: Increment/decrement balance to zero")
    void testPreciseCounterBalance() throws Exception {
        ThreadSafeCounter.PreciseCounter counter = new ThreadSafeCounter.PreciseCounter();
        int operations = 50000;
        
        ExecutorService executor = Executors.newFixedThreadPool(4);
        CountDownLatch latch = new CountDownLatch(2);
        
        executor.submit(() -> {
            for (int i = 0; i < operations; i++) {
                counter.increment();
            }
            latch.countDown();
        });
        
        executor.submit(() -> {
            for (int i = 0; i < operations; i++) {
                counter.decrement();
            }
            latch.countDown();
        });
        
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        executor.shutdown();
        
        assertEquals(0, counter.get(), 
            "Equal increments and decrements should result in zero");
    }
    
    // ==================== ThreadSafeCounter.IntCounter Tests ====================
    
    @Test
    @DisplayName("IntCounter: Basic operations work correctly")
    void testIntCounterBasicOperations() {
        ThreadSafeCounter.IntCounter counter = new ThreadSafeCounter.IntCounter();
        
        assertEquals(0, counter.get());
        assertEquals(1, counter.increment());
        assertEquals(11, counter.add(10));
        assertEquals(10, counter.decrement());
        
        counter.reset();
        assertEquals(0, counter.get());
    }
    
    // ==================== ThreadSafeCounter.HighThroughputCounter Tests ====================
    
    @Test
    @DisplayName("HighThroughputCounter: Basic operations work correctly")
    void testHighThroughputCounterBasicOperations() {
        ThreadSafeCounter.HighThroughputCounter counter = 
            new ThreadSafeCounter.HighThroughputCounter();
        
        counter.increment();
        counter.increment();
        counter.add(10);
        counter.decrement();
        
        assertEquals(11, counter.sum());
        assertEquals(11, counter.get());
        
        long sumBefore = counter.sumThenReset();
        assertEquals(11, sumBefore);
        assertEquals(0, counter.sum());
    }
    
    @Test
    @Timeout(30)
    @DisplayName("HighThroughputCounter: Handles high contention")
    void testHighThroughputCounterHighContention() throws Exception {
        ThreadSafeCounter.HighThroughputCounter counter = 
            new ThreadSafeCounter.HighThroughputCounter();
        int threadCount = 200;
        int incrementsPerThread = 10000;
        
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        
        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                for (int j = 0; j < incrementsPerThread; j++) {
                    counter.increment();
                }
                latch.countDown();
            });
        }
        
        assertTrue(latch.await(30, TimeUnit.SECONDS));
        executor.shutdown();
        
        assertEquals(threadCount * incrementsPerThread, counter.sum());
    }
    
    // ==================== InterruptibleTask Tests ====================
    
    @Test
    @DisplayName("InterruptibleTask: Normal execution returns result")
    void testInterruptibleTaskNormalExecution() {
        String result = InterruptibleTask.run(() -> {
            Thread.sleep(10);
            return "success";
        });
        
        assertEquals("success", result);
    }
    
    @Test
    @Timeout(5)
    @DisplayName("InterruptibleTask: Interrupt status is restored on interruption")
    void testInterruptibleTaskRestoresInterruptStatus() {
        AtomicBoolean interruptStatusRestored = new AtomicBoolean(false);
        AtomicBoolean exceptionThrown = new AtomicBoolean(false);
        
        Thread testThread = new Thread(() -> {
            try {
                InterruptibleTask.sleepSeconds(10);
            } catch (InterruptibleTask.TaskInterruptedException e) {
                exceptionThrown.set(true);
                interruptStatusRestored.set(Thread.currentThread().isInterrupted());
            }
        });
        
        testThread.start();
        
        // Give thread time to start sleeping
        try { Thread.sleep(100); } catch (InterruptedException e) { 
            Thread.currentThread().interrupt(); 
        }
        
        testThread.interrupt();
        
        try {
            testThread.join(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        assertFalse(testThread.isAlive());
        assertTrue(exceptionThrown.get(), "Exception should be thrown");
        assertTrue(interruptStatusRestored.get(), "Interrupt status should be restored");
    }
    
    @Test
    @DisplayName("InterruptibleTask: Suppressed exception returns null")
    void testInterruptibleTaskSuppressedException() {
        Thread testThread = Thread.currentThread();
        
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.schedule(() -> testThread.interrupt(), 50, TimeUnit.MILLISECONDS);
        
        String result = InterruptibleTask.run(() -> {
            Thread.sleep(10000);
            return "should not reach";
        }, true);
        
        scheduler.shutdown();
        
        assertNull(result);
        assertTrue(Thread.interrupted()); // Clear and verify interrupt status was set
    }
    
    @Test
    @DisplayName("InterruptibleTask: runWithDefault returns default on interrupt")
    void testInterruptibleTaskWithDefault() {
        Thread testThread = Thread.currentThread();
        
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.schedule(() -> testThread.interrupt(), 50, TimeUnit.MILLISECONDS);
        
        String result = InterruptibleTask.runWithDefault(() -> {
            Thread.sleep(10000);
            return "not reached";
        }, "default_value");
        
        scheduler.shutdown();
        
        assertEquals("default_value", result);
        Thread.interrupted(); // Clear interrupt status
    }
    
    @Test
    @DisplayName("InterruptibleTask: sleepQuietly returns false on interrupt")
    void testSleepQuietly() {
        Thread testThread = Thread.currentThread();
        
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.schedule(() -> testThread.interrupt(), 50, TimeUnit.MILLISECONDS);
        
        boolean completed = InterruptibleTask.sleepQuietly(10, TimeUnit.SECONDS);
        
        scheduler.shutdown();
        
        assertFalse(completed);
        Thread.interrupted(); // Clear interrupt status
    }
    
    // ==================== SynchronizationUtils.SynchronizedExecutor Tests ====================
    
    @Test
    @DisplayName("SynchronizedExecutor: Basic execute works")
    void testSynchronizedExecutorBasic() {
        SynchronizationUtils.SynchronizedExecutor executor = 
            SynchronizationUtils.newSynchronizedExecutor();
        
        AtomicInteger counter = new AtomicInteger(0);
        
        executor.execute(() -> counter.incrementAndGet());
        executor.execute(() -> counter.addAndGet(10));
        
        assertEquals(11, counter.get());
    }
    
    @Test
    @DisplayName("SynchronizedExecutor: Execute with return value")
    void testSynchronizedExecutorWithReturn() {
        SynchronizationUtils.SynchronizedExecutor executor = 
            SynchronizationUtils.newSynchronizedExecutor();
        
        String result = executor.execute(() -> "hello");
        assertEquals("hello", result);
    }
    
    @Test
    @Timeout(30)
    @DisplayName("SynchronizedExecutor: Provides mutual exclusion")
    void testSynchronizedExecutorMutualExclusion() throws Exception {
        SynchronizationUtils.SynchronizedExecutor executor = 
            SynchronizationUtils.newSynchronizedExecutor();
        
        StringBuilder sharedBuilder = new StringBuilder();
        int threadCount = 50;
        CountDownLatch latch = new CountDownLatch(threadCount);
        
        ExecutorService threadPool = Executors.newFixedThreadPool(threadCount);
        
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            threadPool.submit(() -> {
                executor.execute(() -> {
                    // This read-modify-write would fail without synchronization
                    String current = sharedBuilder.toString();
                    try { Thread.sleep(1); } catch (InterruptedException e) { 
                        Thread.currentThread().interrupt(); 
                    }
                    sharedBuilder.append(threadId).append(",");
                });
                latch.countDown();
            });
        }
        
        assertTrue(latch.await(30, TimeUnit.SECONDS));
        threadPool.shutdown();
        
        String result = sharedBuilder.toString();
        for (int i = 0; i < threadCount; i++) {
            assertTrue(result.contains(i + ","), 
                "Thread " + i + " update should be present");
        }
    }
    
    // ==================== SynchronizationUtils.ReentrantLockExecutor Tests ====================
    
    @Test
    @DisplayName("ReentrantLockExecutor: tryExecute returns false when locked")
    void testReentrantLockExecutorTryExecute() throws Exception {
        SynchronizationUtils.ReentrantLockExecutor executor = 
            SynchronizationUtils.newReentrantLockExecutor();
        
        CountDownLatch lockAcquired = new CountDownLatch(1);
        CountDownLatch canRelease = new CountDownLatch(1);
        AtomicBoolean secondTrySucceeded = new AtomicBoolean(true);
        
        // Thread 1: Hold the lock
        Thread holder = new Thread(() -> {
            executor.execute(() -> {
                lockAcquired.countDown();
                try { canRelease.await(); } catch (InterruptedException e) { 
                    Thread.currentThread().interrupt(); 
                }
            });
        });
        holder.start();
        
        lockAcquired.await();
        
        // Thread 2: Try to acquire (should fail)
        secondTrySucceeded.set(executor.tryExecute(() -> {}));
        
        canRelease.countDown();
        holder.join(5000);
        
        assertFalse(secondTrySucceeded.get(), "tryExecute should fail when lock is held");
    }
    
    @Test
    @DisplayName("ReentrantLockExecutor: Is reentrant")
    void testReentrantLockExecutorReentrancy() {
        SynchronizationUtils.ReentrantLockExecutor executor = 
            SynchronizationUtils.newReentrantLockExecutor();
        
        AtomicBoolean innerExecuted = new AtomicBoolean(false);
        
        executor.execute(() -> {
            // Nested acquire should work
            executor.execute(() -> innerExecuted.set(true));
        });
        
        assertTrue(innerExecuted.get(), "Reentrant lock should allow nested acquisition");
    }
    
    // ==================== SynchronizationUtils.ReadWriteLockExecutor Tests ====================
    
    @Test
    @Timeout(10)
    @DisplayName("ReadWriteLockExecutor: Multiple readers can run concurrently")
    void testReadWriteLockConcurrentReaders() throws Exception {
        SynchronizationUtils.ReadWriteLockExecutor executor = 
            SynchronizationUtils.newReadWriteLockExecutor();
        
        int readerCount = 10;
        CountDownLatch readersStarted = new CountDownLatch(readerCount);
        CountDownLatch readersFinished = new CountDownLatch(readerCount);
        CountDownLatch allReadersInside = new CountDownLatch(1);
        AtomicBoolean concurrencyAchieved = new AtomicBoolean(false);
        
        ExecutorService threadPool = Executors.newFixedThreadPool(readerCount);
        
        for (int i = 0; i < readerCount; i++) {
            threadPool.submit(() -> {
                executor.read(() -> {
                    readersStarted.countDown();
                    try {
                        if (readersStarted.await(5, TimeUnit.SECONDS)) {
                            concurrencyAchieved.set(true);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
                readersFinished.countDown();
            });
        }
        
        assertTrue(readersFinished.await(10, TimeUnit.SECONDS));
        assertTrue(concurrencyAchieved.get(), "All readers should run concurrently");
        
        threadPool.shutdown();
    }
    
    @Test
    @Timeout(10)
    @DisplayName("ReadWriteLockExecutor: Writer excludes readers")
    void testReadWriteLockWriterExcludesReaders() throws Exception {
        SynchronizationUtils.ReadWriteLockExecutor executor = 
            SynchronizationUtils.newReadWriteLockExecutor();
        
        AtomicBoolean writerActive = new AtomicBoolean(false);
        AtomicBoolean readerSawWriterActive = new AtomicBoolean(false);
        CountDownLatch writerStarted = new CountDownLatch(1);
        CountDownLatch testComplete = new CountDownLatch(2);
        
        ExecutorService threadPool = Executors.newFixedThreadPool(2);
        
        // Start writer
        threadPool.submit(() -> {
            executor.write(() -> {
                writerActive.set(true);
                writerStarted.countDown();
                try { Thread.sleep(200); } catch (InterruptedException e) { 
                    Thread.currentThread().interrupt(); 
                }
                writerActive.set(false);
            });
            testComplete.countDown();
        });
        
        // Start reader after writer
        threadPool.submit(() -> {
            try { 
                writerStarted.await();
                Thread.sleep(50); // Ensure writer is in critical section
            } catch (InterruptedException e) { 
                Thread.currentThread().interrupt(); 
            }
            
            executor.read(() -> {
                if (writerActive.get()) {
                    readerSawWriterActive.set(true);
                }
            });
            testComplete.countDown();
        });
        
        assertTrue(testComplete.await(10, TimeUnit.SECONDS));
        assertFalse(readerSawWriterActive.get(), 
            "Reader should not see writer in critical section");
        
        threadPool.shutdown();
    }
    
    // ==================== SynchronizationUtils.OptimisticLockExecutor Tests ====================
    
    @Test
    @DisplayName("OptimisticLockExecutor: Optimistic read works without contention")
    void testOptimisticReadNoContention() {
        SynchronizationUtils.OptimisticLockExecutor executor = 
            SynchronizationUtils.newOptimisticLockExecutor();
        
        AtomicReference<String> data = new AtomicReference<>("initial");
        
        String result = executor.optimisticRead(data::get);
        
        assertEquals("initial", result);
    }
    
    @Test
    @DisplayName("OptimisticLockExecutor: Write excludes optimistic reads")
    void testOptimisticReadWithWrite() {
        SynchronizationUtils.OptimisticLockExecutor executor = 
            SynchronizationUtils.newOptimisticLockExecutor();
        
        AtomicReference<String> data = new AtomicReference<>("initial");
        
        // Write should work
        executor.write(() -> data.set("modified"));
        
        // Subsequent read should see modification
        String result = executor.optimisticRead(data::get);
        
        assertEquals("modified", result);
    }
}
