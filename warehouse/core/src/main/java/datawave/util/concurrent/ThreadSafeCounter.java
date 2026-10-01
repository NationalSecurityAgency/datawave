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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Thread-safe counter implementations to replace non-atomic primitive counters.
 * <p>
 * This class addresses SonarQube issue: "Use atomic variables instead of synchronized methods"
 * Reference: <a href="https://wiki.sei.cmu.edu/confluence/x/SjdGBQ">SEI CERT VNA02-J</a>
 * </p>
 * <p>
 * For DataWave Issue #2321: Fix Major issues in SonarQube
 * </p>
 * 
 * <h2>Migration Examples</h2>
 * 
 * <h3>Before (Vulnerable):</h3>
 * <pre>{@code
 * public class QueryMetrics {
 *     private int queryCount = 0;  // Not thread-safe!
 *     
 *     public void incrementCount() {
 *         queryCount++;  // Race condition!
 *     }
 *     
 *     public int getCount() {
 *         return queryCount;
 *     }
 * }
 * }</pre>
 * 
 * <h3>After (Thread-Safe):</h3>
 * <pre>{@code
 * public class QueryMetrics {
 *     private final ThreadSafeCounter.PreciseCounter queryCount = 
 *         new ThreadSafeCounter.PreciseCounter();
 *     
 *     public void incrementCount() {
 *         queryCount.increment();  // Thread-safe!
 *     }
 *     
 *     public long getCount() {
 *         return queryCount.get();
 *     }
 * }
 * }</pre>
 * 
 * <h3>For High-Throughput Scenarios:</h3>
 * <pre>{@code
 * // When many threads increment frequently and reads are less common
 * private final ThreadSafeCounter.HighThroughputCounter eventCount = 
 *     new ThreadSafeCounter.HighThroughputCounter();
 * }</pre>
 */
public final class ThreadSafeCounter {
    
    private ThreadSafeCounter() {
        // Utility class - prevent instantiation
    }
    
    /**
     * A thread-safe counter using AtomicLong.
     * <p>
     * Best for scenarios where:
     * <ul>
     *   <li>You need precise counts at any moment</li>
     *   <li>Read frequency is similar to write frequency</li>
     *   <li>You need atomic compare-and-set operations</li>
     * </ul>
     * </p>
     */
    public static class PreciseCounter {
        private final AtomicLong count;
        
        /**
         * Create a counter initialized to zero.
         */
        public PreciseCounter() {
            this.count = new AtomicLong(0);
        }
        
        /**
         * Create a counter initialized to the given value.
         *
         * @param initialValue the initial count
         */
        public PreciseCounter(long initialValue) {
            this.count = new AtomicLong(initialValue);
        }
        
        /**
         * Atomically increment the counter by one.
         *
         * @return the updated value
         */
        public long increment() {
            return count.incrementAndGet();
        }
        
        /**
         * Atomically increment the counter by the given delta.
         *
         * @param delta the value to add (can be negative)
         * @return the updated value
         */
        public long add(long delta) {
            return count.addAndGet(delta);
        }
        
        /**
         * Atomically decrement the counter by one.
         *
         * @return the updated value
         */
        public long decrement() {
            return count.decrementAndGet();
        }
        
        /**
         * Get the current count.
         *
         * @return the current count
         */
        public long get() {
            return count.get();
        }
        
        /**
         * Set the counter to a specific value.
         *
         * @param newValue the new value
         */
        public void set(long newValue) {
            count.set(newValue);
        }
        
        /**
         * Reset the counter to zero.
         */
        public void reset() {
            count.set(0);
        }
        
        /**
         * Atomically set to newValue and return the old value.
         *
         * @param newValue the new value
         * @return the previous value
         */
        public long getAndSet(long newValue) {
            return count.getAndSet(newValue);
        }
        
        /**
         * Compare and set atomically.
         *
         * @param expected the expected value
         * @param update the new value
         * @return true if successful
         */
        public boolean compareAndSet(long expected, long update) {
            return count.compareAndSet(expected, update);
        }
        
        /**
         * Atomically increment and return the OLD value.
         *
         * @return the value before increment
         */
        public long getAndIncrement() {
            return count.getAndIncrement();
        }
        
        /**
         * Atomically decrement and return the OLD value.
         *
         * @return the value before decrement
         */
        public long getAndDecrement() {
            return count.getAndDecrement();
        }
        
        @Override
        public String toString() {
            return String.valueOf(count.get());
        }
    }
    
    /**
     * A thread-safe integer counter using AtomicInteger.
     * <p>
     * Use this when you know your count will fit in an int (max ~2 billion).
     * Slightly more memory efficient than PreciseCounter for large numbers of counters.
     * </p>
     */
    public static class IntCounter {
        private final AtomicInteger count;
        
        /**
         * Create a counter initialized to zero.
         */
        public IntCounter() {
            this.count = new AtomicInteger(0);
        }
        
        /**
         * Create a counter initialized to the given value.
         *
         * @param initialValue the initial count
         */
        public IntCounter(int initialValue) {
            this.count = new AtomicInteger(initialValue);
        }
        
        /**
         * Atomically increment the counter by one.
         *
         * @return the updated value
         */
        public int increment() {
            return count.incrementAndGet();
        }
        
        /**
         * Atomically increment the counter by the given delta.
         *
         * @param delta the value to add
         * @return the updated value
         */
        public int add(int delta) {
            return count.addAndGet(delta);
        }
        
        /**
         * Atomically decrement the counter by one.
         *
         * @return the updated value
         */
        public int decrement() {
            return count.decrementAndGet();
        }
        
        /**
         * Get the current count.
         *
         * @return the current count
         */
        public int get() {
            return count.get();
        }
        
        /**
         * Set the counter to a specific value.
         *
         * @param newValue the new value
         */
        public void set(int newValue) {
            count.set(newValue);
        }
        
        /**
         * Reset the counter to zero.
         */
        public void reset() {
            count.set(0);
        }
        
        /**
         * Compare and set atomically.
         *
         * @param expected the expected value
         * @param update the new value
         * @return true if successful
         */
        public boolean compareAndSet(int expected, int update) {
            return count.compareAndSet(expected, update);
        }
        
        @Override
        public String toString() {
            return String.valueOf(count.get());
        }
    }
    
    /**
     * A thread-safe counter optimized for high-contention writes using LongAdder.
     * <p>
     * Best for scenarios where:
     * <ul>
     *   <li>Many threads are incrementing frequently</li>
     *   <li>Reads (sum()) are less frequent than writes</li>
     *   <li>You don't need atomic compare-and-set</li>
     *   <li>Exact point-in-time accuracy is not critical</li>
     * </ul>
     * </p>
     * <p>
     * <strong>Note:</strong> sum() is NOT atomic - it may not include concurrent updates.
     * Use {@link PreciseCounter} if you need atomic reads.
     * </p>
     */
    public static class HighThroughputCounter {
        private final LongAdder adder;
        
        /**
         * Create a counter initialized to zero.
         */
        public HighThroughputCounter() {
            this.adder = new LongAdder();
        }
        
        /**
         * Increment the counter by one.
         * <p>
         * This is more efficient than AtomicLong under high contention
         * because it uses striped cells to reduce CAS conflicts.
         * </p>
         */
        public void increment() {
            adder.increment();
        }
        
        /**
         * Add the given value to the counter.
         *
         * @param delta the value to add (can be negative)
         */
        public void add(long delta) {
            adder.add(delta);
        }
        
        /**
         * Decrement the counter by one.
         */
        public void decrement() {
            adder.decrement();
        }
        
        /**
         * Get the current sum.
         * <p>
         * <strong>Note:</strong> This is NOT atomic with respect to updates.
         * The returned value may not include all concurrent updates.
         * </p>
         *
         * @return the current sum
         */
        public long sum() {
            return adder.sum();
        }
        
        /**
         * Equivalent to sum(), for consistency with other counter APIs.
         *
         * @return the current sum
         */
        public long get() {
            return adder.sum();
        }
        
        /**
         * Reset to zero.
         * <p>
         * <strong>Note:</strong> This is NOT atomic with respect to updates.
         * </p>
         */
        public void reset() {
            adder.reset();
        }
        
        /**
         * Sum then reset atomically.
         * <p>
         * This IS atomic - useful for periodic reporting.
         * </p>
         *
         * @return the sum before reset
         */
        public long sumThenReset() {
            return adder.sumThenReset();
        }
        
        @Override
        public String toString() {
            return String.valueOf(adder.sum());
        }
    }
    
    // ==================== Factory Methods ====================
    
    /**
     * Create a new precise counter initialized to zero.
     *
     * @return a new PreciseCounter
     */
    public static PreciseCounter newPreciseCounter() {
        return new PreciseCounter();
    }
    
    /**
     * Create a new precise counter with initial value.
     *
     * @param initialValue the initial value
     * @return a new PreciseCounter
     */
    public static PreciseCounter newPreciseCounter(long initialValue) {
        return new PreciseCounter(initialValue);
    }
    
    /**
     * Create a new integer counter initialized to zero.
     *
     * @return a new IntCounter
     */
    public static IntCounter newIntCounter() {
        return new IntCounter();
    }
    
    /**
     * Create a new integer counter with initial value.
     *
     * @param initialValue the initial value
     * @return a new IntCounter
     */
    public static IntCounter newIntCounter(int initialValue) {
        return new IntCounter(initialValue);
    }
    
    /**
     * Create a new high-throughput counter.
     *
     * @return a new HighThroughputCounter
     */
    public static HighThroughputCounter newHighThroughputCounter() {
        return new HighThroughputCounter();
    }
}
