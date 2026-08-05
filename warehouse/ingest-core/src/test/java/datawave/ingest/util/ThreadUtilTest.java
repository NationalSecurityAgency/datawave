package datawave.ingest.util;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ThreadUtilTest {
    
    /**
     * Tests for {@link ThreadUtil#blockUntil(long, long, Supplier)}.
     */
    @Nested
    class BlockUntilTests {
        
        /**
         * Verify {@link ThreadUtil#blockUntil(long, long, Supplier)} throws an exception when given a negative timeout.
         */
        @Test
        void testNegativeTimeout() {
            assertThatThrownBy(() -> ThreadUtil.blockUntil(-1, 100, () -> true)).isInstanceOf(IllegalArgumentException.class)
                            .hasMessageContaining("timeoutMs must be 0 or greater");
        }
        
        /**
         * Verify {@link ThreadUtil#blockUntil(long, long, Supplier)} throws an exception when given a negative poll interval.
         */
        @Test
        void testNegativePollInterval() {
            assertThatThrownBy(() -> ThreadUtil.blockUntil(60_000, -1, () -> true)).isInstanceOf(IllegalArgumentException.class)
                            .hasMessageContaining("pollIntervalMs must be 0 or greater");
        }
        
        /**
         * Verify {@link ThreadUtil#blockUntil(long, long, Supplier)} returns false when the condition never evaluates to true within the timeout.
         */
        @Test
        void testTimeoutExceeded() throws InterruptedException {
            long startTime = System.currentTimeMillis();
            
            // Verify that blockUntil returns false.
            assertThat(ThreadUtil.blockUntil(1000, 100, () -> false)).isFalse();
            
            // Assert that the thread was blocked for at least 1000 ms.
            assertThat(System.currentTimeMillis() - startTime).isGreaterThanOrEqualTo(1000L);
        }
        
        /**
         * Verify {@link ThreadUtil#blockUntil(long, long, Supplier)} returns true when the condition evaluates to true within the timeout.
         */
        @Test
        void testTimeoutNotExceeded() throws InterruptedException {
            AtomicBoolean condition = new AtomicBoolean(false);
            long startTime = System.currentTimeMillis();
            
            // Set this condition to true 1 second in the future.
            CompletableFuture.runAsync(() -> condition.set(true), CompletableFuture.delayedExecutor(1000, TimeUnit.MILLISECONDS));
            
            // Verify that blockUntil returns true after the condition is set to true within the timeout of 3 seconds.
            assertThat(ThreadUtil.blockUntil(3000, 100, condition::get)).isTrue();
            
            // Assert that the thread was blocked for at least 1000 ms, and no more than 3000 ms.
            assertThat(System.currentTimeMillis() - startTime).isBetween(1000L, 3000L);
        }
        
    }
}
