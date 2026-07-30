package datawave.webservice.query.limit;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;

class QueryLimiterUtilsTest {

    /**
     * Verify behavior for {@link QueryLimiterUtils#normalizeUserDn(String)}.
     */
    @Test
    public void testNormalizeUserDn() {
        assertNull(QueryLimiterUtils.normalizeUserDn(null));
        assertEquals("", QueryLimiterUtils.normalizeUserDn("   "));
        assertEquals("cn=user", QueryLimiterUtils.normalizeUserDn(" CN=User "));
    }

    /**
     * Verify behavior for {@link QueryLimiterUtils#normalizeSystem(String)}.
     */
    @Test
    public void testNormalizeSystem() {
        assertEquals(QueryLimiterUtils.EMPTY_SYSTEM_FROM, QueryLimiterUtils.normalizeSystem(null));
        assertEquals(QueryLimiterUtils.EMPTY_SYSTEM_FROM, QueryLimiterUtils.normalizeSystem("   "));
        assertEquals("System-01", QueryLimiterUtils.normalizeSystem(" System-01 "));
    }

    /**
     * Verify behavior for {@link QueryLimiterUtils#normalizeQueryLogic(String)}.
     */
    @Test
    public void testNormalizeQueryLogic() {
        assertNull(QueryLimiterUtils.normalizeQueryLogic(null));
        assertEquals("", QueryLimiterUtils.normalizeQueryLogic("   "));
        assertEquals("TLDQueryLogic", QueryLimiterUtils.normalizeQueryLogic(" TLDQueryLogic "));
    }

    /**
     * Verify {@link QueryLimiterUtils#await(long, int, Supplier)} throws an exception when given a negative timeout.
     */
    @Test
    void testAwaitGivenNegativeTimeout() {
        assertThatThrownBy(() -> QueryLimiterUtils.await(-1, 100, () -> true)).isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContaining("timeoutMs must be 0 or greater");
    }

    /**
     * Verify {@link QueryLimiterUtils#await(long, int, Supplier)} throws an exception when given a negative poll interval.
     */
    @Test
    void testAwaitGivenNegativePollInterval() {
        assertThatThrownBy(() -> QueryLimiterUtils.await(60_000, -1, () -> true)).isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContaining("pollIntervalMs must be 0 or greater");
    }

    /**
     * Verify {@link QueryLimiterUtils#await(long, int, Supplier)} returns false when the condition never evaluates to true within the timeout.
     */
    @Test
    void testAwaitReturnsFalseWhenTimeoutExceeded() throws InterruptedException {
        assertFalse(QueryLimiterUtils.await(1000, 100, () -> false));
    }

    /**
     * Verify {@link QueryLimiterUtils#await(long, int, Supplier)} returns true when the condition evaluates to true within the timeout.
     */
    @Test
    void testAwaitReturnsTrueWhenTimeoutNotExceeded() throws InterruptedException {
        AtomicBoolean condition = new AtomicBoolean(false);
        // Set this condition to true 1 second in the future.
        CompletableFuture.runAsync(() -> condition.set(true), CompletableFuture.delayedExecutor(1000, TimeUnit.MILLISECONDS));
        // Verify the QueryLimiter returns true after the condition is set to true within the timeout of 3 seconds.
        assertTrue(QueryLimiterUtils.await(3000, 100, condition::get));
    }
}
