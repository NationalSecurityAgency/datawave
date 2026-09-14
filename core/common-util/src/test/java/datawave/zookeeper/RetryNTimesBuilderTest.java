package datawave.zookeeper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import org.apache.curator.retry.RetryNTimes;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link RetryNTimesBuilder}.
 */
class RetryNTimesBuilderTest {

    /**
     * Verify that {@link RetryNTimesBuilder#RetryNTimesBuilder(RetryNTimesBuilder)} returns a duplicate instance.
     */
    @Test
    void testCopyConstructor() {
        RetryNTimesBuilder original = new RetryNTimesBuilder().setMaxRetries(8).setSleepBetweenRetriesMs(2000);
        RetryNTimesBuilder copy = new RetryNTimesBuilder(original);

        assertEquals(original.getMaxRetries(), copy.getMaxRetries());
        assertEquals(original.getSleepBetweenRetriesMs(), copy.getSleepBetweenRetriesMs());
        assertEquals(original, copy);
        assertNotSame(original, copy);

        copy.setMaxRetries(42);

        assertEquals(8, original.getMaxRetries());
        assertEquals(42, copy.getMaxRetries());
    }

    /**
     * Verify that {@link RetryNTimesBuilder#duplicate()} returns a duplicate instance.
     */
    @Test
    void testDuplicate() {
        RetryNTimesBuilder original = new RetryNTimesBuilder().setMaxRetries(3).setSleepBetweenRetriesMs(1500);
        RetryNTimesBuilder duplicate = original.duplicate();

        assertNotSame(original, duplicate);
        assertEquals(original, duplicate);
    }

    /**
     * Verify that {@link RetryNTimesBuilder#build()} returns a configured {@link RetryNTimesBuilder}.
     */
    @Test
    void testBuildReturnsConfiguredRetryPolicy() {
        RetryNTimesBuilder builder = new RetryNTimesBuilder().setMaxRetries(6).setSleepBetweenRetriesMs(750);
        RetryNTimes policy = builder.build();
        assertNotNull(policy);
        assertEquals(6, policy.getN());
        // Unfortunately, RetryNTimes does not expose sleepBetweenRetriesMs.
    }

    /**
     * Verify that {@link RetryNTimesBuilder#build()} always returns a new {@link RetryNTimesBuilder}.
     */
    @Test
    void testBuildReturnsNewInstanceEachTime() {
        RetryNTimesBuilder builder = new RetryNTimesBuilder();
        RetryNTimes first = builder.build();
        RetryNTimes second = builder.build();
        assertNotSame(first, second);
    }
}
