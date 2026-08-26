package datawave.zookeeper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ExponentialBackoffRetryBuilder}.
 */
class ExponentialBackoffRetryBuilderTest {

    /**
     * Verify that {@link ExponentialBackoffRetryBuilder#ExponentialBackoffRetryBuilder(ExponentialBackoffRetryBuilder)} returns a duplicate instance.
     */
    @Test
    void testCopyConstructor() {
        ExponentialBackoffRetryBuilder original = new ExponentialBackoffRetryBuilder().setBaseSleepTimeMs(200).setMaxRetries(7).setMaxSleepMs(9000);
        ExponentialBackoffRetryBuilder copy = new ExponentialBackoffRetryBuilder(original);

        assertEquals(original.getBaseSleepTimeMs(), copy.getBaseSleepTimeMs());
        assertEquals(original.getMaxRetries(), copy.getMaxRetries());
        assertEquals(original.getMaxSleepMs(), copy.getMaxSleepMs());
        assertEquals(original, copy);
        assertNotSame(original, copy);

        copy.setBaseSleepTimeMs(9999);

        assertEquals(200, original.getBaseSleepTimeMs());
        assertEquals(9999, copy.getBaseSleepTimeMs());
    }

    /**
     * Verify that {@link ExponentialBackoffRetryBuilder#duplicate()} returns a duplicate instance.
     */
    @Test
    void testDuplicate() {
        ExponentialBackoffRetryBuilder original = new ExponentialBackoffRetryBuilder().setBaseSleepTimeMs(300).setMaxRetries(4).setMaxSleepMs(8000);
        ExponentialBackoffRetryBuilder duplicate = original.duplicate();

        assertNotSame(original, duplicate);
        assertEquals(original, duplicate);
    }

    /**
     * Verify that {@link ExponentialBackoffRetryBuilder#build()} returns a configured {@link ExponentialBackoffRetry}.
     */
    @Test
    void testBuildReturnsConfiguredRetryPolicy() {
        ExponentialBackoffRetryBuilder builder = new ExponentialBackoffRetryBuilder().setBaseSleepTimeMs(150).setMaxRetries(6).setMaxSleepMs(4000);
        ExponentialBackoffRetry policy = builder.build();

        assertNotNull(policy);
        assertEquals(150, policy.getBaseSleepTimeMs());
        assertEquals(6, policy.getN());
    }

    /**
     * Verify that {@link ExponentialBackoffRetryBuilder#build()} always returns a new instance.
     */
    @Test
    void testBuildReturnsNewInstanceEachTime() {
        ExponentialBackoffRetryBuilder builder = new ExponentialBackoffRetryBuilder();
        ExponentialBackoffRetry first = builder.build();
        ExponentialBackoffRetry second = builder.build();
        assertNotSame(first, second);
    }
}
