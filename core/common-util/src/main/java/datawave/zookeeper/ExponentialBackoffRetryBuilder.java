package datawave.zookeeper;

import java.util.Objects;
import java.util.StringJoiner;

import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * Implementation of {@link RetryPolicyBuilder} that provides instances of {@link ExponentialBackoffRetry}. This retry policy will retry a set number of times
 * with increasing sleep time between retries.
 */
public class ExponentialBackoffRetryBuilder implements RetryPolicyBuilder<ExponentialBackoffRetry> {

    /**
     * The initial amount of time in milliseconds to wait between retries.
     */
    private int baseSleepTimeMs = 1000;

    /**
     * The maximum number of times to retry.
     */
    private int maxRetries = 3;

    /**
     * The maximum amount of time in milliseconds to sleep between retries.
     */
    private int maxSleepMs = 60_000;

    /**
     * Default constructor.
     */
    public ExponentialBackoffRetryBuilder() {}

    /**
     * Copy constructor.
     *
     * @param other
     *            the instance to copy
     */
    public ExponentialBackoffRetryBuilder(ExponentialBackoffRetryBuilder other) {
        this.baseSleepTimeMs = other.baseSleepTimeMs;
        this.maxRetries = other.maxRetries;
        this.maxSleepMs = other.maxSleepMs;
    }

    /**
     * Return the initial amount of time in milliseconds to wait between retries.
     *
     * @return the initial amount of time
     */
    public int getBaseSleepTimeMs() {
        return baseSleepTimeMs;
    }

    /**
     * Set the initial amount of time in milliseconds to wait between retries.
     *
     * @param baseSleepTimeMs
     *            the time
     * @return this {@link ExponentialBackoffRetryBuilder}
     */
    public ExponentialBackoffRetryBuilder setBaseSleepTimeMs(int baseSleepTimeMs) {
        this.baseSleepTimeMs = baseSleepTimeMs;
        return this;
    }

    /**
     * Return the maximum number of times to retry
     *
     * @return the maximum retries
     */
    public int getMaxRetries() {
        return maxRetries;
    }

    /**
     * Set the maximum number of times to retry
     *
     * @param maxRetries
     *            the maximum retries
     * @return this {@link ExponentialBackoffRetryBuilder}
     */
    public ExponentialBackoffRetryBuilder setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
        return this;
    }

    /**
     * Return the maximum amount of time in milliseconds to sleep between retries.
     *
     * @return the maximum sleep time
     */
    public int getMaxSleepMs() {
        return maxSleepMs;
    }

    /**
     * Set the maximum amount of time in milliseconds to sleep between retries.
     *
     * @param maxSleepMs
     *            the maximum sleep time
     * @return this {@link ExponentialBackoffRetryBuilder}
     */
    public ExponentialBackoffRetryBuilder setMaxSleepMs(int maxSleepMs) {
        this.maxSleepMs = maxSleepMs;
        return this;
    }

    /**
     * Return a new {@link ExponentialBackoffRetry}
     *
     * @return the retry policy
     */
    @Override
    public ExponentialBackoffRetry build() {
        return new ExponentialBackoffRetry(baseSleepTimeMs, maxRetries, maxSleepMs);
    }

    /**
     * Return a duplicate of this {@link ExponentialBackoffRetryBuilder}.
     *
     * @return the duplicate
     */
    @Override
    public ExponentialBackoffRetryBuilder duplicate() {
        return new ExponentialBackoffRetryBuilder(this);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ExponentialBackoffRetryBuilder that = (ExponentialBackoffRetryBuilder) o;
        return baseSleepTimeMs == that.baseSleepTimeMs && maxRetries == that.maxRetries && maxSleepMs == that.maxSleepMs;
    }

    @Override
    public int hashCode() {
        return Objects.hash(baseSleepTimeMs, maxRetries, maxSleepMs);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", ExponentialBackoffRetryBuilder.class.getSimpleName() + "[", "]").add("baseSleepTimeMs=" + baseSleepTimeMs)
                        .add("maxRetries=" + maxRetries).add("maxSleepMs=" + maxSleepMs).toString();
    }
}
