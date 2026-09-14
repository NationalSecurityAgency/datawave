package datawave.zookeeper;

import java.util.Objects;
import java.util.StringJoiner;

import org.apache.curator.retry.RetryNTimes;

/**
 * Implementation of {@link RetryPolicyBuilder} that provides instances of {@link RetryNTimes}.
 */
public class RetryNTimesBuilder implements RetryPolicyBuilder<RetryNTimes> {

    /**
     * The maximum number of times to retry.
     */
    private int maxRetries = 5;

    /**
     * The time in milliseconds to sleep between retries.
     */
    private int sleepBetweenRetriesMs = 1000;

    /**
     * Default constructor.
     */
    public RetryNTimesBuilder() {}

    /**
     * Copy constructor.
     *
     * @param other
     *            the instance to copy
     */
    public RetryNTimesBuilder(RetryNTimesBuilder other) {
        this.maxRetries = other.maxRetries;
        this.sleepBetweenRetriesMs = other.sleepBetweenRetriesMs;
    }

    /**
     * Return the maximum number of times to retry.
     *
     * @return the maximum of retries
     */
    public int getMaxRetries() {
        return maxRetries;
    }

    /**
     * Set the maximum number of times to retry.
     *
     * @param maxRetries
     *            the maximum retries
     * @return this {@link RetryNTimesBuilder}
     */
    public RetryNTimesBuilder setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
        return this;
    }

    /**
     * Return the time in milliseconds to sleep between retries.
     *
     * @return the time
     */
    public int getSleepBetweenRetriesMs() {
        return sleepBetweenRetriesMs;
    }

    /**
     * Set the time in milliseconds to sleep between retries.
     *
     * @param sleepBetweenRetriesMs
     *            the time
     * @return this {@link RetryNTimesBuilder}
     */
    public RetryNTimesBuilder setSleepBetweenRetriesMs(int sleepBetweenRetriesMs) {
        this.sleepBetweenRetriesMs = sleepBetweenRetriesMs;
        return this;
    }

    /**
     * Build and return a new {@link RetryNTimes}.
     *
     * @return the new retry policy
     */
    @Override
    public RetryNTimes build() {
        return new RetryNTimes(maxRetries, sleepBetweenRetriesMs);
    }

    /**
     * Return a duplicate of this {@link RetryNTimesBuilder}.
     *
     * @return the duplicate
     */
    @Override
    public RetryNTimesBuilder duplicate() {
        return new RetryNTimesBuilder(this);
    }

    @Override
    public boolean equals(Object object) {
        if (object == null || getClass() != object.getClass())
            return false;
        RetryNTimesBuilder that = (RetryNTimesBuilder) object;
        return maxRetries == that.maxRetries && sleepBetweenRetriesMs == that.sleepBetweenRetriesMs;
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxRetries, sleepBetweenRetriesMs);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", RetryNTimesBuilder.class.getSimpleName() + "[", "]").add("maxRetries=" + maxRetries)
                        .add("sleepMsBetweenRetries=" + sleepBetweenRetriesMs).toString();
    }
}
