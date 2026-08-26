package datawave.zookeeper;

import java.util.Objects;
import java.util.StringJoiner;

import org.apache.curator.RetryPolicy;
import org.apache.curator.retry.RetryNTimes;

/**
 * Implementation of {@link RetryPolicyBuilder} that provides instances of {@link RetryNTimes}.
 */
public class RetryNTimesBuilder implements RetryPolicyBuilder {

    /**
     * The number of times the Zookeeper client should attempt to reconnect to Zookeeper after a connection loss. Defaults to 10.
     */
    private int n = 10;

    /**
     * The time in milliseconds the Zookeeper client should sleep between attempts to establish a connection to Zookeeper. Defaults to 1000.
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
        this.n = other.n;
        this.sleepBetweenRetriesMs = other.sleepBetweenRetriesMs;
    }

    /**
     * Get the number of times the Zookeeper client should attempt to establish a connection to Zookeeper.
     *
     * @return the retry times
     */
    public int getN() {
        return n;
    }

    /**
     * Set the number of times the Zookeeper client should attempt to establish a connection to Zookeeper.
     *
     * @param n
     *            the retry times
     */
    public void setN(int n) {
        this.n = n;
    }

    public RetryNTimesBuilder withN(int n) {
        setN(n);
        return this;
    }

    /**
     * Return the time in milliseconds the Zookeeper client should sleep between attempts to establish a connection to Zookeeper
     *
     * @return the time in ms
     */
    public int getSleepBetweenRetriesMs() {
        return sleepBetweenRetriesMs;
    }

    /**
     * Set the time in milliseconds the Zookeeper client should sleep between attempts to establish a connection to Zookeeper
     *
     * @param sleepBetweenRetriesMs
     *            the time in ms
     */
    public void setSleepBetweenRetriesMs(int sleepBetweenRetriesMs) {
        this.sleepBetweenRetriesMs = sleepBetweenRetriesMs;
    }

    public RetryNTimesBuilder withSleepBetweenRetriesMs(int sleepBetweenRetriesMs) {
        setSleepBetweenRetriesMs(sleepBetweenRetriesMs);
        return this;
    }

    @Override
    public RetryPolicy build() {
        return new RetryNTimes(n, sleepBetweenRetriesMs);
    }

    @Override
    public RetryPolicyBuilder duplicate() {
        return new RetryNTimesBuilder(this);
    }

    public boolean equals(Object object) {
        if (object == null || getClass() != object.getClass())
            return false;
        RetryNTimesBuilder that = (RetryNTimesBuilder) object;
        return n == that.n && sleepBetweenRetriesMs == that.sleepBetweenRetriesMs;
    }

    @Override
    public int hashCode() {
        return Objects.hash(n, sleepBetweenRetriesMs);
    }

    @Override
    public String toString() {
        // @formatter:off
        return new StringJoiner(", ", RetryNTimesBuilder.class.getSimpleName() + "[", "]")
                .add("n=" + n)
                .add("sleepMsBetweenRetries=" + sleepBetweenRetriesMs)
                .toString();
        // @formatter:on
    }
}
