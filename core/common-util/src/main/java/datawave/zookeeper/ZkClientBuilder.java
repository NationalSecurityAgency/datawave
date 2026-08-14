package datawave.zookeeper;

import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;

import com.google.common.base.Preconditions;

/**
 * A configurable Zookeeper client builder that can provide instances {@link CuratorFrameworkFactory} and {@link CuratorFramework}.
 */
public class ZkClientBuilder {

    /**
     * The string for connecting to zookeeper. This can either be a list of servers or a path to a zookeeper config file.
     */
    private String connectString;

    /**
     * The namespace for the Zookeeper client.
     */
    private String namespace;

    /**
     * The session timeout in milliseconds for the Zookeeper client.
     */
    private int sessionTimeoutMs = 60_000;

    /**
     * The connection timeout in milliseconds for the Zookeeper client.
     */
    private int connectionTimeoutMs = 60_000;

    /**
     * The builder for creating the retry policy of the Zookeeper client. Defaults to a {@link ExponentialBackoffRetryBuilder}.
     */
    private RetryPolicyBuilder<?> retryPolicyBuilder = new ExponentialBackoffRetryBuilder();

    /**
     * Default constructor.
     */
    public ZkClientBuilder() {}

    /**
     * Copy constructor.
     *
     * @param other
     *            the instance to copy
     */
    public ZkClientBuilder(ZkClientBuilder other) {
        this.connectString = other.connectString;
        this.namespace = other.namespace;
        this.sessionTimeoutMs = other.sessionTimeoutMs;
        this.connectionTimeoutMs = other.connectionTimeoutMs;
        this.retryPolicyBuilder = other.retryPolicyBuilder != null ? other.retryPolicyBuilder.duplicate() : null;
    }

    /**
     * Return the connect string.
     *
     * @return the connect string
     */
    public String getConnectString() {
        return connectString;
    }

    /**
     * Set the string for connecting to Zookeeper.
     *
     * @param connectString
     *            the connect string
     * @return this {@link ZkClientBuilder}
     */
    public ZkClientBuilder setConnectString(String connectString) {
        this.connectString = connectString;
        return this;
    }

    /**
     * Return the namespace for the Zookeeper client.
     *
     * @return the namespace
     */
    public String getNamespace() {
        return namespace;
    }

    /**
     * Set the namespace for the Zookeeper client.
     *
     * @param namespace
     *            the namespace
     * @return this {@link ZkClientBuilder}
     */
    public ZkClientBuilder setNamespace(String namespace) {
        this.namespace = namespace;
        return this;
    }

    /**
     * Return session timeout in milliseconds for the Zookeeper client.
     *
     * @return the session timeout
     */
    public int getSessionTimeoutMs() {
        return sessionTimeoutMs;
    }

    /**
     * Set the session timeout in milliseconds for the Zookeeper client.
     *
     * @param sessionTimeoutMs
     *            the session timeout
     * @return this {@link ZkClientBuilder}
     */
    public ZkClientBuilder setSessionTimeoutMs(int sessionTimeoutMs) {
        this.sessionTimeoutMs = sessionTimeoutMs;
        return this;
    }

    /**
     * Return the connection timeout in milliseconds for the Zookeeper client.
     *
     * @return the connection timeout
     */
    public int getConnectionTimeoutMs() {
        return connectionTimeoutMs;
    }

    /**
     * Set the connection timeout in milliseconds for the Zookeeper client.
     *
     * @param connectionTimeoutMs
     *            the connection timeout
     * @return this {@link ZkClientBuilder}
     */
    public ZkClientBuilder setConnectionTimeoutMs(int connectionTimeoutMs) {
        this.connectionTimeoutMs = connectionTimeoutMs;
        return this;
    }

    /**
     * Return the {@link RetryPolicyBuilder} for this {@link ZkClientBuilder}.
     *
     * @return the retry policy builder
     */
    public RetryPolicyBuilder<?> getRetryPolicyBuilder() {
        return retryPolicyBuilder;
    }

    /**
     * Set the {@link RetryPolicyBuilder} for this {@link ZkClientBuilder}.
     *
     * @param retryPolicyBuilder
     *            the retry policy builder
     * @return this {@link ZkClientBuilder}
     */
    public ZkClientBuilder setRetryPolicyBuilder(RetryPolicyBuilder<?> retryPolicyBuilder) {
        this.retryPolicyBuilder = retryPolicyBuilder;
        return this;
    }

    /**
     * Return a new {@link CuratorFrameworkFactory.Builder} that reflects the configuration of this {@link ZkClientBuilder}.
     *
     * @return the new builder
     * @throws IllegalArgumentException
     *             if the connectString is null or blank
     * @throws NullPointerException
     *             if the retryPolicyBuilder is null
     */
    public CuratorFrameworkFactory.Builder createFactoryBuilder() {
        Preconditions.checkArgument(connectString != null && !connectString.isBlank(), "connectString must not be null or blank");
        Preconditions.checkNotNull(retryPolicyBuilder, "retryPolicyBuilder must not be null");
        // @formatter:off
        return CuratorFrameworkFactory.builder()
                        .connectString(ZkUtils.getConnectString(connectString))
                        .namespace(namespace)
                        .sessionTimeoutMs(sessionTimeoutMs)
                        .connectionTimeoutMs(connectionTimeoutMs)
                        .retryPolicy(retryPolicyBuilder.build());
        // @formatter:on
    }

    /**
     * Return a new, non-started {@link CuratorFramework} client.
     *
     * @return the new client
     * @throws Exception
     *             if an error occurs while creating the builder for the client
     */
    public CuratorFramework build() throws Exception {
        return createFactoryBuilder().build();
    }

    /**
     * Return a new, started {@link CuratorFramework} client. The execution of the current thread will be halted until the connection to Zookeeper is fully
     * established, or until maxWaitTime has been exceeded.
     *
     * @param maxWaitTime
     *            the max wait time
     * @param timeUnit
     *            the time unit for the max wait time
     * @return the started client
     * @throws TimeoutException
     *             if the client fails to connect to Zookeeper within the wait period
     * @throws Exception
     *             if an error occurs while creating the builder for the client, or while waiting for the client to connect
     * @throws IllegalArgumentException
     *             if maxWaitTime is less than 1
     * @throws NullPointerException
     *             if timeUnit is null
     */
    public CuratorFramework buildAndStart(int maxWaitTime, TimeUnit timeUnit) throws Exception {
        Preconditions.checkArgument(maxWaitTime > 0, "maxWaitTime must be greater than 0");
        Preconditions.checkNotNull(timeUnit, "timeUnit must not be null");
        CuratorFramework client = build();
        try {
            client.start();
            boolean connected = client.blockUntilConnected(maxWaitTime, timeUnit);
            if (!connected) {
                throw new TimeoutException("Failed to connect to zookeeper within timeout of " + maxWaitTime + " " + timeUnit);
            }
            return client;
        } catch (Exception e) {
            // If an exception occurs after starting the client, ensure any connection to Zookeeper is closed.
            client.close();
            throw e;
        }

    }

    /**
     * Return a duplicate of this {@link ZkClientBuilder}.
     *
     * @return the duplicate
     */
    public ZkClientBuilder duplicate() {
        return new ZkClientBuilder(this);
    }

    @Override
    public boolean equals(Object object) {
        if (object == null || getClass() != object.getClass())
            return false;
        ZkClientBuilder that = (ZkClientBuilder) object;
        return sessionTimeoutMs == that.sessionTimeoutMs && connectionTimeoutMs == that.connectionTimeoutMs && Objects.equals(connectString, that.connectString)
                        && Objects.equals(namespace, that.namespace) && Objects.equals(retryPolicyBuilder, that.retryPolicyBuilder);
    }

    @Override
    public int hashCode() {
        return Objects.hash(connectString, namespace, sessionTimeoutMs, connectionTimeoutMs, retryPolicyBuilder);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", ZkClientBuilder.class.getSimpleName() + "[", "]").add("connectString='" + connectString + "'")
                        .add("namespace='" + namespace + "'").add("sessionTimeoutMs=" + sessionTimeoutMs).add("connectionTimeoutMs=" + connectionTimeoutMs)
                        .add("retryPolicyBuilder=" + retryPolicyBuilder).toString();
    }
}
