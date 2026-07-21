package datawave.zookeeper;

import static datawave.zookeeper.ZkUtils.getQuorumPeerConfig;

import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;

/**
 * A configurable Zookeeper client builder.
 */
public class ZkClientBuilder {

    /**
     * The string for connecting to zookeeper. This can either be a list of servers or a path to a zookeeper config file.
     */
    private String connectString;

    /**
     * The namespace for the Zookeeper client. Defaults to {@code null.}
     */
    private String namespace;

    /**
     * The session timeout in milliseconds for the Zookeeper client. Defaults to 1 minute.
     */
    private int sessionTimeoutMs = 60 * 1000;

    /**
     * The connection timeout in milliseconds for the Zookeeper client. Defaults to 1 minute.
     */
    private int connectionTimeoutMs = 60 * 1000;

    /**
     * The builder for creating the retry policy of the Zookeeper client. Defaults to a {@link RetryNTimesBuilder}.
     */
    private RetryPolicyBuilder retryPolicyBuilder = new RetryNTimesBuilder();

    /**
     * Return the string for connecting to Zookeeper. This may either be a list of servers or a path to a zookeeper config file.
     *
     * @return the connection string
     */
    public String getConnectString() {
        return connectString;
    }

    /**
     * Set the string for connecting to Zookeeper. This can either be a list of servers or a path to a zookeeper config file.
     */
    public void setConnectString(String connectString) {
        this.connectString = connectString;
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
     */
    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    /**
     * Return the session timeout in milliseconds for the Zookeeper client.
     *
     * @return the timeout
     */
    public int getSessionTimeoutMs() {
        return sessionTimeoutMs;
    }

    /**
     * Set the session timeout in milliseconds for the Zookeeper client.
     *
     * @param sessionTimeoutMs
     *            the timeout
     */
    public void setSessionTimeoutMs(int sessionTimeoutMs) {
        this.sessionTimeoutMs = sessionTimeoutMs;
    }

    /**
     * Return the connection timeout in milliseconds for the Zookeeper client.
     *
     * @return the timeout
     */
    public int getConnectionTimeoutMs() {
        return connectionTimeoutMs;
    }

    /**
     * Set the connection timeout in milliseconds for the Zookeeper client.
     *
     * @param connectionTimeoutMs
     *            the timeout
     */
    public void setConnectionTimeoutMs(int connectionTimeoutMs) {
        this.connectionTimeoutMs = connectionTimeoutMs;
    }

    /**
     * Return the retry policy builder
     *
     * @return the policy builder
     */
    public RetryPolicyBuilder getRetryPolicyBuilder() {
        return retryPolicyBuilder;
    }

    /**
     * Set the retry policy builder
     *
     * @param retryPolicyBuilder
     *            the policy builder
     */
    public void setRetryPolicyBuilder(RetryPolicyBuilder retryPolicyBuilder) {
        this.retryPolicyBuilder = retryPolicyBuilder;
    }

    /**
     * Return a new {@link CuratorFrameworkFactory.Builder} that reflects the configuration of this {@link ZkClientBuilder}.
     *
     * @return the new builder
     * @throws Exception
     *             if an error occurs while creating the builder
     */
    public CuratorFrameworkFactory.Builder createBuilder() throws Exception {
        // @formatter:off
        return CuratorFrameworkFactory.builder()
                .connectString(getQuorumPeerConfig(connectString))
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
        return createBuilder().build();
    }

    /**
     * Return a new {@link CuratorFramework} client. If startClient is true, the client will be started, and the execution of the current thread will be halted
     * until the connection to Zookeeper is fully established, or until maxWaitTime has been exceeded.
     * <ul>
     * <li>If maxWaitTime is less than one, and timeUnit is not null, the thread will not be halted.</li>
     * <li>If maxWaitTime is less than one, and timeUnit is null, the thread will be halted indefinitely until the connection is established.</li>
     * </ul>
     *
     * @param maxWaitTime
     *            the max wait time
     * @param timeUnit
     *            the time unit for the max wait time
     * @return the started client
     * @throws Exception
     *             if an error occurs while creating the builder for the client, or while waiting for the client to connect
     */
    public CuratorFramework build(boolean startClient, int maxWaitTime, TimeUnit timeUnit) throws Exception {
        CuratorFramework client = build();
        if (startClient) {
            client.start();
            client.blockUntilConnected(maxWaitTime, timeUnit);
        }
        return client;
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
        // @formatter:off
        return new StringJoiner(", ", ZkClientBuilder.class.getSimpleName() + "[", "]")
                .add("connectString='" + connectString + "'")
                .add("namespace='" + namespace + "'")
                .add("sessionTimeoutMs=" + sessionTimeoutMs)
                .add("connectionTimeoutMs=" + connectionTimeoutMs)
                .add("retryPolicyBuilder=" + retryPolicyBuilder)
                .toString();
        // @formatter:on
    }
}
