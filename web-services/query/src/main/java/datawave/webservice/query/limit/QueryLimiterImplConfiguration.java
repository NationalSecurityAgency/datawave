package datawave.webservice.query.limit;

import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;

import datawave.core.common.zookeeper.ZkClientBuilder;

/**
 * Represents a configuration for a {@link QueryLimiterImpl}.
 */
public class QueryLimiterImplConfiguration {

    /**
     * The mode that the limiter will start up in.
     */
    private QueryLimiterMode mode = QueryLimiterMode.DISABLED;

    /**
     * The configuration for creating the Zookeeper client.
     */
    private ZkClientBuilder zkClientBuilder;

    /**
     * The limits to enforce.
     */
    private QueryLimitConfiguration limitConfiguration;

    /**
     * The interval at which the heartbeat cache will periodically be cleaned up of stopped heartbeats.
     */
    private long heartbeatCleanupInterval = 10;

    /**
     * The time unit for {@link #heartbeatCleanupInterval}.
     */
    private TimeUnit heartbeatCleanupIntervalUnit = TimeUnit.MINUTES;

    /**
     * The timeout for establishing a connection to Zookeeper.
     */
    private int zkClientConnectTimeout = 3;

    /**
     * The time unit for {@link #zkClientConnectTimeout}.
     */
    private TimeUnit zkClientConnectTimeoutUnit = TimeUnit.MINUTES;

    /**
     * The timeout for the creation of new ephemeral nodes.
     */
    private int nodeCreationTimeout = 1;

    /**
     * The time unit for {@link #nodeCreationTimeout}.
     */
    private TimeUnit nodeCreationTimeoutUnit = TimeUnit.SECONDS;

    /**
     * Default constructor.
     */
    public QueryLimiterImplConfiguration() {}

    /**
     * Return a deep copy of the given configuration.
     *
     * @param other
     *            the instance to copy
     */
    public QueryLimiterImplConfiguration(QueryLimiterImplConfiguration other) {
        this.mode = other.mode;
        this.zkClientBuilder = other.zkClientBuilder == null ? null : other.zkClientBuilder.duplicate();
        this.limitConfiguration = other.limitConfiguration == null ? null : other.limitConfiguration.deepCopy();
        this.heartbeatCleanupInterval = other.heartbeatCleanupInterval;
        this.heartbeatCleanupIntervalUnit = other.heartbeatCleanupIntervalUnit;
        this.zkClientConnectTimeout = other.zkClientConnectTimeout;
        this.zkClientConnectTimeoutUnit = other.zkClientConnectTimeoutUnit;
        this.nodeCreationTimeout = other.nodeCreationTimeout;
        this.nodeCreationTimeoutUnit = other.nodeCreationTimeoutUnit;
    }

    public QueryLimiterMode getMode() {
        return mode;
    }

    public void setMode(QueryLimiterMode mode) {
        this.mode = mode;
    }

    public ZkClientBuilder getZkClientBuilder() {
        return zkClientBuilder;
    }

    public void setZkClientBuilder(ZkClientBuilder zkClientBuilder) {
        this.zkClientBuilder = zkClientBuilder;
    }

    public QueryLimitConfiguration getLimitConfiguration() {
        return limitConfiguration;
    }

    public void setLimitConfiguration(QueryLimitConfiguration limitConfiguration) {
        this.limitConfiguration = limitConfiguration;
    }

    public long getHeartbeatCleanupInterval() {
        return heartbeatCleanupInterval;
    }

    public void setHeartbeatCleanupInterval(long heartbeatCleanupInterval) {
        this.heartbeatCleanupInterval = heartbeatCleanupInterval;
    }

    public TimeUnit getHeartbeatCleanupIntervalUnit() {
        return heartbeatCleanupIntervalUnit;
    }

    public void setHeartbeatCleanupIntervalUnit(TimeUnit heartbeatCleanupIntervalUnit) {
        this.heartbeatCleanupIntervalUnit = heartbeatCleanupIntervalUnit;
    }

    public int getZkClientConnectTimeout() {
        return zkClientConnectTimeout;
    }

    public void setZkClientConnectTimeout(int zkClientConnectTimeout) {
        this.zkClientConnectTimeout = zkClientConnectTimeout;
    }

    public TimeUnit getZkClientConnectTimeoutUnit() {
        return zkClientConnectTimeoutUnit;
    }

    public void setZkClientConnectTimeoutUnit(TimeUnit zkClientConnectTimeoutUnit) {
        this.zkClientConnectTimeoutUnit = zkClientConnectTimeoutUnit;
    }

    public int getNodeCreationTimeout() {
        return nodeCreationTimeout;
    }

    public void setNodeCreationTimeout(int nodeCreationTimeout) {
        this.nodeCreationTimeout = nodeCreationTimeout;
    }

    public TimeUnit getNodeCreationTimeoutUnit() {
        return nodeCreationTimeoutUnit;
    }

    public void setNodeCreationTimeoutUnit(TimeUnit nodeCreationTimeoutUnit) {
        this.nodeCreationTimeoutUnit = nodeCreationTimeoutUnit;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        QueryLimiterImplConfiguration that = (QueryLimiterImplConfiguration) o;
        return heartbeatCleanupInterval == that.heartbeatCleanupInterval && zkClientConnectTimeout == that.zkClientConnectTimeout
                        && nodeCreationTimeout == that.nodeCreationTimeout && mode == that.mode && Objects.equals(zkClientBuilder, that.zkClientBuilder)
                        && Objects.equals(limitConfiguration, that.limitConfiguration) && heartbeatCleanupIntervalUnit == that.heartbeatCleanupIntervalUnit
                        && zkClientConnectTimeoutUnit == that.zkClientConnectTimeoutUnit && nodeCreationTimeoutUnit == that.nodeCreationTimeoutUnit;
    }

    @Override
    public int hashCode() {
        return Objects.hash(mode, zkClientBuilder, limitConfiguration, heartbeatCleanupInterval, heartbeatCleanupIntervalUnit, zkClientConnectTimeout,
                        zkClientConnectTimeoutUnit, nodeCreationTimeout, nodeCreationTimeoutUnit);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", QueryLimiterImplConfiguration.class.getSimpleName() + "[", "]").add("mode=" + mode)
                        .add("zkClientBuilder=" + zkClientBuilder).add("limitConfiguration=" + limitConfiguration)
                        .add("heartbeatCleanupInterval=" + heartbeatCleanupInterval).add("heartbeatCleanupIntervalUnit=" + heartbeatCleanupIntervalUnit)
                        .add("zkClientConnectTimeout=" + zkClientConnectTimeout).add("zkClientConnectTimeoutUnit=" + zkClientConnectTimeoutUnit)
                        .add("nodeCreationTimeout=" + nodeCreationTimeout).add("nodeCreationTimeoutUnit=" + nodeCreationTimeoutUnit).toString();
    }
}
