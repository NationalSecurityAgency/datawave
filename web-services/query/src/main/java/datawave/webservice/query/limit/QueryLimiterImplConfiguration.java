package datawave.webservice.query.limit;

import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;

import datawave.zookeeper.ZkClientBuilder;

/**
 * Represents a configuration for a {@link QueryLimiterImpl}.
 */
public class QueryLimiterImplConfiguration {

    private ZkClientBuilder zkClientBuilder;

    private QueryLimitConfiguration limitConfiguration;

    private long heartbeatCleanupInterval = 10;

    private TimeUnit heartbeatCleanupTimeUnit = TimeUnit.MINUTES;

    private int zkClientConnectTimeout = 3;

    private TimeUnit zkClientConnectTimeoutUnit = TimeUnit.MINUTES;

    public QueryLimiterImplConfiguration() {}

    public QueryLimiterImplConfiguration(QueryLimiterImplConfiguration other) {
        this.zkClientBuilder = other.zkClientBuilder.duplicate();
        this.limitConfiguration = other.limitConfiguration == null ? null : other.limitConfiguration.deepCopy();
        this.heartbeatCleanupInterval = other.heartbeatCleanupInterval;
        this.heartbeatCleanupTimeUnit = other.heartbeatCleanupTimeUnit;
        this.zkClientConnectTimeout = other.zkClientConnectTimeout;
        this.zkClientConnectTimeoutUnit = other.zkClientConnectTimeoutUnit;
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

    public TimeUnit getHeartbeatCleanupTimeUnit() {
        return heartbeatCleanupTimeUnit;
    }

    public void setHeartbeatCleanupTimeUnit(TimeUnit heartbeatCleanupTimeUnit) {
        this.heartbeatCleanupTimeUnit = heartbeatCleanupTimeUnit;
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

    /**
     * Return a deep copy of this {@link QueryLimitConfiguration}
     *
     * @return the deep copy
     */
    public QueryLimiterImplConfiguration deepCopy() {
        return new QueryLimiterImplConfiguration(this);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        QueryLimiterImplConfiguration that = (QueryLimiterImplConfiguration) o;
        return heartbeatCleanupInterval == that.heartbeatCleanupInterval && zkClientConnectTimeout == that.zkClientConnectTimeout
                        && Objects.equals(zkClientBuilder, that.zkClientBuilder) && Objects.equals(limitConfiguration, that.limitConfiguration)
                        && heartbeatCleanupTimeUnit == that.heartbeatCleanupTimeUnit && zkClientConnectTimeoutUnit == that.zkClientConnectTimeoutUnit;
    }

    @Override
    public int hashCode() {
        return Objects.hash(zkClientBuilder, limitConfiguration, heartbeatCleanupInterval, heartbeatCleanupTimeUnit, zkClientConnectTimeout,
                        zkClientConnectTimeoutUnit);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", QueryLimiterImplConfiguration.class.getSimpleName() + "[", "]").add("zkClientBuilder=" + zkClientBuilder)
                        .add("limitConfiguration=" + limitConfiguration).add("heartbeatCleanupInterval=" + heartbeatCleanupInterval)
                        .add("heartbeatCleanupTimeUnit=" + heartbeatCleanupTimeUnit).add("zkClientConnectTimeout=" + zkClientConnectTimeout)
                        .add("zkClientConnectTimeoutUnit=" + zkClientConnectTimeoutUnit).toString();
    }
}
