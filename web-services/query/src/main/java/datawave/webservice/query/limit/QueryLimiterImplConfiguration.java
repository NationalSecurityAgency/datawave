package datawave.webservice.query.limit;

import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;

import datawave.core.common.zookeeper.ZkClientBuilder;

/**
 * Represents a configuration for a {@link QueryLimiterImpl}.
 */
public class QueryLimiterImplConfiguration {
    
    private QueryLimiterMode mode = QueryLimiterMode.DISABLED;
    
    private ZkClientBuilder zkClientBuilder;

    private QueryLimitConfiguration limitConfiguration;

    private long heartbeatCleanupInterval = 10;

    private TimeUnit heartbeatCleanupIntervalUnit = TimeUnit.MINUTES;

    private int zkClientConnectTimeout = 3;

    private TimeUnit zkClientConnectTimeoutUnit = TimeUnit.MINUTES;

    public QueryLimiterImplConfiguration() {}
    
    /**
     * Return a deep copy of the given configuration.
     * @param other the instance to copy
     */
    public QueryLimiterImplConfiguration(QueryLimiterImplConfiguration other) {
        this.mode = other.mode;
        this.zkClientBuilder = other.zkClientBuilder == null ? null : other.zkClientBuilder.duplicate();
        this.limitConfiguration = other.limitConfiguration == null ? null : other.limitConfiguration.deepCopy();
        this.heartbeatCleanupInterval = other.heartbeatCleanupInterval;
        this.heartbeatCleanupIntervalUnit = other.heartbeatCleanupIntervalUnit;
        this.zkClientConnectTimeout = other.zkClientConnectTimeout;
        this.zkClientConnectTimeoutUnit = other.zkClientConnectTimeoutUnit;
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

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        QueryLimiterImplConfiguration that = (QueryLimiterImplConfiguration) o;
        return heartbeatCleanupInterval == that.heartbeatCleanupInterval && zkClientConnectTimeout == that.zkClientConnectTimeout && mode == that.mode
                        && Objects.equals(zkClientBuilder, that.zkClientBuilder) && Objects.equals(limitConfiguration, that.limitConfiguration)
                        && heartbeatCleanupIntervalUnit == that.heartbeatCleanupIntervalUnit && zkClientConnectTimeoutUnit == that.zkClientConnectTimeoutUnit;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(mode, zkClientBuilder, limitConfiguration, heartbeatCleanupInterval, heartbeatCleanupIntervalUnit, zkClientConnectTimeout,
                        zkClientConnectTimeoutUnit);
    }
    
    @Override
    public String toString() {
        return new StringJoiner(", ", QueryLimiterImplConfiguration.class.getSimpleName() + "[", "]").add("mode=" + mode)
                        .add("zkClientBuilder=" + zkClientBuilder).add("limitConfiguration=" + limitConfiguration)
                        .add("heartbeatCleanupInterval=" + heartbeatCleanupInterval).add("heartbeatCleanupIntervalUnit=" + heartbeatCleanupIntervalUnit)
                        .add("zkClientConnectTimeout=" + zkClientConnectTimeout).add("zkClientConnectTimeoutUnit=" + zkClientConnectTimeoutUnit).toString();
    }
}
