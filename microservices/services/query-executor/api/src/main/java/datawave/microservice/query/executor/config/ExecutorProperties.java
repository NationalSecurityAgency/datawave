package datawave.microservice.query.executor.config;

import java.util.concurrent.TimeUnit;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.PositiveOrZero;

import org.springframework.validation.annotation.Validated;

@Validated
public class ExecutorProperties {
    
    // What pool this executor is handling
    private String pool = "default";
    // A multiplier on the page size used to determine how big the pool of generated results should be.
    private float availableResultsPageMultiplier = 2.5f;
    // The maximum number of queue tasks
    private int maxQueueSize = 400;
    // The core thread pool size
    private int coreThreads = 10;
    // The maximum thread pool size
    private int maxThreads = 40;
    // The keep alive time (how long to keep an idle thread alive if maxThreads > coreThreads)
    private long keepAliveMs = TimeUnit.MINUTES.toMillis(10);
    // The amount of time before invalidating the local QueryStatus object
    private long queryStatusExpirationMs = TimeUnit.MINUTES.toMillis(1);
    // The number of results from one results task in between which we flush the checkpoint
    private int checkpointFlushResults = 2;
    // The amount of time for one results task after which we flush the checkpoint
    private long checkpointFlushMs = TimeUnit.SECONDS.toMillis(1);
    
    @PositiveOrZero
    private long monitorTaskLease = TimeUnit.MILLISECONDS.toMillis(100);
    @NotNull
    private TimeUnit monitorTaskLeaseTimeUnit = TimeUnit.MILLISECONDS;
    
    // how often should executor status be logged regardless of whether there are status changes
    private long logStatusPeriodMs = TimeUnit.MINUTES.toMillis(10);
    // how often should executor status be logged when the status has changed
    private long logStatusWhenChangedMs = TimeUnit.MINUTES.toMillis(5);
    
    // The time after which we consider a task orphaned. Note that this must be greater than checkpointFlushMs
    // as that defines how ofter the task timestamp is updated.
    private long orphanThresholdMs = TimeUnit.MINUTES.toMillis(1);
    // The max number of orphaned tasks to check per monitor cycle
    private int maxOrphanedTasksToCheck = 100;
    
    private String queryMetricsUrlPrefix = null;
    
    private long healthCheckWaitTime = TimeUnit.SECONDS.toMillis(30);
    private TimeUnit healthCheckWaitTimeUnit = TimeUnit.MILLISECONDS;
    
    private long healthCheckLeaseTime = TimeUnit.SECONDS.toMillis(30);
    private TimeUnit healthCheckLeaseTimeUnit = TimeUnit.MILLISECONDS;
    
    public String getPool() {
        return pool;
    }
    
    public void setPool(String pool) {
        this.pool = pool;
    }
    
    public int getMaxQueueSize() {
        return maxQueueSize;
    }
    
    public void setMaxQueueSize(int maxQueueSize) {
        this.maxQueueSize = maxQueueSize;
    }
    
    public int getCoreThreads() {
        return coreThreads;
    }
    
    public void setCoreThreads(int coreThreads) {
        this.coreThreads = coreThreads;
    }
    
    public int getMaxThreads() {
        return maxThreads;
    }
    
    public void setMaxThreads(int maxThreads) {
        this.maxThreads = maxThreads;
    }
    
    public long getKeepAliveMs() {
        return keepAliveMs;
    }
    
    public void setKeepAliveMs(long keepAliveMs) {
        this.keepAliveMs = keepAliveMs;
    }
    
    public float getAvailableResultsPageMultiplier() {
        return availableResultsPageMultiplier;
    }
    
    public void setAvailableResultsPageMultiplier(float availableResultsPageMultiplier) {
        this.availableResultsPageMultiplier = availableResultsPageMultiplier;
    }
    
    public long getQueryStatusExpirationMs() {
        return queryStatusExpirationMs;
    }
    
    public void setQueryStatusExpirationMs(long queryStatusExpirationMs) {
        this.queryStatusExpirationMs = queryStatusExpirationMs;
    }
    
    public long getMonitorTaskLease() {
        return monitorTaskLease;
    }
    
    public void setMonitorTaskLease(long monitorTaskLease) {
        this.monitorTaskLease = monitorTaskLease;
    }
    
    public long getMonitorTaskLeaseMillis() {
        return monitorTaskLeaseTimeUnit.toMillis(monitorTaskLease);
    }
    
    public TimeUnit getMonitorTaskLeaseTimeUnit() {
        return monitorTaskLeaseTimeUnit;
    }
    
    public void setMonitorTaskLeaseTimeUnit(TimeUnit monitorTaskLeaseTimeUnit) {
        this.monitorTaskLeaseTimeUnit = monitorTaskLeaseTimeUnit;
    }
    
    public int getCheckpointFlushResults() {
        return checkpointFlushResults;
    }
    
    public void setCheckpointFlushResults(int checkpointFlushResults) {
        this.checkpointFlushResults = checkpointFlushResults;
    }
    
    public long getCheckpointFlushMs() {
        return checkpointFlushMs;
    }
    
    public void setCheckpointFlushMs(long checkpointFlushMs) {
        this.checkpointFlushMs = checkpointFlushMs;
    }
    
    public long getOrphanThresholdMs() {
        return orphanThresholdMs;
    }
    
    public void setOrphanThresholdMs(long orphanThresholdMs) {
        this.orphanThresholdMs = orphanThresholdMs;
    }
    
    public int getMaxOrphanedTasksToCheck() {
        return maxOrphanedTasksToCheck;
    }
    
    public void setMaxOrphanedTasksToCheck(int maxOrphanedTasksToCheck) {
        this.maxOrphanedTasksToCheck = maxOrphanedTasksToCheck;
    }
    
    public long getLogStatusPeriodMs() {
        return logStatusPeriodMs;
    }
    
    public void setLogStatusPeriodMs(long logStatusPeriodMs) {
        this.logStatusPeriodMs = logStatusPeriodMs;
    }
    
    public long getLogStatusWhenChangedMs() {
        return logStatusWhenChangedMs;
    }
    
    public void setLogStatusWhenChangedMs(long logStatusWhenChangedMs) {
        this.logStatusWhenChangedMs = logStatusWhenChangedMs;
    }
    
    public String getQueryMetricsUrlPrefix() {
        return queryMetricsUrlPrefix;
    }
    
    public void setQueryMetricsUrlPrefix(String queryMetricsUrlPrefix) {
        this.queryMetricsUrlPrefix = queryMetricsUrlPrefix;
    }
    
    public long getHealthCheckWaitTime() {
        return healthCheckWaitTime;
    }
    
    public long getHealthCheckWaitTimeMillis() {
        return healthCheckWaitTimeUnit.toMillis(healthCheckWaitTime);
    }
    
    public void setHealthCheckWaitTime(long healthCheckWaitTime) {
        this.healthCheckWaitTime = healthCheckWaitTime;
    }
    
    public TimeUnit getHealthCheckWaitTimeUnit() {
        return healthCheckWaitTimeUnit;
    }
    
    public void setHealthCheckWaitTimeUnit(TimeUnit healthCheckWaitTimeUnit) {
        this.healthCheckWaitTimeUnit = healthCheckWaitTimeUnit;
    }
    
    public long getHealthCheckLeaseTime() {
        return healthCheckLeaseTime;
    }
    
    public long getHealthCheckLeaseTimeMillis() {
        return healthCheckLeaseTimeUnit.toMillis(healthCheckLeaseTime);
    }
    
    public void setHealthCheckLeaseTime(long healthCheckLeaseTime) {
        this.healthCheckLeaseTime = healthCheckLeaseTime;
    }
    
    public TimeUnit getHealthCheckLeaseTimeUnit() {
        return healthCheckLeaseTimeUnit;
    }
    
    public void setHealthCheckLeaseTimeUnit(TimeUnit healthCheckLeaseTimeUnit) {
        this.healthCheckLeaseTimeUnit = healthCheckLeaseTimeUnit;
    }
}
