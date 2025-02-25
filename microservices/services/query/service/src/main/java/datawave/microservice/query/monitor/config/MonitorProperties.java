package datawave.microservice.query.monitor.config;

import java.util.concurrent.TimeUnit;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;
import javax.validation.constraints.PositiveOrZero;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "datawave.query.monitor")
public class MonitorProperties {
    private String schedulerCrontab;
    @PositiveOrZero
    private long monitorInterval = TimeUnit.MILLISECONDS.toMillis(30);
    @NotNull
    private TimeUnit monitorIntervalUnit = TimeUnit.MILLISECONDS;
    // The amount of time to wait for the monitor lock to be acquired
    @PositiveOrZero
    private long lockWaitTime = 0;
    @NotNull
    private TimeUnit lockWaitTimeUnit = TimeUnit.MILLISECONDS;
    // The amount of time that the monitor lock will be held before being automatically released
    @Positive
    private long lockLeaseTime = TimeUnit.MINUTES.toMillis(1);
    @NotNull
    private TimeUnit lockLeaseTimeUnit = TimeUnit.MILLISECONDS;
    // The amount of time that an inactive query should remain in the query cache
    @PositiveOrZero
    private long inactiveQueryTimeToLive = 1;
    @NotNull
    private TimeUnit inactiveQueryTimeToLiveUnit = TimeUnit.DAYS;
    // The amount of time to wait for the executor status cache lock
    @PositiveOrZero
    private long executorStatusLockWaitTime = 30;
    @NotNull
    private TimeUnit executorStatusLockWaitTimeUnit = TimeUnit.SECONDS;
    // The amount of time that the executor status cache lock will be held before being automatically released
    @Positive
    private long executorStatusLockLeaseTime = 30;
    @NotNull
    private TimeUnit executorStatusLockLeaseTimeUnit = TimeUnit.SECONDS;
    
    public String getSchedulerCrontab() {
        return schedulerCrontab;
    }
    
    public void setSchedulerCrontab(String schedulerCrontab) {
        this.schedulerCrontab = schedulerCrontab;
    }
    
    public long getMonitorInterval() {
        return monitorInterval;
    }
    
    public long getMonitorIntervalMillis() {
        return monitorIntervalUnit.toMillis(monitorInterval);
    }
    
    public void setMonitorInterval(long monitorInterval) {
        this.monitorInterval = monitorInterval;
    }
    
    public TimeUnit getMonitorIntervalUnit() {
        return monitorIntervalUnit;
    }
    
    public void setMonitorIntervalUnit(TimeUnit monitorIntervalUnit) {
        this.monitorIntervalUnit = monitorIntervalUnit;
    }
    
    public long getLockWaitTime() {
        return lockWaitTime;
    }
    
    public long getLockWaitTimeMillis() {
        return lockWaitTimeUnit.toMillis(lockWaitTime);
    }
    
    public void setLockWaitTime(long lockWaitTime) {
        this.lockWaitTime = lockWaitTime;
    }
    
    public TimeUnit getLockWaitTimeUnit() {
        return lockWaitTimeUnit;
    }
    
    public void setLockWaitTimeUnit(TimeUnit lockWaitTimeUnit) {
        this.lockWaitTimeUnit = lockWaitTimeUnit;
    }
    
    public long getLockLeaseTime() {
        return lockLeaseTime;
    }
    
    public long getLockLeaseTimeMillis() {
        return lockLeaseTimeUnit.toMillis(lockLeaseTime);
    }
    
    public void setLockLeaseTime(long lockLeaseTime) {
        this.lockLeaseTime = lockLeaseTime;
    }
    
    public TimeUnit getLockLeaseTimeUnit() {
        return lockLeaseTimeUnit;
    }
    
    public void setLockLeaseTimeUnit(TimeUnit lockLeaseTimeUnit) {
        this.lockLeaseTimeUnit = lockLeaseTimeUnit;
    }
    
    public long getInactiveQueryTimeToLive() {
        return inactiveQueryTimeToLive;
    }
    
    public long getInactiveQueryTimeToLiveMillis() {
        return inactiveQueryTimeToLiveUnit.toMillis(inactiveQueryTimeToLive);
    }
    
    public void setInactiveQueryTimeToLive(long inactiveQueryTimeToLive) {
        this.inactiveQueryTimeToLive = inactiveQueryTimeToLive;
    }
    
    public TimeUnit getInactiveQueryTimeToLiveUnit() {
        return inactiveQueryTimeToLiveUnit;
    }
    
    public void setInactiveQueryTimeToLiveUnit(TimeUnit inactiveQueryTimeToLiveUnit) {
        this.inactiveQueryTimeToLiveUnit = inactiveQueryTimeToLiveUnit;
    }
    
    public long getExecutorStatusLockWaitTime() {
        return executorStatusLockWaitTime;
    }
    
    public long getExecutorStatusLockWaitTimeMillis() {
        return executorStatusLockWaitTimeUnit.toMillis(executorStatusLockWaitTime);
    }
    
    public void setExecutorStatusLockWaitTime(long executorStatusLockWaitTime) {
        this.executorStatusLockWaitTime = executorStatusLockWaitTime;
    }
    
    public TimeUnit getExecutorStatusLockWaitTimeUnit() {
        return executorStatusLockWaitTimeUnit;
    }
    
    public void setExecutorStatusLockWaitTimeUnit(TimeUnit executorStatusLockWaitTimeUnit) {
        this.executorStatusLockWaitTimeUnit = executorStatusLockWaitTimeUnit;
    }
    
    public long getExecutorStatusLockLeaseTime() {
        return executorStatusLockLeaseTime;
    }
    
    public long getExecutorStatusLockLeaseTimeMillis() {
        return executorStatusLockLeaseTimeUnit.toMillis(executorStatusLockLeaseTime);
    }
    
    public void setExecutorStatusLockLeaseTime(long executorStatusLockLeaseTime) {
        this.executorStatusLockLeaseTime = executorStatusLockLeaseTime;
    }
    
    public TimeUnit getExecutorStatusLockLeaseTimeUnit() {
        return executorStatusLockLeaseTimeUnit;
    }
    
    public void setExecutorStatusLockLeaseTimeUnit(TimeUnit executorStatusLockLeaseTimeUnit) {
        this.executorStatusLockLeaseTimeUnit = executorStatusLockLeaseTimeUnit;
    }
}
