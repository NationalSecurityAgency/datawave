package datawave.microservice.query.cachedresults.monitor.config;

import java.util.concurrent.TimeUnit;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;
import javax.validation.constraints.PositiveOrZero;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "datawave.query.cached-results.monitor")
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
}
