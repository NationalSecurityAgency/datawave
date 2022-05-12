package datawave.microservice.query.monitor.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;
import javax.validation.constraints.PositiveOrZero;
import java.util.concurrent.TimeUnit;

@Validated
@ConfigurationProperties(prefix = "datawave.query.monitor")
public class MonitorProperties {
    private String schedulerCrontab;
    @PositiveOrZero
    private long monitorInterval = TimeUnit.MILLISECONDS.toMillis(30);
    @NotNull
    private TimeUnit monitorIntervalTimeUnit = TimeUnit.MILLISECONDS;
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
    private TimeUnit inactiveQueryTimeUnit = TimeUnit.DAYS;
    
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
        return monitorIntervalTimeUnit.toMillis(monitorInterval);
    }
    
    public void setMonitorInterval(long monitorInterval) {
        this.monitorInterval = monitorInterval;
    }
    
    public TimeUnit getMonitorIntervalTimeUnit() {
        return monitorIntervalTimeUnit;
    }
    
    public void setMonitorIntervalTimeUnit(TimeUnit monitorIntervalTimeUnit) {
        this.monitorIntervalTimeUnit = monitorIntervalTimeUnit;
    }
    
    public long getLockWaitTime() {
        return lockWaitTime;
    }
    
    public long getLockWaitTimeMillis() {
        return lockWaitTime;
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
        return inactiveQueryTimeUnit.toMillis(inactiveQueryTimeToLive);
    }
    
    public void setInactiveQueryTimeToLive(long inactiveQueryTimeToLive) {
        this.inactiveQueryTimeToLive = inactiveQueryTimeToLive;
    }
    
    public TimeUnit getInactiveQueryTimeUnit() {
        return inactiveQueryTimeUnit;
    }
    
    public void setInactiveQueryTimeUnit(TimeUnit inactiveQueryTimeUnit) {
        this.inactiveQueryTimeUnit = inactiveQueryTimeUnit;
    }
}
