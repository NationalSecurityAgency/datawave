package datawave.microservice.query.executor.config;

import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.PositiveOrZero;
import java.util.concurrent.TimeUnit;

@Validated
public class ExecutorProperties {
    
    // What pool this executor is handling
    private String pool = "default";
    // A multiplier on the page size use to determine how but the pool of generated results should be.
    private float availableResultsPageMultiplier = 2.5f;
    // The maximum age of the query status object
    private long maxQueryStatusAge = 1000L;
    // The maximum number of queue tasks
    private int maxQueueSize = 400;
    // The core thread pool size
    private int coreThreads = 10;
    // The maximum thread pool size
    private int maxThreads = 40;
    // The keep alive time (how long to keep an idle thread alive if maxThreads > coreThreads)
    private long keepAliveMs = 10 * 60 * 1000;
    // The amount of time before invalidating the local QueryStatus object
    private long queryStatusExpirationMs = 60 * 1000;
    @PositiveOrZero
    private long monitorTaskLease = TimeUnit.MILLISECONDS.toMillis(100);
    @NotNull
    private TimeUnit monitorTaskLeaseTimeUnit = TimeUnit.MILLISECONDS;
    // the max cache size used by the monitor to avoid excessive close/cancel task executions
    private int monitorMaxCacheSize = 500;
    // The amount of time to wait for the lock to be acquired
    @PositiveOrZero
    private long lockWaitTimeMillis = TimeUnit.SECONDS.toMillis(5);
    // The amount of time that the lock will be held before being automatically released
    @PositiveOrZero
    private long lockLeaseTimeMillis = TimeUnit.SECONDS.toMillis(30);
    
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
    
    public long getMaxQueryStatusAge() {
        return maxQueryStatusAge;
    }
    
    public void setMaxQueryStatusAge(long maxQueryStatusAge) {
        this.maxQueryStatusAge = maxQueryStatusAge;
    }
    
    public long getQueryStatusExpirationMs() {
        return queryStatusExpirationMs;
    }
    
    public void setQueryStatusExpirationMs(long queryStatusExpirationMs) {
        this.queryStatusExpirationMs = queryStatusExpirationMs;
    }
    
    public long getLockWaitTimeMillis() {
        return lockWaitTimeMillis;
    }
    
    public void setLockWaitTimeMillis(long lockWaitTimeMillis) {
        this.lockWaitTimeMillis = lockWaitTimeMillis;
    }
    
    public long getLockLeaseTimeMillis() {
        return lockLeaseTimeMillis;
    }
    
    public void setLockLeaseTimeMillis(long lockLeaseTimeMillis) {
        this.lockLeaseTimeMillis = lockLeaseTimeMillis;
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
    
    public int getMonitorMaxCacheSize() {
        return monitorMaxCacheSize;
    }
    
    public void setMonitorMaxCacheSize(int monitorMaxCacheSize) {
        this.monitorMaxCacheSize = monitorMaxCacheSize;
    }
}
