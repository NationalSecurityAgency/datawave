package datawave.microservice.query.executor.config;

import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.PositiveOrZero;
import java.util.concurrent.TimeUnit;

@Validated
public class ExecutorProperties {
    
    // Should we use the query status to track the number of generated results or poll the underlying queue size
    private boolean pollQueueSize = true;
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
    // The amount of time to wait for the lock to be acquired
    @PositiveOrZero
    private long lockWaitTimeMillis = TimeUnit.SECONDS.toMillis(5);
    // The amount of time that the lock will be held before being automatically released
    @PositiveOrZero
    private long lockLeaseTimeMillis = TimeUnit.SECONDS.toMillis(30);
    
    public boolean isPollQueueSize() {
        return pollQueueSize;
    }
    
    public void setPollQueueSize(boolean pollQueueSize) {
        this.pollQueueSize = pollQueueSize;
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
}
