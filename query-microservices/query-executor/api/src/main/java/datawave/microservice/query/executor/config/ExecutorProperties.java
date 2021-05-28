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
