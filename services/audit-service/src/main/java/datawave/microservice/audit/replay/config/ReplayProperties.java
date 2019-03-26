package datawave.microservice.audit.replay.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;
import javax.validation.constraints.PositiveOrZero;
import java.util.concurrent.TimeUnit;

@Validated
@ConfigurationProperties(prefix = "audit.replay")
public class ReplayProperties {
    private boolean enabled;
    
    // Whether events should be published to the event bus for other audit service instances to act on
    private boolean publishEvents = true;
    
    // The amount of time before an inactive audit replay is considered to be idle
    @PositiveOrZero
    private long idleTimeoutMillis = TimeUnit.SECONDS.toMillis(10);
    
    // How long to wait before forcefully stopping an audit replay
    @PositiveOrZero
    private long stopGracePeriodMillis = 500L;
    
    // How often the status should be updated during an audit replay
    @PositiveOrZero
    private long statusUpdateIntervalMillis = TimeUnit.SECONDS.toMillis(1);
    
    // The amount of time to wait for the lock to be acquired
    @PositiveOrZero
    private long lockWaitTimeMillis = TimeUnit.SECONDS.toMillis(5);
    
    // The amount of time that the lock will be held before being automatically released
    @PositiveOrZero
    private long lockLeaseTimeMillis = TimeUnit.SECONDS.toMillis(5);
    
    @Valid
    private ExecutorProperties executor = new ExecutorProperties();
    
    public boolean isEnabled() {
        return enabled;
    }
    
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }
    
    public boolean isPublishEvents() {
        return publishEvents;
    }
    
    public void setPublishEvents(boolean publishEvents) {
        this.publishEvents = publishEvents;
    }
    
    public long getIdleTimeoutMillis() {
        return idleTimeoutMillis;
    }
    
    public void setIdleTimeoutMillis(long idleTimeoutMillis) {
        this.idleTimeoutMillis = idleTimeoutMillis;
    }
    
    public long getStopGracePeriodMillis() {
        return stopGracePeriodMillis;
    }
    
    public void setStopGracePeriodMillis(long stopGracePeriodMillis) {
        this.stopGracePeriodMillis = stopGracePeriodMillis;
    }
    
    public long getStatusUpdateIntervalMillis() {
        return statusUpdateIntervalMillis;
    }
    
    public void setStatusUpdateIntervalMillis(long statusUpdateIntervalMillis) {
        this.statusUpdateIntervalMillis = statusUpdateIntervalMillis;
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
    
    public ExecutorProperties getExecutor() {
        return executor;
    }
    
    public void setExecutor(ExecutorProperties executor) {
        this.executor = executor;
    }
    
    @Validated
    public static class ExecutorProperties {
        @PositiveOrZero
        private int corePoolSize = 0;
        
        @Positive
        private int maxPoolSize = 5;
        
        @PositiveOrZero
        private int queueCapacity = 0;
        
        @NotNull
        private String threadNamePrefix = "replayTask-";
        
        public int getCorePoolSize() {
            return corePoolSize;
        }
        
        public void setCorePoolSize(int corePoolSize) {
            this.corePoolSize = corePoolSize;
        }
        
        public int getMaxPoolSize() {
            return maxPoolSize;
        }
        
        public void setMaxPoolSize(int maxPoolSize) {
            this.maxPoolSize = maxPoolSize;
        }
        
        public int getQueueCapacity() {
            return queueCapacity;
        }
        
        public void setQueueCapacity(int queueCapacity) {
            this.queueCapacity = queueCapacity;
        }
        
        public String getThreadNamePrefix() {
            return threadNamePrefix;
        }
        
        public void setThreadNamePrefix(String threadNamePrefix) {
            this.threadNamePrefix = threadNamePrefix;
        }
    }
}
