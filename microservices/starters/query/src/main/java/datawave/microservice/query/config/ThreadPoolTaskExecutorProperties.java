package datawave.microservice.query.config;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;
import javax.validation.constraints.PositiveOrZero;

import org.springframework.validation.annotation.Validated;

@Validated
public class ThreadPoolTaskExecutorProperties {
    @PositiveOrZero
    private int corePoolSize = 0;
    @Positive
    private int maxPoolSize = 5;
    @PositiveOrZero
    private int queueCapacity = 0;
    @NotNull
    private String threadNamePrefix = "";
    
    public ThreadPoolTaskExecutorProperties() {
        
    }
    
    public ThreadPoolTaskExecutorProperties(int corePoolSize, int maxPoolSize, int queueCapacity, String threadNamePrefix) {
        this.corePoolSize = corePoolSize;
        this.maxPoolSize = maxPoolSize;
        this.queueCapacity = queueCapacity;
        this.threadNamePrefix = threadNamePrefix;
    }
    
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
