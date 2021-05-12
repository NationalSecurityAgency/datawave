package datawave.microservice.query.config;

import org.springframework.validation.annotation.Validated;

import javax.annotation.Nonnegative;
import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;
import javax.validation.constraints.PositiveOrZero;
import java.util.concurrent.TimeUnit;

@Validated
public class QueryProperties {
    
    @Valid
    private QueryExpirationProperties expiration;
    @NotEmpty
    private String privilegedRole = "PrivilegedUser";
    @Positive
    private long resultQueueIntervalMillis = TimeUnit.SECONDS.toMillis(60);
    // The amount of time to wait for the lock to be acquired
    @PositiveOrZero
    private long lockWaitTimeMillis = TimeUnit.SECONDS.toMillis(5);
    // The amount of time that the lock will be held before being automatically released
    @PositiveOrZero
    private long lockLeaseTimeMillis = TimeUnit.SECONDS.toMillis(30);
    private String executorServiceName = "executor";
    private int concurrentNextLimit = 1;
    private ExecutorProperties nextExecutor = new ExecutorProperties();
    private DefaultParameters defaultParams = new DefaultParameters();
    
    public QueryExpirationProperties getExpiration() {
        return expiration;
    }
    
    public void setExpiration(QueryExpirationProperties expiration) {
        this.expiration = expiration;
    }
    
    public String getPrivilegedRole() {
        return privilegedRole;
    }
    
    public void setPrivilegedRole(String privilegedRole) {
        this.privilegedRole = privilegedRole;
    }
    
    public long getResultQueueIntervalMillis() {
        return resultQueueIntervalMillis;
    }
    
    public void setResultQueueIntervalMillis(long resultQueueIntervalMillis) {
        this.resultQueueIntervalMillis = resultQueueIntervalMillis;
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
    
    public String getExecutorServiceName() {
        return executorServiceName;
    }
    
    public void setExecutorServiceName(String executorServiceName) {
        this.executorServiceName = executorServiceName;
    }
    
    public int getConcurrentNextLimit() {
        return concurrentNextLimit;
    }
    
    public void setConcurrentNextLimit(int concurrentNextLimit) {
        this.concurrentNextLimit = concurrentNextLimit;
    }
    
    public ExecutorProperties getNextExecutor() {
        return nextExecutor;
    }
    
    public void setNextExecutor(ExecutorProperties nextExecutor) {
        this.nextExecutor = nextExecutor;
    }
    
    public DefaultParameters getDefaultParams() {
        return defaultParams;
    }
    
    public void setDefaultParams(DefaultParameters defaultParams) {
        this.defaultParams = defaultParams;
    }
    
    @Validated
    public static class DefaultParameters {
        
        @NotEmpty
        private String pool = "unassigned";
        
        @Nonnegative
        private int maxConcurrentTasks = 10;
        
        public String getPool() {
            return pool;
        }
        
        public void setPool(String pool) {
            this.pool = pool;
        }
        
        public int getMaxConcurrentTasks() {
            return maxConcurrentTasks;
        }
        
        public void setMaxConcurrentTasks(int maxConcurrentTasks) {
            this.maxConcurrentTasks = maxConcurrentTasks;
        }
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
