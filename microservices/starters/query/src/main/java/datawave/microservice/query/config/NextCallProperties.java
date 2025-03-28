package datawave.microservice.query.config;

import java.util.concurrent.TimeUnit;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;
import javax.validation.constraints.PositiveOrZero;

import org.springframework.validation.annotation.Validated;

@Validated
public class NextCallProperties {
    @PositiveOrZero
    private long resultPollInterval = TimeUnit.SECONDS.toMillis(6);
    @NotNull
    private TimeUnit resultPollIntervalUnit = TimeUnit.MILLISECONDS;
    @Positive
    private int concurrency = 1;
    @PositiveOrZero
    private long statusUpdateInterval = TimeUnit.SECONDS.toMillis(6);
    @NotNull
    private TimeUnit statusUpdateIntervalUnit = TimeUnit.MILLISECONDS;
    @PositiveOrZero
    private long maxResultsTimeout = 5l;
    @NotNull
    private TimeUnit maxResultsTimeoutUnit = TimeUnit.SECONDS;
    
    private ThreadPoolTaskExecutorProperties executor = new ThreadPoolTaskExecutorProperties(10, 100, 100, "nextCall-");
    
    public long getResultPollInterval() {
        return resultPollInterval;
    }
    
    public long getResultPollIntervalMillis() {
        return resultPollIntervalUnit.toMillis(resultPollInterval);
    }
    
    public void setResultPollInterval(long resultPollInterval) {
        this.resultPollInterval = resultPollInterval;
    }
    
    public TimeUnit getResultPollIntervalUnit() {
        return resultPollIntervalUnit;
    }
    
    public void setResultPollIntervalUnit(TimeUnit resultPollIntervalUnit) {
        this.resultPollIntervalUnit = resultPollIntervalUnit;
    }
    
    public int getConcurrency() {
        return concurrency;
    }
    
    public void setConcurrency(int concurrency) {
        this.concurrency = concurrency;
    }
    
    public long getStatusUpdateInterval() {
        return statusUpdateInterval;
    }
    
    public long getStatusUpdateIntervalMillis() {
        return statusUpdateIntervalUnit.toMillis(statusUpdateInterval);
    }
    
    public void setStatusUpdateInterval(long statusUpdateInterval) {
        this.statusUpdateInterval = statusUpdateInterval;
    }
    
    public TimeUnit getStatusUpdateIntervalUnit() {
        return statusUpdateIntervalUnit;
    }
    
    public void setStatusUpdateIntervalUnit(TimeUnit statusUpdateIntervalUnit) {
        this.statusUpdateIntervalUnit = statusUpdateIntervalUnit;
    }
    
    public long getMaxResultsTimeout() {
        return maxResultsTimeout;
    }
    
    public long getMaxResultsTimeoutMillis() {
        return maxResultsTimeoutUnit.toMillis(maxResultsTimeout);
    }
    
    public void setMaxResultsTimeout(long maxResultsTimeout) {
        this.maxResultsTimeout = maxResultsTimeout;
    }
    
    public TimeUnit getMaxResultsTimeoutUnit() {
        return maxResultsTimeoutUnit;
    }
    
    public void setMaxResultsTimeoutUnit(TimeUnit maxResultsTimeoutUnit) {
        this.maxResultsTimeoutUnit = maxResultsTimeoutUnit;
    }
    
    public ThreadPoolTaskExecutorProperties getExecutor() {
        return executor;
    }
    
    public void setExecutor(ThreadPoolTaskExecutorProperties executor) {
        this.executor = executor;
    }
}
