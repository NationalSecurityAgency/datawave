package datawave.microservice.query.stream;

import java.util.concurrent.TimeUnit;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.PositiveOrZero;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.http.MediaType;

import datawave.microservice.query.config.ThreadPoolTaskExecutorProperties;

@ConfigurationProperties(prefix = "datawave.query.stream")
public class StreamingProperties {
    // NOTE: Long.MAX_VALUE should not be used here. See org.xnio.nio.WorkerThread which Undertow uses to calculate the
    // thread "deadline". Using a sufficiently large timeout value can cause a long overflow resulting in unpredictable
    // async call timeouts.
    @PositiveOrZero
    private long callTimeout = 1;
    @NotNull
    private TimeUnit callTimeoutUnit = TimeUnit.DAYS;
    
    @NotEmpty
    private String defaultContentType = MediaType.APPLICATION_JSON_VALUE;
    
    private ThreadPoolTaskExecutorProperties executor = new ThreadPoolTaskExecutorProperties(10, 100, 100, "streamingCall-");
    
    public long getCallTimeout() {
        return callTimeout;
    }
    
    public long getCallTimeoutMillis() {
        return callTimeoutUnit.toMillis(callTimeout);
    }
    
    public void setCallTimeout(long callTimeout) {
        this.callTimeout = callTimeout;
    }
    
    public TimeUnit getCallTimeoutUnit() {
        return callTimeoutUnit;
    }
    
    public void setCallTimeoutUnit(TimeUnit callTimeoutUnit) {
        this.callTimeoutUnit = callTimeoutUnit;
    }
    
    public String getDefaultContentType() {
        return defaultContentType;
    }
    
    public void setDefaultContentType(String defaultContentType) {
        this.defaultContentType = defaultContentType;
    }
    
    public ThreadPoolTaskExecutorProperties getExecutor() {
        return executor;
    }
    
    public void setExecutor(ThreadPoolTaskExecutorProperties executor) {
        this.executor = executor;
    }
}
