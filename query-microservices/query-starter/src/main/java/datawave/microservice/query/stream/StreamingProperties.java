package datawave.microservice.query.stream;

import datawave.microservice.query.config.ThreadPoolTaskExecutorProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "datawave.query.stream")
public class StreamingProperties {
    
    private ThreadPoolTaskExecutorProperties executor = new ThreadPoolTaskExecutorProperties(10, 100, 100, "streamingQuery-");
    
    public ThreadPoolTaskExecutorProperties getExecutor() {
        return executor;
    }
    
    public void setExecutor(ThreadPoolTaskExecutorProperties executor) {
        this.executor = executor;
    }
}
