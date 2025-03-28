package datawave.microservice.querymetric.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "datawave.query.metric.correlator")
public class CorrelatorProperties {
    
    private long maxCorrelationTimeMs = 30000;
    private long maxCorrelationQueueSize = 1000;
    private boolean enabled = true;
    
    public long getMaxCorrelationTimeMs() {
        return maxCorrelationTimeMs;
    }
    
    public void setMaxCorrelationTimeMs(long maxCorrelationTimeMs) {
        this.maxCorrelationTimeMs = maxCorrelationTimeMs;
    }
    
    public long getMaxCorrelationQueueSize() {
        return maxCorrelationQueueSize;
    }
    
    public void setMaxCorrelationQueueSize(long maxCorrelationQueueSize) {
        this.maxCorrelationQueueSize = maxCorrelationQueueSize;
    }
    
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }
    
    public boolean isEnabled() {
        return enabled;
    }
}
