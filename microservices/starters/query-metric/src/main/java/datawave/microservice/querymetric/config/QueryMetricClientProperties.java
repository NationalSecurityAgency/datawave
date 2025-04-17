package datawave.microservice.querymetric.config;

import java.util.concurrent.TimeUnit;

import javax.validation.constraints.PositiveOrZero;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@ConfigurationProperties(prefix = "datawave.query.metric.client")
public class QueryMetricClientProperties {
    private boolean enabled;
    private QueryMetricTransportType transport = QueryMetricTransportType.MESSAGE;
    private String scheme = "https";
    private String host = "localhost";
    private int port = 8443;
    private String updateMetricUrl = "/querymetric/v1/updateMetric";
    private String updateMetricsUrl = "/querymetric/v1/updateMetrics";
    private boolean confirmAckEnabled = true;
    private long confirmAckTimeoutMillis = 500L;
    
    Retry retry = new Retry();
    
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }
    
    public boolean isEnabled() {
        return enabled;
    }
    
    public void setTransport(QueryMetricTransportType transport) {
        this.transport = transport;
    }
    
    public QueryMetricTransportType getTransport() {
        return transport;
    }
    
    public void setScheme(String scheme) {
        this.scheme = scheme;
    }
    
    public String getScheme() {
        return scheme;
    }
    
    public void setHost(String host) {
        this.host = host;
    }
    
    public String getHost() {
        return host;
    }
    
    public int getPort() {
        return port;
    }
    
    public void setPort(int port) {
        this.port = port;
    }
    
    public String getUpdateMetricUrl() {
        return updateMetricUrl;
    }
    
    public void setUpdateMetricUrl(String updateMetricUrl) {
        this.updateMetricUrl = updateMetricUrl;
    }
    
    public String getUpdateMetricsUrl() {
        return updateMetricsUrl;
    }
    
    public void setUpdateMetricsUrl(String updateMetricsUrl) {
        this.updateMetricsUrl = updateMetricsUrl;
    }
    
    public boolean isConfirmAckEnabled() {
        return confirmAckEnabled;
    }
    
    public void setConfirmAckEnabled(boolean confirmAckEnabled) {
        this.confirmAckEnabled = confirmAckEnabled;
    }
    
    public long getConfirmAckTimeoutMillis() {
        return confirmAckTimeoutMillis;
    }
    
    public void setConfirmAckTimeoutMillis(long confirmAckTimeoutMillis) {
        this.confirmAckTimeoutMillis = confirmAckTimeoutMillis;
    }
    
    public Retry getRetry() {
        return retry;
    }
    
    public void setRetry(Retry retry) {
        this.retry = retry;
    }
    
    @Validated
    public static class Retry {
        @PositiveOrZero
        private int maxAttempts = 10;
        
        @PositiveOrZero
        private long failTimeoutMillis = TimeUnit.MINUTES.toMillis(5);
        
        @PositiveOrZero
        private long backoffIntervalMillis = TimeUnit.SECONDS.toMillis(5);
        
        public int getMaxAttempts() {
            return maxAttempts;
        }
        
        public void setMaxAttempts(int maxAttempts) {
            this.maxAttempts = maxAttempts;
        }
        
        public long getFailTimeoutMillis() {
            return failTimeoutMillis;
        }
        
        public void setFailTimeoutMillis(long failTimeoutMillis) {
            this.failTimeoutMillis = failTimeoutMillis;
        }
        
        public long getBackoffIntervalMillis() {
            return backoffIntervalMillis;
        }
        
        public void setBackoffIntervalMillis(long backoffIntervalMillis) {
            this.backoffIntervalMillis = backoffIntervalMillis;
        }
    }
}
