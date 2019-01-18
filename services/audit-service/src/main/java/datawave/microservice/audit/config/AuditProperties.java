package datawave.microservice.audit.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

import java.util.List;
import java.util.concurrent.TimeUnit;

@EnableConfigurationProperties(AuditProperties.class)
@ConfigurationProperties(prefix = "audit")
public class AuditProperties {
    private boolean confirmAckEnabled = true;
    private long confirmAckTimeoutMillis = 500L;
    
    private Retry retry = new Retry();
    private Filesystem fs = new Filesystem();
    
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
        if (retry == null)
            throw new NullPointerException("Audit Retry properties must not be null.");
        this.retry = retry;
    }
    
    public Filesystem getFs() {
        return fs;
    }
    
    public void setFs(Filesystem fs) {
        this.fs = fs;
    }
    
    public static class Retry {
        private int maxAttempts = 10;
        private long failTimeoutMillis = TimeUnit.MINUTES.toMillis(5);
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
    
    public static class Filesystem {
        protected String fileUri;
        protected List<String> configResources;
        
        public String getFileUri() {
            return fileUri;
        }
        
        public void setFileUri(String fileUri) {
            this.fileUri = fileUri;
        }
        
        public List<String> getConfigResources() {
            return configResources;
        }
        
        public void setConfigResources(List<String> configResources) {
            this.configResources = configResources;
        }
    }
}
