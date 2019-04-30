package datawave.microservice.audit.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.PositiveOrZero;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Validated
@EnableConfigurationProperties(AuditProperties.class)
@ConfigurationProperties(prefix = "audit")
public class AuditProperties {
    private boolean confirmAckEnabled = true;
    private long confirmAckTimeoutMillis = 500L;
    
    @Valid
    private Retry retry = new Retry();
    
    @Valid
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
    
    @Validated
    public static class Filesystem {
        @NotNull
        protected String pathUri;
        protected List<String> configResources;
        
        public String getPathUri() {
            return pathUri;
        }
        
        public void setPathUri(String pathUri) {
            this.pathUri = pathUri;
        }
        
        public List<String> getConfigResources() {
            return configResources;
        }
        
        public void setConfigResources(List<String> configResources) {
            this.configResources = configResources;
        }
    }
}
