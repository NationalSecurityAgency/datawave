package datawave.microservice.audit.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "audit")
public class AuditProperties {
    private boolean confirmAckEnabled = true;
    private long confirmAckTimeoutMillis = 500L;
    
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
}
