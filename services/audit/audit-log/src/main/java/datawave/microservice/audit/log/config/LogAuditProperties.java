package datawave.microservice.audit.log.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "audit.log")
public class LogAuditProperties {
    
    private boolean enabled = true;
    private String queueName = "AuditLog";
    private boolean durable = true;
    
    public boolean isEnabled() {
        return enabled;
    }
    
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }
    
    public String getQueueName() {
        return queueName;
    }
    
    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }
    
    public boolean isDurable() {
        return durable;
    }
    
    public void setDurable(boolean durable) {
        this.durable = durable;
    }
}
