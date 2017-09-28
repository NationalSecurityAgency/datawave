package datawave.microservice.audit.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "audit")
public class AuditProperties {
    
    private String exchangeName = "Auditor";
    
    public String getExchangeName() {
        return exchangeName;
    }
    
    public void setExchangeName(String exchangeName) {
        this.exchangeName = exchangeName;
    }
}
