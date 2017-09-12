package datawave.microservice.audit.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "dw.audit")
public class AuditProperties {
    
    private String exchangeName;
    
    public String getExchangeName() {
        return exchangeName;
    }
    
    public void setExchangeName(String exchangeName) {
        this.exchangeName = exchangeName;
    }
}
