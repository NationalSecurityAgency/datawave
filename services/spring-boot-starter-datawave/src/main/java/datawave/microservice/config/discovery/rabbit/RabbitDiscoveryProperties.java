package datawave.microservice.config.discovery.rabbit;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties for discovering RabbitMQ using service discovery.
 */
@ConfigurationProperties(prefix = "spring.rabbitmq.discovery")
public class RabbitDiscoveryProperties {
    private boolean enabled = false;
    
    private String serviceId = "rabbitmq";
    
    private boolean failFast = false;
    
    public boolean isEnabled() {
        return enabled;
    }
    
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }
    
    public String getServiceId() {
        return serviceId;
    }
    
    public void setServiceId(String serviceId) {
        this.serviceId = serviceId;
    }
    
    public boolean isFailFast() {
        return failFast;
    }
    
    public void setFailFast(boolean failFast) {
        this.failFast = failFast;
    }
}
