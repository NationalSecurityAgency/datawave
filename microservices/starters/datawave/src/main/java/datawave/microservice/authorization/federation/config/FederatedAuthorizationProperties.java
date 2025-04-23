package datawave.microservice.authorization.federation.config;

import java.util.LinkedHashSet;
import java.util.Set;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "datawave.authorization.federation")
public class FederatedAuthorizationProperties {
    private Set<String> registeredServices = new LinkedHashSet<>();
    
    public Set<String> getRegisteredServices() {
        return registeredServices;
    }
    
    public void setRegisteredServices(Set<String> registeredServices) {
        this.registeredServices = registeredServices;
    }
}
