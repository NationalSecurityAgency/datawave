package datawave.microservice.authorization.federation.config;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import datawave.security.authorization.UserOperations;

@EnableConfigurationProperties(FederatedAuthorizationProperties.class)
@Configuration
public class FederatedAuthorizationConfiguration {
    
    @Bean
    public Set<UserOperations> registeredFederatedUserOperations(FederatedAuthorizationProperties federatedAuthorizationProperties,
                    Map<String,UserOperations> userOperationsMap) {
        Set<UserOperations> registeredFederatedUserOperations = new LinkedHashSet<>();
        for (Map.Entry<String,UserOperations> entry : userOperationsMap.entrySet()) {
            if (federatedAuthorizationProperties.getRegisteredServices().contains(entry.getKey())) {
                registeredFederatedUserOperations.add(entry.getValue());
            }
        }
        return registeredFederatedUserOperations;
    }
}
