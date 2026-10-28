package datawave.microservice.authorization.remote.config;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import datawave.security.authorization.UserOperations;

@EnableConfigurationProperties(MSRemoteUserOperationsProperties.class)
@Configuration
public class MSRemoteUserOperationsConfiguration {

    @Bean
    public Set<UserOperations> registeredRemoteUserOperations(MSRemoteUserOperationsProperties msRemoteUserOperationsProperties,
                    Map<String,UserOperations> userOperationsMap) {
        Set<UserOperations> registeredRemoteUserOperations = new LinkedHashSet<>();
        for (Map.Entry<String,UserOperations> entry : userOperationsMap.entrySet()) {
            if (msRemoteUserOperationsProperties.getRegisteredServices().contains(entry.getKey())) {
                registeredRemoteUserOperations.add(entry.getValue());
            }
        }
        return registeredRemoteUserOperations;
    }
}
