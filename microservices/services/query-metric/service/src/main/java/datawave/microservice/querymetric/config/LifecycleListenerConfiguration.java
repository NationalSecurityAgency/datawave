package datawave.microservice.querymetric.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import datawave.microservice.querymetric.MergeLockLifecycleListener;

@Configuration
public class LifecycleListenerConfiguration {
    
    @Bean
    public MergeLockLifecycleListener mergeLockLifecycleListener() {
        return new MergeLockLifecycleListener();
    }
}
