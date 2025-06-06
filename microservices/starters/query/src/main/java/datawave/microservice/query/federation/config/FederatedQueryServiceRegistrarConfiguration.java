package datawave.microservice.query.federation.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import datawave.microservice.query.federation.DynamicFederatedQueryServiceBeanDefinitionRegistrar;

@Configuration
public class FederatedQueryServiceRegistrarConfiguration {
    @Bean
    public static DynamicFederatedQueryServiceBeanDefinitionRegistrar federatedQueryServiceBeanDefinitionRegistrar(Environment environment) {
        return new DynamicFederatedQueryServiceBeanDefinitionRegistrar(environment);
    }
}
