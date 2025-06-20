package datawave.microservice.query.remote.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import datawave.microservice.query.remote.DynamicRemoteQueryServiceBeanDefinitionRegistrar;

@Configuration
public class RemoteQueryServiceRegistrarConfiguration {
    @Bean
    public static DynamicRemoteQueryServiceBeanDefinitionRegistrar remoteServiceBeanDefinitionRegistrar(Environment environment) {
        return new DynamicRemoteQueryServiceBeanDefinitionRegistrar(environment);
    }
}
