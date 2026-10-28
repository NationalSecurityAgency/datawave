package datawave.microservice.query.remote.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import datawave.microservice.query.remote.DynamicMSRemoteQueryServiceBeanDefinitionRegistrar;

@Configuration
public class RemoteMSQueryServiceRegistrarConfiguration {
    @Bean
    public static DynamicMSRemoteQueryServiceBeanDefinitionRegistrar remoteServiceBeanDefinitionRegistrar(Environment environment) {
        return new DynamicMSRemoteQueryServiceBeanDefinitionRegistrar(environment);
    }
}
