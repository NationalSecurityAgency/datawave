package datawave.webservice.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;

import datawave.webservice.DynamicWSRemoteQueryServiceBeanDefinitionRegistrar;
import datawave.webservice.query.result.event.ResponseObjectFactory;

@Configuration
@Profile("federation")
public class WSRemoteQueryServiceRegistrarConfiguration {
    @Bean
    public static DynamicWSRemoteQueryServiceBeanDefinitionRegistrar wsRemoteServiceBeanDefinitionRegistrar(Environment environment,
                    ResponseObjectFactory responseObjectFactory) {
        return new DynamicWSRemoteQueryServiceBeanDefinitionRegistrar(environment, responseObjectFactory);
    }
}
