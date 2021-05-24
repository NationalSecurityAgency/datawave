package datawave.microservice.query.executor.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import datawave.microservice.query.executor.config.ExecutorProperties;

@Configuration
public class QueryExecutorConfig {
    
    @Bean
    @ConditionalOnMissingBean(ExecutorProperties.class)
    @ConfigurationProperties("datawave.query.executor")
    public ExecutorProperties executorProperties() {
        return new ExecutorProperties();
    }
}
