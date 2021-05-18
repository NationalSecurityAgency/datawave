package datawave.microservice.query.config;

import datawave.microservice.query.DefaultQueryParameters;
import datawave.microservice.query.QueryParameters;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.web.context.annotation.RequestScope;

@Configuration
public class QueryServiceConfig {
    
    @Bean
    @ConditionalOnMissingBean
    @RequestScope
    public QueryParameters queryParameters() {
        return new DefaultQueryParameters();
    }
    
    @Bean
    @ConditionalOnMissingBean(QueryProperties.class)
    @ConfigurationProperties("datawave.query")
    public QueryProperties queryProperties() {
        return new QueryProperties();
    }
    
    @RefreshScope
    @Bean
    public ThreadPoolTaskExecutor nextExecutor(QueryProperties queryProperties) {
        QueryProperties.ExecutorProperties executorProperties = queryProperties.getNextExecutor();
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(executorProperties.getCorePoolSize());
        executor.setMaxPoolSize(executorProperties.getMaxPoolSize());
        executor.setQueueCapacity(executorProperties.getQueueCapacity());
        executor.setThreadNamePrefix(executorProperties.getThreadNamePrefix());
        executor.initialize();
        return executor;
    }
}
