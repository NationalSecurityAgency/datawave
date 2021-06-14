package datawave.microservice.query.config;

import datawave.marking.ColumnVisibilitySecurityMarking;
import datawave.marking.SecurityMarking;
import datawave.microservice.query.DefaultQueryParameters;
import datawave.microservice.query.QueryParameters;
import datawave.webservice.query.cache.QueryMetricFactory;
import datawave.webservice.query.cache.QueryMetricFactoryImpl;
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
        DefaultQueryParameters queryParameters = new DefaultQueryParameters();
        queryParameters.clear();
        return queryParameters;
    }
    
    @Bean
    @ConditionalOnMissingBean
    @RequestScope
    public SecurityMarking securityMarking() {
        SecurityMarking securityMarking = new ColumnVisibilitySecurityMarking();
        securityMarking.clear();
        return securityMarking;
    }
    
    @Bean
    @ConditionalOnMissingBean(QueryProperties.class)
    @ConfigurationProperties("datawave.query")
    public QueryProperties queryProperties() {
        return new QueryProperties();
    }
    
    @Bean
    @ConditionalOnMissingBean(type = "QueryMetricFactory")
    public QueryMetricFactory queryMetricFactory() {
        return new QueryMetricFactoryImpl();
    }
    
    @RefreshScope
    @Bean
    public ThreadPoolTaskExecutor nextCallExecutor(QueryProperties queryProperties) {
        ThreadPoolTaskExecutorProperties executorProperties = queryProperties.getNextCall().getExecutor();
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(executorProperties.getCorePoolSize());
        executor.setMaxPoolSize(executorProperties.getMaxPoolSize());
        executor.setQueueCapacity(executorProperties.getQueueCapacity());
        executor.setThreadNamePrefix(executorProperties.getThreadNamePrefix());
        executor.initialize();
        return executor;
    }
}
