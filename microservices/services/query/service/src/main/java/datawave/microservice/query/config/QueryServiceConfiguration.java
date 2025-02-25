package datawave.microservice.query.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.security.config.annotation.web.configuration.WebSecurityCustomizer;
import org.springframework.web.context.annotation.RequestScope;

import datawave.marking.ColumnVisibilitySecurityMarking;
import datawave.marking.SecurityMarking;
import datawave.microservice.query.DefaultQueryParameters;
import datawave.microservice.query.QueryParameters;
import datawave.microservice.query.stream.StreamingProperties;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.QueryMetricFactory;
import datawave.microservice.querymetric.QueryMetricFactoryImpl;

@Configuration
public class QueryServiceConfiguration {
    
    @Bean
    public WebSecurityCustomizer ignorePoolHealthEndpoint() {
        return (web) -> web.ignoring().antMatchers("/v1/pool/{poolName}/health");
    }
    
    @Bean
    @RequestScope
    @ConditionalOnProperty(name = "datawave.defaults.QueryParameters.enabled", havingValue = "true", matchIfMissing = true)
    public QueryParameters queryParameters() {
        DefaultQueryParameters queryParameters = new DefaultQueryParameters();
        queryParameters.clear();
        return queryParameters;
    }
    
    @Bean
    @RequestScope
    @ConditionalOnProperty(name = "datawave.defaults.SecurityMarking.enabled", havingValue = "true", matchIfMissing = true)
    public SecurityMarking querySecurityMarking() {
        SecurityMarking securityMarking = new ColumnVisibilitySecurityMarking();
        securityMarking.clear();
        return securityMarking;
    }
    
    @Bean
    @RequestScope
    public BaseQueryMetric baseQueryMetric(QueryMetricFactory queryMetricFactory) {
        return queryMetricFactory.createMetric();
    }
    
    @Bean
    @ConditionalOnProperty(name = "datawave.defaults.QueryMetricFactory.enabled", havingValue = "true", matchIfMissing = true)
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
    
    @RefreshScope
    @Bean
    public ThreadPoolTaskExecutor streamingCallExecutor(StreamingProperties streamingProperties) {
        ThreadPoolTaskExecutorProperties executorProperties = streamingProperties.getExecutor();
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(executorProperties.getCorePoolSize());
        executor.setMaxPoolSize(executorProperties.getMaxPoolSize());
        executor.setQueueCapacity(executorProperties.getQueueCapacity());
        executor.setThreadNamePrefix(executorProperties.getThreadNamePrefix());
        executor.initialize();
        return executor;
    }
}
