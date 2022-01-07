package datawave.microservice.query.config;

import datawave.marking.ColumnVisibilitySecurityMarking;
import datawave.marking.SecurityMarking;
import datawave.microservice.query.DefaultQueryParameters;
import datawave.microservice.query.QueryParameters;
import datawave.microservice.query.stream.StreamingProperties;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.QueryMetricFactory;
import datawave.microservice.querymetric.QueryMetricFactoryImpl;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
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
    @ConditionalOnMissingBean
    @RequestScope
    public BaseQueryMetric baseQueryMetric() {
        return queryMetricFactory().createMetric();
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
    
    @RefreshScope
    @Bean
    public ThreadPoolTaskExecutor streamingExecutor(StreamingProperties streamingProperties) {
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
