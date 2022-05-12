package datawave.microservice.query.executor.config;

import datawave.microservice.query.config.QueryProperties;
import datawave.microservice.query.executor.QueryExecutor;
import datawave.microservice.query.executor.task.FindWorkMonitor;
import datawave.microservice.query.storage.QueryStorageCache;
import datawave.microservice.querymetric.QueryMetricFactory;
import datawave.microservice.querymetric.QueryMetricFactoryImpl;
import datawave.services.common.cache.AccumuloTableCache;
import datawave.services.common.connection.AccumuloConnectionFactory;
import datawave.services.common.connection.AccumuloConnectionFactoryImpl;
import datawave.services.common.result.ConnectionPoolsProperties;
import datawave.services.query.predict.NoOpQueryPredictor;
import datawave.services.query.predict.QueryPredictor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration
@EnableScheduling
public class QueryExecutorConfig {
    
    @Bean
    @ConditionalOnMissingBean(ExecutorProperties.class)
    @ConfigurationProperties("datawave.query.executor")
    public ExecutorProperties executorProperties() {
        return new ExecutorProperties();
    }
    
    @Bean
    @ConditionalOnMissingBean(ConnectionPoolsProperties.class)
    @ConfigurationProperties("datawave.connection.factory")
    public ConnectionPoolsProperties poolProperties() {
        return new ConnectionPoolsProperties();
    }
    
    @Bean
    @ConditionalOnMissingBean(name = "accumuloConnectionFactory")
    public AccumuloConnectionFactory connectionFactory(AccumuloTableCache cache, ConnectionPoolsProperties config) {
        return AccumuloConnectionFactoryImpl.getInstance(cache, config);
    }
    
    @Bean
    @ConditionalOnMissingBean(type = "QueryMetricFactory")
    public QueryMetricFactory queryMetricFactory() {
        return new QueryMetricFactoryImpl();
    }
    
    @Bean
    @ConditionalOnMissingBean(QueryPredictor.class)
    public QueryPredictor queryPredictor() {
        return new NoOpQueryPredictor();
    }
    
    @Bean
    @ConditionalOnMissingBean(FindWorkMonitor.class)
    @ConditionalOnProperty(name = "datawave.query.executor.monitor.enabled", havingValue = "true", matchIfMissing = true)
    public FindWorkMonitor findWorkMonitor(ExecutorProperties executorProperties, QueryProperties queryProperties, QueryStorageCache cache,
                    QueryExecutor executor) {
        return new FindWorkMonitor(executorProperties, queryProperties, cache, executor);
    }
}
