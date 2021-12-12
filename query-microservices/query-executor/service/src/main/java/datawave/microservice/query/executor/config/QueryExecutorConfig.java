package datawave.microservice.query.executor.config;

import datawave.microservice.query.executor.task.FindWorkTask;
import datawave.microservice.querymetric.QueryMetricFactory;
import datawave.microservice.querymetric.QueryMetricFactoryImpl;
import datawave.services.common.cache.AccumuloTableCache;
import datawave.services.common.cache.AccumuloTableCacheConfiguration;
import datawave.services.common.cache.AccumuloTableCacheImpl;
import datawave.services.common.connection.AccumuloConnectionFactory;
import datawave.services.common.connection.AccumuloConnectionFactoryImpl;
import datawave.services.common.result.ConnectionPoolsProperties;
import datawave.services.query.predict.NoOpQueryPredictor;
import datawave.services.query.predict.QueryPredictor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
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
    @ConditionalOnMissingBean(AccumuloTableCacheConfiguration.class)
    @ConfigurationProperties("datawave.table.cache")
    public AccumuloTableCacheConfiguration tableCacheConfiguration() {
        return new AccumuloTableCacheConfiguration();
    }
    
    @Bean
    @ConditionalOnMissingBean(AccumuloTableCache.class)
    public AccumuloTableCache tableCache(AccumuloTableCacheConfiguration accumuloTableCacheConfiguration) {
        return new AccumuloTableCacheImpl(accumuloTableCacheConfiguration);
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
    
    /**
     * A cache of queries cancelled or closed to avoid extra work in the FindWorkTask
     */
    @Bean
    public FindWorkTask.CloseCancelCache closeCancelCache(ExecutorProperties executorProperties) {
        return new FindWorkTask.CloseCancelCache(executorProperties.getMonitorMaxCacheSize());
    }
    
}
