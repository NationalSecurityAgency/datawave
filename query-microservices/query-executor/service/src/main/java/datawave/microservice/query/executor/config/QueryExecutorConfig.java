package datawave.microservice.query.executor.config;

import datawave.microservice.query.config.QueryProperties;
import datawave.microservice.querymetric.QueryMetricFactory;
import datawave.microservice.querymetric.QueryMetricFactoryImpl;
import datawave.webservice.common.cache.AccumuloTableCache;
import datawave.webservice.common.cache.AccumuloTableCacheConfiguration;
import datawave.webservice.common.cache.AccumuloTableCacheImpl;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.common.connection.AccumuloConnectionFactoryImpl;
import datawave.webservice.common.result.ConnectionPoolsProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class QueryExecutorConfig {
    
    @Bean
    @ConditionalOnMissingBean(ExecutorProperties.class)
    @ConfigurationProperties("datawave.query.executor")
    public ExecutorProperties executorProperties() {
        return new ExecutorProperties();
    }
    
    @Bean
    @ConditionalOnMissingBean(QueryProperties.class)
    @ConfigurationProperties("datawave.query")
    public QueryProperties queryProperties() {
        return new QueryProperties();
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
}
