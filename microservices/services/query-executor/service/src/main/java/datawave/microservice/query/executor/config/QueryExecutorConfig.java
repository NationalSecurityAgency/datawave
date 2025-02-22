package datawave.microservice.query.executor.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

import datawave.core.common.cache.AccumuloTableCache;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.common.connection.AccumuloConnectionFactoryImpl;
import datawave.core.common.result.ConnectionPoolsProperties;
import datawave.core.query.predict.NoOpQueryPredictor;
import datawave.core.query.predict.QueryPredictor;
import datawave.microservice.querymetric.QueryMetricFactory;
import datawave.microservice.querymetric.QueryMetricFactoryImpl;

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
    public AccumuloConnectionFactory accumuloConnectionFactory(AccumuloTableCache cache, ConnectionPoolsProperties config) {
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
}
