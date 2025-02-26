package datawave.microservice.query.mapreduce.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import datawave.core.common.cache.AccumuloTableCache;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.common.connection.AccumuloConnectionFactoryImpl;
import datawave.core.common.result.ConnectionPoolsProperties;

@Configuration
public class AccumuloConnectionFactoryConfig {
    @Bean
    @ConfigurationProperties("datawave.connection.factory")
    public ConnectionPoolsProperties connectionPoolsProperties() {
        return new ConnectionPoolsProperties();
    }
    
    @Bean
    public AccumuloConnectionFactory accumuloConnectionFactory(AccumuloTableCache accumuloTableCache, ConnectionPoolsProperties connectionPoolsProperties) {
        return AccumuloConnectionFactoryImpl.getInstance(accumuloTableCache, connectionPoolsProperties);
    }
}
