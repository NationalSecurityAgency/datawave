package datawave.microservice.querymetric.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import datawave.core.common.cache.AccumuloTableCache;
import datawave.core.common.cache.AccumuloTableCacheProperties;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.common.connection.AccumuloConnectionFactoryImpl;
import datawave.core.common.result.ConnectionPoolsProperties;

@Configuration
public class AccumuloConfiguration {

    @Bean
    @ConfigurationProperties("datawave.table.cache")
    public AccumuloTableCacheProperties accumuloTableCacheProperties() {
        return new AccumuloTableCacheProperties();
    }

    @Bean
    @ConfigurationProperties("datawave.connection.factory")
    public ConnectionPoolsProperties connectionPoolsProperties() {
        return new ConnectionPoolsProperties();
    }

    @Bean
    @ConditionalOnMissingBean
    public AccumuloConnectionFactory accumuloConnectionFactory(ConnectionPoolsProperties connectionPoolsProperties,
                    @Autowired(required = false) AccumuloTableCache tableCache) {
        AccumuloConnectionFactory connectionFactory = AccumuloConnectionFactoryImpl.getInstance(tableCache, connectionPoolsProperties);
        if (tableCache != null) {
            tableCache.setConnectionFactory(connectionFactory);
        }
        return connectionFactory;
    }
}
