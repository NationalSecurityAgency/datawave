package datawave.microservice.annotation.common.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.common.connection.AccumuloConnectionFactoryImpl;
import datawave.core.common.result.ConnectionPoolsProperties;

/** Accumulo connection classes used for obtaining connections to Accumulo */
@Configuration
public class AccumuloConfiguration {
    @Bean
    @ConfigurationProperties("datawave.connection.factory")
    public ConnectionPoolsProperties connectionPoolsProperties() {
        return new ConnectionPoolsProperties();
    }

    @Bean
    @ConditionalOnMissingBean
    public AccumuloConnectionFactory accumuloConnectionFactory(ConnectionPoolsProperties connectionPoolsProperties) {
        // no need for metadata cache here.
        return AccumuloConnectionFactoryImpl.getInstance(null, connectionPoolsProperties);
    }
}
