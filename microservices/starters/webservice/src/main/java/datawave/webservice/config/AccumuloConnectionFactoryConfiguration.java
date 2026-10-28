package datawave.webservice.config;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import datawave.core.common.cache.AccumuloTableCache;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.common.connection.AccumuloConnectionFactoryImpl;
import datawave.core.common.result.ConnectionPoolsProperties;
import datawave.webservice.DatawaveResourceFactory;

@Configuration
public class AccumuloConnectionFactoryConfiguration {

    @Bean
    @ConfigurationProperties("datawave.connection.factory")
    public ConnectionPoolsProperties connectionPoolsProperties() {
        return new ConnectionPoolsProperties();
    }

    @Bean
    public AccumuloConnectionFactoryImpl accumuloConnectionFactory(ObjectProvider<AccumuloTableCache> accumuloTableCache,
                    ConnectionPoolsProperties connectionPoolsProperties) {
        return new AccumuloConnectionFactoryImpl(accumuloTableCache.getIfAvailable(), connectionPoolsProperties);
    }

    @Bean
    public AccumuloConnectionFactory accumuloConnectionFactory(DatawaveResourceFactory datawaveResourceFactory) {
        return datawaveResourceFactory.accumuloConnectionFactory();
    }
}
