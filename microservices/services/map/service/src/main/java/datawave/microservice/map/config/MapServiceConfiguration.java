package datawave.microservice.map.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import datawave.microservice.config.accumulo.AccumuloProperties;

@EnableConfigurationProperties({MapServiceProperties.class})
@Configuration
public class MapServiceConfiguration {
    @Bean
    @Qualifier("warehouse")
    @ConfigurationProperties("datawave.map.accumulo")
    public AccumuloProperties accumuloProperties() {
        return new AccumuloProperties();
    }
}
