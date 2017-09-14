package datawave.microservice.config;

import datawave.microservice.config.metrics.MetricsConfigurationProperties;
import datawave.microservice.config.web.DatawaveServerProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.SearchStrategy;
import org.springframework.boot.autoconfigure.web.ServerProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configures default beans needed by DATAWAVE microservices.
 */
@Configuration
public class DatawaveMicroserviceConfig {
    @Bean
    @ConditionalOnMissingBean(search = SearchStrategy.CURRENT)
    public ServerProperties serverProperties() {
        return new DatawaveServerProperties();
    }
    
    @Bean
    public MetricsConfigurationProperties metricsConfigurationProperties() {
        return new MetricsConfigurationProperties();
    }
}
