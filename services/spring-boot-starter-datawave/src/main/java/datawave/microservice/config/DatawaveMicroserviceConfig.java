package datawave.microservice.config;

import datawave.microservice.config.metrics.MetricsConfigurationProperties;
import datawave.microservice.config.web.RestClientProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.SearchStrategy;
import org.springframework.cloud.bus.ServiceMatcher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.util.AntPathMatcher;

/**
 * Configures default beans needed by DATAWAVE microservices.
 */
@Configuration
public class DatawaveMicroserviceConfig {
    @Bean
    @ConditionalOnMissingBean(search = SearchStrategy.CURRENT)
    public RestClientProperties restClientProperties() {
        return new RestClientProperties();
    }
    
    @Bean
    public MetricsConfigurationProperties metricsConfigurationProperties() {
        return new MetricsConfigurationProperties();
    }
    
    @Bean
    @Profile("nomessaging")
    public ServiceMatcher serviceMatcher() {
        // This matcher is used to deal with spring bus events. If we're running with the "nomessaging" profile, that means
        // we're not using a message bus and therefore no ServiceMatcher will be created. However, we still reference one
        // for internal use, so we make a dummy for that case.
        return new ServiceMatcher(new AntPathMatcher(), "invalid");
    }
}
