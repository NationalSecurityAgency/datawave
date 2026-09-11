package datawave.microservice.annotation.util.lookup.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class LookupConfig {
    @Bean
    @ConditionalOnMissingBean(LookupProperties.class)
    @ConfigurationProperties("annotation.lookup")
    public LookupProperties lookupProperties() {
        return new LookupProperties();
    }
}
