package datawave.microservice.authorization.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Provides a default {@link DatawaveSecurityProperties}.
 */
@Configuration
@EnableConfigurationProperties
public class DatawaveSecurityConfiguration {
    @Bean
    @RefreshScope
    @ConditionalOnMissingBean
    public DatawaveSecurityProperties datawaveSecurityProperties() {
        return new DatawaveSecurityProperties();
    }
}
