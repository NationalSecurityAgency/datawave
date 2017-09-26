package datawave.microservice.authorization.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.core.GrantedAuthorityDefaults;

/**
 * Provides a default {@link DatawaveSecurityProperties}, and also provide an empty default role prefix for method security annotations. We don't want our roles
 * to be prefixed with "ROLE_".
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
    
    @Bean
    @ConditionalOnMissingBean
    public GrantedAuthorityDefaults grantedAuthorityDefaults() {
        return new GrantedAuthorityDefaults("");
    }
}
