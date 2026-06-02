package datawave.microservice.audit.config;

import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import datawave.microservice.audit.AuditServiceProvider;
import datawave.microservice.audit.config.discovery.AuditServiceDiscoveryConfiguration;

@Configuration
@ConditionalOnProperty(name = "audit-client.enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(AuditServiceProperties.class)
@AutoConfigureAfter({AuditServiceDiscoveryConfiguration.class})
public class AuditServiceConfiguration {
    @Bean
    @ConditionalOnMissingBean
    public AuditServiceProvider auditServiceInstanceProvider(AuditServiceProperties serviceProperties) {
        return new AuditServiceProvider(serviceProperties);
    }
}
