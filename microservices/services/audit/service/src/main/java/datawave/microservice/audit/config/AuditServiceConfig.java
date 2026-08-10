package datawave.microservice.audit.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import datawave.microservice.audit.common.AuditMessageSupplier;

/**
 * Configuration for the audit service.
 * <p>
 * This configuration is used to specify the binding for the audit producer, and to establish a confirm ack channel which is used to confirm that audit messages
 * have been successfully received by our messaging infrastructure.
 */
@Configuration
@EnableConfigurationProperties(AuditProperties.class)
public class AuditServiceConfig {
    @Bean
    public AuditMessageSupplier auditSource() {
        return new AuditMessageSupplier();
    }
}
