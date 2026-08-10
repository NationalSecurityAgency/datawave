package datawave.microservice.audit.config;

import java.util.function.Supplier;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import datawave.webservice.common.audit.AuditParameters;

@Configuration
@ConditionalOnProperty(name = "audit-client.enabled", havingValue = "true", matchIfMissing = true)
public class AuditClientConfiguration {
    @Bean
    @Qualifier("auditRequestValidator")
    @ConditionalOnMissingBean(name = "auditRequestValidator")
    @ConditionalOnProperty(name = "datawave.defaults.AuditParametersSupplier.enabled", havingValue = "true", matchIfMissing = true)
    public Supplier<AuditParameters> auditValidationSupplier() {
        return AuditParameters::new;
    }
}
