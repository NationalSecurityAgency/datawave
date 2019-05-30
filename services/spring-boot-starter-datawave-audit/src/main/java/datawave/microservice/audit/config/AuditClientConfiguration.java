package datawave.microservice.audit.config;

import datawave.webservice.common.audit.AuditParameters;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.function.Supplier;

@Configuration
@ConditionalOnProperty(name = "audit-client.enabled", havingValue = "true", matchIfMissing = true)
public class AuditClientConfiguration {
    @Bean
    @Qualifier("auditRequestValidator")
    @ConditionalOnMissingBean
    public Supplier<AuditParameters> auditValidationSupplier() {
        return () -> (new AuditParameters());
    }
}
