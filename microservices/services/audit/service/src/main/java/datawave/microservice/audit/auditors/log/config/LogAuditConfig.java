package datawave.microservice.audit.auditors.log.config;

import javax.annotation.Resource;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import datawave.microservice.audit.auditors.log.LogAuditor;
import datawave.microservice.audit.common.AuditMessageConsumer;
import datawave.webservice.common.audit.AuditParameters;
import datawave.webservice.common.audit.Auditor;

/**
 * Configures the LogAuditor to process messages received by the audit service. This configuration is activated via the 'audit.auditors.log.enabled' property.
 * When enabled, this configuration will also enable the appropriate Spring Cloud Stream configuration for the log audit binding, as specified in the audit
 * config.
 */
@Configuration
@ConditionalOnProperty(name = "audit.auditors.log.enabled", havingValue = "true")
public class LogAuditConfig {
    
    @Resource(name = "msgHandlerAuditParams")
    private AuditParameters msgHandlerAuditParams;
    
    @Bean
    public AuditMessageConsumer logAuditSink(Auditor logAuditor) {
        return new AuditMessageConsumer(msgHandlerAuditParams, logAuditor);
    }
    
    @Bean
    public Auditor logAuditor() {
        return new LogAuditor();
    }
}
