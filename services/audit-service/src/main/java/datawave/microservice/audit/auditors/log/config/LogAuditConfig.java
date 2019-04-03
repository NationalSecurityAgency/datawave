package datawave.microservice.audit.auditors.log.config;

import datawave.microservice.audit.common.AuditMessage;
import datawave.microservice.audit.common.AuditMessageHandler;
import datawave.microservice.audit.auditors.log.LogAuditor;
import datawave.webservice.common.audit.AuditParameters;
import datawave.webservice.common.audit.Auditor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.SubscribableChannel;

import javax.annotation.Resource;

/**
 * Configures the LogAuditor to process messages received by the audit service. This configuration is activated via the 'audit.auditors.log.enabled' property.
 * When enabled, this configuration will also enable the appropriate Spring Cloud Stream configuration for the log audit binding, as specified in the audit
 * config.
 */
@Configuration
@EnableBinding(LogAuditConfig.LogAuditBinding.class)
@ConditionalOnProperty(name = "audit.auditors.log.enabled", havingValue = "true")
public class LogAuditConfig {
    
    @Resource(name = "msgHandlerAuditParams")
    private AuditParameters msgHandlerAuditParams;
    
    @Bean
    public AuditMessageHandler logAuditMessageHandler(Auditor logAuditor) {
        return new AuditMessageHandler(msgHandlerAuditParams, logAuditor) {
            @Override
            @StreamListener(LogAuditBinding.NAME)
            public void onMessage(AuditMessage msg) throws Exception {
                super.onMessage(msg);
            }
        };
    }
    
    @Bean
    public Auditor logAuditor() {
        return new LogAuditor();
    }
    
    public interface LogAuditBinding {
        String NAME = "logAuditSink";
        
        @Input(NAME)
        SubscribableChannel logAuditSink();
    }
}
