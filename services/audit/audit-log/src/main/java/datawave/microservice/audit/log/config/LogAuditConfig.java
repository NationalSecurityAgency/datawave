package datawave.microservice.audit.log.config;

import datawave.microservice.audit.common.AuditMessageHandler;
import datawave.microservice.audit.common.AuditParameters;
import datawave.microservice.audit.common.Auditor;
import datawave.microservice.audit.log.LogAuditor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.SubscribableChannel;

import java.util.Map;

@Configuration
@EnableBinding(LogAuditConfig.LogAuditBinding.class)
@ConditionalOnProperty(name = "audit.log.enabled", havingValue = "true")
public class LogAuditConfig {
    
    @Bean
    public AuditMessageHandler logAuditMessageHandler(AuditParameters auditParameters, Auditor logAuditor) {
        return new AuditMessageHandler(auditParameters, logAuditor) {
            @Override
            @StreamListener(LogAuditBinding.NAME)
            public void onMessage(Map<String,Object> msg) throws Exception {
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
