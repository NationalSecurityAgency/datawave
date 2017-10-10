package datawave.microservice.audit.accumulo.config;

import datawave.microservice.audit.accumulo.AccumuloAuditor;
import datawave.microservice.audit.common.AuditMessageHandler;
import datawave.microservice.audit.common.AuditParameters;
import datawave.microservice.audit.common.Auditor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.SubscribableChannel;

import java.util.Map;

@Configuration
@EnableConfigurationProperties(AccumuloAuditProperties.class)
@EnableBinding(AccumuloAuditConfig.AccumuloAuditBinding.class)
@ConditionalOnProperty(name = "audit.accumulo.enabled", havingValue = "true")
public class AccumuloAuditConfig {
    
    @Bean
    public AuditMessageHandler accumuloAuditMessageHandler(AuditParameters auditParameters, Auditor accumuloAuditor) {
        return new AuditMessageHandler(auditParameters, accumuloAuditor) {
            @Override
            @StreamListener(AccumuloAuditBinding.NAME)
            public void onMessage(Map<String,Object> msg) throws Exception {
                super.onMessage(msg);
            }
        };
    }

    @Bean
    public Auditor accumuloAuditor(AccumuloAuditProperties accumuloAuditProperties) {
        return new AccumuloAuditor(accumuloAuditProperties);
    }

    public interface AccumuloAuditBinding {
        String NAME = "accumuloAuditSink";
        
        @Input(NAME)
        SubscribableChannel accumuloAuditSink();
    }
}
