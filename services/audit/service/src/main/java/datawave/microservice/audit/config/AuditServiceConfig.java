package datawave.microservice.audit.config;

import org.springframework.cloud.stream.annotation.Output;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.MessageChannel;

@Configuration
public class AuditServiceConfig {
    public interface AuditSourceBinding {
        String NAME = "auditSource";
        
        @Output(NAME)
        MessageChannel auditSource();
    }
}
