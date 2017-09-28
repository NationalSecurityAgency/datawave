package datawave.microservice.audit.config;

import org.springframework.amqp.core.FanoutExchange;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(AuditProperties.class)
public class AuditServiceConfig {
    @Bean
    public FanoutExchange auditExchange(AuditProperties auditProperties) {
        return new FanoutExchange(auditProperties.getExchangeName());
    }
}
